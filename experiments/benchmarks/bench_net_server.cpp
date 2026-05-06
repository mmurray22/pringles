#define _GNU_SOURCE
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>
#include <vector>

#define CLIENT_PORT 8888
#define STORAGE_REPLY_PORT 8889
#define STORAGE_NODE_PORT 9999
#define BATCH_SIZE 512
#define BUF_SIZE 1024

std::vector<int> client_fds;
std::vector<int> storage_fds;
struct sockaddr_in storage_addr;

// Creates a UDP socket bound to a random OS-assigned ephemeral port
int create_random_port_socket() {
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in local_addr;
    memset(&local_addr, 0, sizeof(local_addr));
    
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    local_addr.sin_port = htons(0); // The magic zero: OS assigns a random port

    // Bind the socket to apply the random port assignment
    if (bind(sockfd, (const struct sockaddr *)&local_addr, sizeof(local_addr)) < 0) {
        perror("Bind to port 0 failed");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    return sockfd;
}

int create_reuseport_socket(int port) {
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) return -1;

    int optval = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));

    //int busy_poll_us = 50; // Spin for 50 microseconds before sleeping
    //setsockopt(sockfd, SOL_SOCKET, SO_BUSY_POLL, &busy_poll_us, sizeof(busy_poll_us));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) return -1;
    return sockfd;
}

// --- Frontend: Listens to Clients, Forwards blindly to Storage ---
void client_worker(int thread_id, int num_storage_threads) {
    int recv_fd = client_fds[thread_id]; 
    struct mmsghdr msgs[BATCH_SIZE];
    struct iovec iovecs[BATCH_SIZE];
    char buffers[BATCH_SIZE][BUF_SIZE];

    memset(msgs, 0, sizeof(msgs));
    for (int i = 0; i < BATCH_SIZE; i++) {
        iovecs[i].iov_base = buffers[i];
        msgs[i].msg_hdr.msg_iov = &iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }

    struct sockaddr_in sender_addrs[BATCH_SIZE];

    while (true) {
        for (int i = 0; i < BATCH_SIZE; i++) {
	    msgs[i].msg_hdr.msg_name = &sender_addrs[i]; 
            msgs[i].msg_hdr.msg_namelen = sizeof(struct sockaddr_in);
	    iovecs[i].iov_len = BUF_SIZE;
	}

        int num_received = recvmmsg(recv_fd, msgs, BATCH_SIZE, MSG_WAITFORONE, NULL);
        if (num_received < 0) continue;

        struct mmsghdr out_msgs[BATCH_SIZE];
        int out_count = 0;

        for (int i = 0; i < num_received; i++) {
            if (buffers[i][0] == (char)0xAA) {
                // Completely Stateless! Just route straight to storage.
                // The client's routing info is already inside the payload.
                msgs[i].msg_hdr.msg_name = &storage_addr;
                msgs[i].msg_hdr.msg_namelen = sizeof(storage_addr);
                iovecs[i].iov_len = msgs[i].msg_len;
                out_msgs[out_count++] = msgs[i];
            }
        }

        if (out_count > 0) {
            int send_fd = storage_fds[thread_id % num_storage_threads];
            sendmmsg(send_fd, out_msgs, out_count, 0);
        }
    }
}

// --- Backend: Listens to Storage, Parses Payload to find the Client ---
void storage_worker(int thread_id, int num_client_threads) {
    int recv_fd = storage_fds[thread_id]; 
    struct mmsghdr msgs[BATCH_SIZE];
    struct iovec iovecs[BATCH_SIZE];
    struct sockaddr_in target_addrs[BATCH_SIZE]; // Array to hold parsed addresses
    char buffers[BATCH_SIZE][BUF_SIZE];

    memset(msgs, 0, sizeof(msgs));
    for (int i = 0; i < BATCH_SIZE; i++) {
        iovecs[i].iov_base = buffers[i];
        msgs[i].msg_hdr.msg_iov = &iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }

    /* TODO: delete */
    /*char dummy_payload[64] = {0}; // Adjust size to match your normal payload
    struct iovec iov[40];
    struct mmsghdr msgvec[40];
    
    // Prepare a batch of 40 dummy packets
    for (int i = 0; i < 40; i++) {
        iov[i].iov_base = dummy_payload;
        iov[i].iov_len = sizeof(dummy_payload);
        msgvec[i].msg_hdr.msg_name = &storage_addr; // Point to Storage Node
        msgvec[i].msg_hdr.msg_namelen = sizeof(storage_addr);
        msgvec[i].msg_hdr.msg_iov = &iov[i];
        msgvec[i].msg_hdr.msg_iovlen = 1;
        msgvec[i].msg_len = 0;
    }
    
    // Fire the initial 40 packets at the Storage node to kickstart the loop
    sendmmsg(recv_fd, msgvec, 40, 0);*/
    /* TODO */

    while (true) {
        for (int i = 0; i < BATCH_SIZE; i++) iovecs[i].iov_len = BUF_SIZE;

        int num_received = recvmmsg(recv_fd, msgs, BATCH_SIZE, MSG_WAITFORONE, NULL);
        if (num_received < 0) continue;

        struct mmsghdr out_msgs[BATCH_SIZE];
        int out_count = 0;

        for (int i = 0; i < num_received; i++) {
            char* payload = buffers[i];
            
            if (payload[0] == (char)0xBB) {
                // Read the Client's IP and Port right out of the payload
                uint32_t client_ip;
                uint16_t client_port;
                memcpy(&client_ip, &payload[4], sizeof(client_ip));
                memcpy(&client_port, &payload[8], sizeof(client_port));
                
                // Reconstruct the destination address dynamically
                memset(&target_addrs[i], 0, sizeof(target_addrs[i]));
                target_addrs[i].sin_family = AF_INET;
                target_addrs[i].sin_addr.s_addr = client_ip;
                target_addrs[i].sin_port = client_port;

                msgs[i].msg_hdr.msg_name = &target_addrs[i];
                msgs[i].msg_hdr.msg_namelen = sizeof(struct sockaddr_in);
                iovecs[i].iov_len = msgs[i].msg_len;
                out_msgs[out_count++] = msgs[i];
            }
        }

        if (out_count > 0) {
            int send_fd = client_fds[thread_id % num_client_threads];
            //int send_fd = storage_fds[thread_id % num_storage_threads]; // TODO: comment this
            sendmmsg(send_fd, out_msgs, out_count, 0);
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <NUM_CLIENT_THREADS> <NUM_STORAGE_THREADS>" << std::endl;
        return -1;
    }

    int num_client_threads = std::stoi(argv[1]);
    int num_storage_threads = std::stoi(argv[2]);
    std::cout << "Client thread number is " << num_client_threads << " and storage thread num is " << num_storage_threads  << std::endl;

    memset(&storage_addr, 0, sizeof(storage_addr));
    storage_addr.sin_family = AF_INET;
    storage_addr.sin_port = htons(STORAGE_NODE_PORT);
    storage_addr.sin_addr.s_addr = inet_addr("10.10.1.4");

    for (int i = 0; i < num_client_threads; i++) {
        client_fds.push_back(create_reuseport_socket(CLIENT_PORT));
    }
    for (int i = 0; i < num_storage_threads; i++) {
        storage_fds.push_back(create_random_port_socket());
    }

    std::vector<std::thread> threads;
    for (int i = 0; i < num_client_threads; ++i) threads.emplace_back(client_worker, i, num_storage_threads);
    for (int i = 0; i < num_storage_threads; ++i) threads.emplace_back(storage_worker, i, num_client_threads);

    for (auto& t : threads) t.join();
    return 0;
}
