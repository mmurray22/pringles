#define _GNU_SOURCE
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>
#include <vector>
#include <string>
#include <atomic>

#define PORT 8888
#define BATCH_SIZE 64
#define BUF_SIZE 1024

std::atomic<uint64_t> seq_no = 0;

// The worker function executed by every server thread
void server_worker(int thread_id) {
    int sockfd;
    struct sockaddr_in server_addr;

    // 1. Every thread creates its own independent socket
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        std::cerr << "Thread " << thread_id << ": Socket creation failed" << std::endl;
        return;
    }

    // 2. CRITICAL: Enable SO_REUSEPORT
    // This tells the kernel "Let me bind to 8888, even if other threads are already bound to it, 
    // and please load-balance incoming packets among us."
    int opt = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        std::cerr << "Thread " << thread_id << ": SO_REUSEPORT failed" << std::endl;
        close(sockfd);
        return;
    }

    const char* iface = "enp65s0f0np0";
    if (setsockopt(sockfd, SOL_SOCKET, SO_BINDTODEVICE, iface, strlen(iface)) < 0) {
        // Silent failure here just in case interface doesn't match testing env
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    server_addr.sin_addr.s_addr = inet_addr("10.10.1.3");

    // 3. Bind the socket
    if (bind(sockfd, (const struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Thread " << thread_id << ": Bind failed." << std::endl;
        close(sockfd);
        return;
    }

    std::cout << "Server Thread " << thread_id << " listening on 10.10.1.3:" << PORT << std::endl;

    // 4. Set up thread-local batching structures (no mutexes needed!)
    struct mmsghdr msgs[BATCH_SIZE];
    struct iovec iovecs[BATCH_SIZE];
    struct sockaddr_in client_addrs[BATCH_SIZE];
    char buffers[BATCH_SIZE][BUF_SIZE];

    memset(msgs, 0, sizeof(msgs));
    
    for (int i = 0; i < BATCH_SIZE; i++) {
        iovecs[i].iov_base = buffers[i];
        iovecs[i].iov_len = BUF_SIZE;

        msgs[i].msg_hdr.msg_name = &client_addrs[i];
        msgs[i].msg_hdr.msg_namelen = sizeof(struct sockaddr_in);
        msgs[i].msg_hdr.msg_iov = &iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }

    // 5. The Hot Loop
    while (true) {
        int num_received = recvmmsg(sockfd, msgs, BATCH_SIZE, MSG_WAITFORONE, NULL);
        if (num_received < 0) continue;

        for (int i = 0; i < num_received; i++) {
            if (msgs[i].msg_len > 0) {
                buffers[i][0] = 0xBB;
                iovecs[i].iov_len = msgs[i].msg_len;
		seq_no += 1;
            }
        }

        sendmmsg(sockfd, msgs, num_received, 0);

        for (int i = 0; i < num_received; i++) {
            msgs[i].msg_hdr.msg_namelen = sizeof(struct sockaddr_in);
            iovecs[i].iov_len = BUF_SIZE;
        }
    }

    close(sockfd);
}

int main(int argc, char* argv[]) {
    // Take number of threads as command line argument
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <NUM_SERVER_THREADS>" << std::endl;
        return -1;
    }

    int num_threads = std::stoi(argv[1]);
    if (num_threads <= 0) {
        std::cerr << "Number of threads must be greater than 0." << std::endl;
        return -1;
    }

    std::cout << "Starting Server with " << num_threads << " threads using SO_REUSEPORT..." << std::endl;

    std::vector<std::thread> threads;

    // Spawn the requested number of server threads
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(server_worker, i);
    }

    // Wait forever (until stopped via Ctrl+C)
    for (auto& t : threads) {
        t.join();
    }

    return 0;
}
