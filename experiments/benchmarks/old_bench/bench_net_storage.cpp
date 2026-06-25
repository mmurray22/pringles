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

#define STORAGE_PORT 9999
#define BATCH_SIZE 512
#define BUF_SIZE 1024

// Worker function for each storage thread
void storage_worker(int thread_id, std::string self_ip) {
    std::cout << "New storage worker!" << std::endl;
    int sockfd;
    struct sockaddr_in storage_addr;

    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        std::cerr << "Thread " << thread_id << ": Socket creation failed" << std::endl;
        return;
    }

    // Enable SO_REUSEPORT so multiple threads can listen on port 9999
    int optval = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) < 0) {
        std::cerr << "Thread " << thread_id << ": setsockopt SO_REUSEPORT failed" << std::endl;
        return;
    }

    //int busy_poll_us = 50; // Spin for 50 microseconds before sleeping
    //setsockopt(sockfd, SOL_SOCKET, SO_BUSY_POLL, &busy_poll_us, sizeof(busy_poll_us));

    memset(&storage_addr, 0, sizeof(storage_addr));
    storage_addr.sin_family = AF_INET;
    storage_addr.sin_port = htons(STORAGE_PORT);
    storage_addr.sin_addr.s_addr = inet_addr(self_ip.c_str());

    if (bind(sockfd, (const struct sockaddr *)&storage_addr, sizeof(storage_addr)) < 0) {
        std::cerr << "Thread " << thread_id << ": Bind failed." << std::endl;
        return;
    }

    struct mmsghdr msgs[BATCH_SIZE];
    struct iovec iovecs[BATCH_SIZE];
    struct sockaddr_in sender_addrs[BATCH_SIZE];
    char buffers[BATCH_SIZE][BUF_SIZE];

    // Initialize structures
    memset(msgs, 0, sizeof(msgs));
    for (int i = 0; i < BATCH_SIZE; i++) {
        iovecs[i].iov_base = buffers[i];
        msgs[i].msg_hdr.msg_name = &sender_addrs[i];
        msgs[i].msg_hdr.msg_iov = &iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }

    while (true) {
        // Reset capacities for the next batch
        for (int i = 0; i < BATCH_SIZE; i++) {
            msgs[i].msg_hdr.msg_namelen = sizeof(struct sockaddr_in);
            iovecs[i].iov_len = BUF_SIZE;
        }


        int num_received = recvmmsg(sockfd, msgs, BATCH_SIZE, MSG_WAITFORONE, NULL);
        if (num_received < 0) continue;

	std::cout << "Server packet!" << std::endl;

        for (int i = 0; i < num_received; i++) {
            if (msgs[i].msg_len > 0) {
                // Change the payload flag to BB, indicating storage processed it.
                // We do NOT touch the rest of the payload, preserving the Server's TX ID.
                buffers[i][0] = 0xBB;
                iovecs[i].iov_len = msgs[i].msg_len;
            }
        }
        
        // Blast the batch back to the Proxy server
        sendmmsg(sockfd, msgs, num_received, 0);
    }
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <NUM_THREADS> <SELF_IP>" << std::endl;
        return -1;
    }

    int num_threads = std::stoi(argv[1]);
    std::string self_ip = std::string(argv[2]);

    if (num_threads <= 0) {
        std::cerr << "Number of threads must be greater than 0." << std::endl;
        return -1;
    }

    std::cout << "Starting Storage Node on " << self_ip << ":" << STORAGE_PORT 
              << " with " << num_threads << " thread(s)..." << std::endl;

    std::vector<std::thread> threads;

    // Spawn the requested number of worker threads
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(storage_worker, i, self_ip);
    }

    // Keep the main thread alive
    for (auto& t : threads) {
        t.join();
    }

    return 0;
}
