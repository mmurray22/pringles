#define _GNU_SOURCE
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#define PORT 8888
#define BATCH_SIZE 100
#define BUF_SIZE 1024

int main() {
    int sockfd;
    struct sockaddr_in server_addr;

    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        std::cerr << "Socket creation failed" << std::endl;
        return -1;
    }

    const char* iface = "enp65s0f0np0";
    setsockopt(sockfd, SOL_SOCKET, SO_BINDTODEVICE, iface, strlen(iface));

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    server_addr.sin_addr.s_addr = inet_addr("10.10.1.3");

    if (bind(sockfd, (const struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Bind failed." << std::endl;
        close(sockfd);
        return -1;
    }

    std::cout << "Fully Batched Server listening on 10.10.1.3:" << PORT 
              << " (Using recvmmsg AND sendmmsg)" << std::endl;

    struct mmsghdr msgs[BATCH_SIZE];
    struct iovec iovecs[BATCH_SIZE];
    struct sockaddr_in client_addrs[BATCH_SIZE];
    char buffers[BATCH_SIZE][BUF_SIZE];

    memset(msgs, 0, sizeof(msgs));
    
    // Wire up the structures once
    for (int i = 0; i < BATCH_SIZE; i++) {
        iovecs[i].iov_base = buffers[i];
        iovecs[i].iov_len = BUF_SIZE;

        msgs[i].msg_hdr.msg_name = &client_addrs[i];
        msgs[i].msg_hdr.msg_namelen = sizeof(struct sockaddr_in);
        msgs[i].msg_hdr.msg_iov = &iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }

    while (true) {
        // 1. Receive a batch of packets from various client threads
        int num_received = recvmmsg(sockfd, msgs, BATCH_SIZE, MSG_WAITFORONE, NULL);
        
        if (num_received < 0) continue;

        // 2. Process the batch and prepare for sending
        for (int i = 0; i < num_received; i++) {
            if (msgs[i].msg_len > 0) {
                // Modify the payload
                buffers[i][0] = 0xBB;
                
                // IMPORTANT: Tell sendmmsg exactly how many bytes to send back
                // by updating the iovec length to match what we received.
                iovecs[i].iov_len = msgs[i].msg_len;
            }
        }

        // 3. Send the entire batch back in a single system call!
        // The kernel looks at msgs[i].msg_hdr.msg_name to route each packet to the correct thread.
        sendmmsg(sockfd, msgs, num_received, 0);

        // 4. Reset structure lengths for the next recvmmsg call
        for (int i = 0; i < num_received; i++) {
            msgs[i].msg_hdr.msg_namelen = sizeof(struct sockaddr_in); // Reset address length
            iovecs[i].iov_len = BUF_SIZE;                             // Reset buffer capacity
        }
    }

    close(sockfd);
    return 0;
}
