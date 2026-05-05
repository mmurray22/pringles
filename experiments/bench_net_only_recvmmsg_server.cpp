#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#define PORT 8888
#define BATCH_SIZE 150 // Number of packets to grab per system call

int main() {
    int sockfd;
    struct sockaddr_in server_addr;
    
    // Arrays for recvmmsg
    struct mmsghdr msgs[BATCH_SIZE];
    struct iovec iovecs[BATCH_SIZE];
    struct sockaddr_in client_addrs[BATCH_SIZE];
    char buffers[BATCH_SIZE][1024];

    // Create UDP socket
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        std::cerr << "Socket creation failed" << std::endl;
        return -1;
    }

    // Bind to the specified network interface
    const char* iface = "enp65s0f0np0";
    if (setsockopt(sockfd, SOL_SOCKET, SO_BINDTODEVICE, iface, strlen(iface)) < 0) {
        std::cerr << "Warning: Failed to bind to interface " << iface << std::endl;
    }

    memset(&server_addr, 0, sizeof(server_addr));

    // Configure server address
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    server_addr.sin_addr.s_addr = inet_addr("10.10.1.3");

    // Bind the socket to the IP and Port
    if (bind(sockfd, (const struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Bind failed. Check if IP 10.10.1.3 is available." << std::endl;
        close(sockfd);
        return -1;
    }

    std::cout << "Single-threaded Server listening on 10.10.1.3:" << PORT << " via interface " << iface << std::endl;
    std::cout << "Using recvmmsg with a batch size of " << BATCH_SIZE << std::endl;

    // Initialize the msgs structures used by recvmmsg
    memset(msgs, 0, sizeof(msgs));
    for (int i = 0; i < BATCH_SIZE; i++) {
        iovecs[i].iov_base = buffers[i];
        iovecs[i].iov_len = sizeof(buffers[i]);
        msgs[i].msg_hdr.msg_iov = &iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
        msgs[i].msg_hdr.msg_name = &client_addrs[i];
        msgs[i].msg_hdr.msg_namelen = sizeof(client_addrs[i]);
    }

    while (true) {
        // Reset msg_namelen before each call, as recvmmsg modifies it to the actual address length
        for (int i = 0; i < BATCH_SIZE; i++) {
            msgs[i].msg_hdr.msg_namelen = sizeof(client_addrs[i]);
        }
        
        // Receive a batch of packets
        // MSG_WAITFORONE tells the kernel to block until at least 1 packet is ready, 
        // but if more are already in the queue, grab up to BATCH_SIZE.
        int vlen = recvmmsg(sockfd, msgs, BATCH_SIZE, MSG_WAITFORONE, nullptr);
        
        if (vlen < 0) {
            std::cerr << "recvmmsg failed" << std::endl;
            continue;
        }

        // Process each packet in the batch
        for (int i = 0; i < vlen; i++) {
            int msg_len = msgs[i].msg_len;
            
            if (msg_len > 0) {
                // Modify payload: Client sends 0xAA, Server replies 0xBB
                buffers[i][0] = 0xBB;

                // Echo modified packet back to the specific client thread that sent it
                // We pull the source address directly from the mmsghdr array
                sendto(sockfd, buffers[i], msg_len, MSG_CONFIRM, 
                       (struct sockaddr *)&client_addrs[i], msgs[i].msg_hdr.msg_namelen);
            }
        }
    }

    close(sockfd);
    return 0;
}
