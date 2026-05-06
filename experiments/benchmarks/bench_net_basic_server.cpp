#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#define PORT 8888

int main() {
    int sockfd;
    struct sockaddr_in server_addr, client_addr;
    char buffer[1024];

    // Create UDP socket
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        std::cerr << "Socket creation failed" << std::endl;
        return -1;
    }

    // Bind to the specified network interface
    const char* iface = "enp65s0f0np0";
    if (setsockopt(sockfd, SOL_SOCKET, SO_BINDTODEVICE, iface, strlen(iface)) < 0) {
        std::cerr << "Warning: Failed to bind to interface " << iface << ". Note: This often requires sudo." << std::endl;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    memset(&client_addr, 0, sizeof(client_addr));

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

    std::cout << "Server listening on 10.10.1.3:" << PORT << " via interface " << iface << std::endl;

    socklen_t len;
    while (true) {
        len = sizeof(client_addr);
        
        // Block and wait to receive a packet
        int n = recvfrom(sockfd, buffer, sizeof(buffer), 0, (struct sockaddr *)&client_addr, &len);
        
        if (n > 0) {
            // MODIFY THE PACKET: Change the first byte to indicate the server processed it
            // We expect the client to send 0xAA, so we change it to 0xBB
            buffer[0] = 0xBB;

            // Echo the modified packet back to the client
            sendto(sockfd, buffer, n, MSG_CONFIRM, (const struct sockaddr *)&client_addr, len);
        }
    }

    close(sockfd);
    return 0;
}
