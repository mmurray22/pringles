#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <chrono>
#include <cstdlib>

#define SERVER_PORT 8888
#define PAYLOAD_SIZE 100

int main(int argc, char* argv[]) {
    // Ensure command line argument is provided
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <DURATION IN SECONDS>" << std::endl;
        return -1;
    }

    int duration = std::stoi(argv[1]);
    if (duration <= 0) {
        std::cerr << "Duration must be greater than 0 seconds." << std::endl;
        return -1;
    }

    int sockfd;
    struct sockaddr_in server_addr, client_addr;

    // Create UDP socket
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        std::cerr << "Socket creation failed" << std::endl;
        return -1;
    }

    // Bind client to the specific interface
    const char* iface = "enp65s0f1np1";
    if (setsockopt(sockfd, SOL_SOCKET, SO_BINDTODEVICE, iface, strlen(iface)) < 0) {
        std::cerr << "Warning: Failed to bind to interface " << iface << ". Note: This often requires sudo." << std::endl;
    }

    // Bind client locally to IP 10.10.1.2
    memset(&client_addr, 0, sizeof(client_addr));
    client_addr.sin_family = AF_INET;
    client_addr.sin_port = htons(0); 
    client_addr.sin_addr.s_addr = inet_addr("10.10.1.2");

    if (bind(sockfd, (const struct sockaddr *)&client_addr, sizeof(client_addr)) < 0) {
        std::cerr << "Bind to client IP 10.10.1.2 failed." << std::endl;
        close(sockfd);
        return -1;
    }

    // Configure the destination (server) address
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    server_addr.sin_addr.s_addr = inet_addr("10.10.1.3");

    // Allocate and initialize 100-byte payload
    char payload[PAYLOAD_SIZE];
    memset(payload, 0xFF, PAYLOAD_SIZE); // Fill with dummy values
    char buffer[1024];

    long long total_packets = 0;
    double total_rtt_ms = 0.0;
    long long invalid_responses = 0; // Track packets that weren't modified correctly

    auto start_time = std::chrono::steady_clock::now();
    auto end_time = start_time + std::chrono::seconds(duration);

    std::cout << "Sending packets for " << duration << " seconds..." << std::endl;

    while (std::chrono::steady_clock::now() < end_time) {
        // Set the first byte of our payload to a specific "request" value
        payload[0] = 0xAA;

        // 1. Record time before send
        auto send_time = std::chrono::steady_clock::now();

        // 2. Send the packet
        sendto(sockfd, payload, PAYLOAD_SIZE, MSG_CONFIRM, (const struct sockaddr *)&server_addr, sizeof(server_addr));

        // 3. Wait for the response (Blocking)
        socklen_t len = sizeof(server_addr);
        int n = recvfrom(sockfd, buffer, sizeof(buffer), 0, (struct sockaddr *)&server_addr, &len);

        // 4. Verify the packet was processed by the server, then record time
        if (n > 0) {
            auto recv_time = std::chrono::steady_clock::now();
            
            // CHECK VERIFICATION VALUE
            // Cast to unsigned char to avoid sign extension issues when comparing hex
            if ((unsigned char)buffer[0] == 0xBB) {
                std::chrono::duration<double, std::milli> rtt = recv_time - send_time;
                total_rtt_ms += rtt.count();
                total_packets++;
            } else {
                // The packet came back, but the server didn't modify it as expected
                invalid_responses++;
            }
        }
    }

    close(sockfd);

    // 5. Print out the final statistics requested
    std::cout << "\n--- Test Results ---" << std::endl;
    if (total_packets > 0) {
        double avg_rtt = total_rtt_ms / total_packets;
        std::cout << "Average Time (RTT): " << avg_rtt << " ms" << std::endl;
        std::cout << "Total Valid Packets Sent & Acknowledged: " << total_packets << std::endl;
    } else {
        std::cout << "No valid packets were successfully sent and received." << std::endl;
    }
    
    if (invalid_responses > 0) {
        std::cout << "Warning: Received " << invalid_responses << " packets missing the server's modification stamp." << std::endl;
    }

    return 0;
}
