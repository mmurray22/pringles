#include <iostream>
#include <cstring>
#include <vector>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#define PORT 8888
#define NUM_THREADS 10 // Adjust this based on your CPU cores

// Function that each worker thread will run
void worker_server(int thread_id) {
    int sockfd;
    struct sockaddr_in server_addr, client_addr;
    // Each thread gets its own independent buffer to avoid data races
    char buffer[1024]; 

    // 1. Create UDP socket
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        std::cerr << "[Thread " << thread_id << "] Socket creation failed\n";
        return;
    }

    // 2. Enable SO_REUSEPORT so multiple threads/sockets can bind to the same port
    int reuse = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "[Thread " << thread_id << "] Failed to set SO_REUSEPORT\n";
        close(sockfd);
        return;
    }

    // 3. Bind to the specified network interface
    const char* iface = "eno1d1"; 
    if (setsockopt(sockfd, SOL_SOCKET, SO_BINDTODEVICE, iface, strlen(iface)) < 0) {
        std::cerr << "[Thread " << thread_id << "] Warning: Failed to bind to interface " 
                  << iface << ". Note: This often requires sudo.\n";
    }

    memset(&server_addr, 0, sizeof(server_addr));
    
    // Configure server address
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    server_addr.sin_addr.s_addr = inet_addr("10.10.1.3"); 

    // 4. Bind the socket (Linux kernel load-balances packets among sockets sharing this port)
    if (bind(sockfd, (const struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "[Thread " << thread_id << "] Bind failed. Check if IP 10.10.1.3 is available.\n";
        close(sockfd);
        return;
    }

    std::cout << "[Thread " << thread_id << "] Server listening on 10.10.1.3:" << PORT << "\n";

    socklen_t len;
    while (true) {
        len = sizeof(client_addr);
        memset(&client_addr, 0, sizeof(client_addr));
        
        // Block and wait to receive a packet
        int n = recvfrom(sockfd, buffer, sizeof(buffer), 0, (struct sockaddr *)&client_addr, &len);
        
        if (n > 0) {
            // MODIFY THE PACKET: Change the first byte to indicate the server processed it
            buffer[0] = 0xBB;

            // Echo the modified packet back to the client
            sendto(sockfd, buffer, n, MSG_CONFIRM, (const struct sockaddr *)&client_addr, len);
        }
    }

    close(sockfd);
}

int main() {
    // Container to hold our threads
    std::vector<std::jthread> threads;

    std::cout << "Spawning " << NUM_THREADS << " worker threads using SO_REUSEPORT..." << std::endl;

    // Launch threads
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back(worker_server, i);
    }

    // The main thread will just block here indefinitely while workers handle packets
    // std::jthread elements will automatically join if main ever exits.
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    return 0;
}
