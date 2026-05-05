#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <chrono>
#include <cstdlib>
#include <thread>
#include <vector>
#include <atomic>

#define SERVER_PORT 8888
#define PAYLOAD_SIZE 100

std::atomic<uint64_t> seq_no = 0;

// Struct to hold individual thread performance data
// Kept separate per thread to avoid mutex locking overhead during the hot loop
struct ThreadStats {
    long long total_packets = 0;
    double total_rtt_ms = 0.0;
    long long invalid_responses = 0;
};

// Worker function executed by each thread
void client_worker(int duration, ThreadStats& stats, int thread_id) {
    int sockfd;
    struct sockaddr_in server_addr, client_addr;

    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        std::cerr << "Thread " << thread_id << ": Socket creation failed" << std::endl;
        return;
    }

    // Optional: Add a timeout to recvfrom so threads don't hang forever if a packet drops
    // Because we are blasting multiple threads, a single dropped packet shouldn't freeze a thread.
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

    const char* iface = "enp65s0f1np1";
    setsockopt(sockfd, SOL_SOCKET, SO_BINDTODEVICE, iface, strlen(iface));

    memset(&client_addr, 0, sizeof(client_addr));
    client_addr.sin_family = AF_INET;
    client_addr.sin_port = htons(0); // Port 0 lets the OS assign a unique ephemeral port
    client_addr.sin_addr.s_addr = inet_addr("10.10.1.2");

    if (bind(sockfd, (const struct sockaddr *)&client_addr, sizeof(client_addr)) < 0) {
        std::cerr << "Thread " << thread_id << ": Bind failed." << std::endl;
        close(sockfd);
        return;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    server_addr.sin_addr.s_addr = inet_addr("10.10.1.3");

    char payload[PAYLOAD_SIZE];
    memset(payload, 0xFF, PAYLOAD_SIZE);
    char buffer[1024];

    auto start_time = std::chrono::steady_clock::now();
    auto end_time = start_time + std::chrono::seconds(duration);

    while (std::chrono::steady_clock::now() < end_time) {
        payload[0] = 0xAA;

        auto send_time = std::chrono::steady_clock::now();
        sendto(sockfd, payload, PAYLOAD_SIZE, MSG_CONFIRM, (const struct sockaddr *)&server_addr, sizeof(server_addr));

        socklen_t len = sizeof(server_addr);
        int n = recvfrom(sockfd, buffer, sizeof(buffer), 0, (struct sockaddr *)&server_addr, &len);

        if (n > 0) {
            auto recv_time = std::chrono::steady_clock::now();
            
            if ((unsigned char)buffer[0] == 0xBB) {
                std::chrono::duration<double, std::milli> rtt = recv_time - send_time;
                stats.total_rtt_ms += rtt.count();
                stats.total_packets++;
		seq_no += 1;
            } else {
                stats.invalid_responses++;
            }
        }
    }

    close(sockfd);
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <DURATION_SECONDS> <NUM_THREADS>" << std::endl;
        return -1;
    }

    int duration = std::stoi(argv[1]);
    int num_threads = std::stoi(argv[2]);

    if (duration <= 0 || num_threads <= 0) {
        std::cerr << "Duration and number of threads must be greater than 0." << std::endl;
        return -1;
    }

    std::vector<std::thread> threads;
    std::vector<ThreadStats> thread_stats(num_threads);

    std::cout << "Spawning " << num_threads << " threads for " << duration << " seconds..." << std::endl;

    // Launch all threads
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(client_worker, duration, std::ref(thread_stats[i]), i);
    }

    // Wait for all threads to finish
    for (auto& t : threads) {
        t.join();
    }

    // Aggregate statistics
    long long total_aggregate_packets = 0;
    double total_aggregate_rtt_ms = 0.0;
    long long total_invalid_responses = 0;

    for (int i = 0; i < num_threads; ++i) {
        total_aggregate_packets += thread_stats[i].total_packets;
        total_aggregate_rtt_ms += thread_stats[i].total_rtt_ms;
        total_invalid_responses += thread_stats[i].invalid_responses;
    }

    // Output final results
    std::cout << "\n--- Aggregate Test Results ---" << std::endl;
    if (total_aggregate_packets > 0) {
        double avg_rtt = total_aggregate_rtt_ms / total_aggregate_packets;
        double throughput_pps = (double)total_aggregate_packets / duration;
        
        std::cout << "Number of Threads: " << num_threads << std::endl;
        std::cout << "Total Valid Packets: " << total_aggregate_packets << std::endl;
        std::cout << "Total Invalid Packets: " << total_invalid_responses << std::endl;
        std::cout << "Average Latency (RTT): " << avg_rtt << " ms" << std::endl;
        std::cout << "Aggregate Throughput: " << throughput_pps << " pkts/sec" << std::endl;
        std::cout << "Highest Sequence Number: " << seq_no.load() << std::endl;
    } else {
        std::cout << "No valid packets were successfully sent and received." << std::endl;
    }
    
    if (total_invalid_responses > 0) {
        std::cout << "Warning: " << total_invalid_responses << " invalid responses received." << std::endl;
    }

    return 0;
}
