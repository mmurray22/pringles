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
#include <fstream>  // Required for file writing
#include <iomanip>  // Required for std::fixed (formatting JSON numbers)

#define SERVER_PORT 8888
#define PAYLOAD_SIZE 100

// Struct to hold individual thread performance data
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

    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

    const char* iface = "enp65s0f1np1";
    if (setsockopt(sockfd, SOL_SOCKET, SO_BINDTODEVICE, iface, strlen(iface)) < 0) {
        // Silent fail for interface binding, useful for local testing
    }

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

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(client_worker, duration, std::ref(thread_stats[i]), i);
    }

    for (auto& t : threads) {
        t.join();
    }

    long long total_aggregate_packets = 0;
    double total_aggregate_rtt_ms = 0.0;
    long long total_invalid_responses = 0;

    for (int i = 0; i < num_threads; ++i) {
        total_aggregate_packets += thread_stats[i].total_packets;
        total_aggregate_rtt_ms += thread_stats[i].total_rtt_ms;
        total_invalid_responses += thread_stats[i].invalid_responses;
    }

    std::cout << "\n--- Aggregate Test Results ---" << std::endl;
    if (total_aggregate_packets > 0) {
        double avg_rtt = total_aggregate_rtt_ms / total_aggregate_packets;
        double throughput_pps = (double)total_aggregate_packets / duration;
        
        std::cout << "Number of Threads: " << num_threads << std::endl;
        std::cout << "Total Valid Packets: " << total_aggregate_packets << std::endl;
        std::cout << "Average Latency (RTT): " << avg_rtt << " ms" << std::endl;
        std::cout << "Aggregate Throughput: " << throughput_pps << " pkts/sec" << std::endl;

        // --- JSON File Generation ---
        std::string filename = std::to_string(num_threads) + "-client-pringles-default.json";
        std::ofstream json_file(filename);

        if (json_file.is_open()) {
            // Write the JSON template mapping our variables into the requested fields
            json_file << "{\n";
            json_file << "    \"agg_tput\": " << std::fixed << throughput_pps << ",\n";
            json_file << "    \"total_avg_latency\": " << avg_rtt << ",\n";
            json_file << "    \"subscribe_delay\": 0.0,\n";
            json_file << "    \"num_clients\": " << num_threads << ",\n";
            json_file << "    \"batch_size\": 1,\n";
            json_file << "    \"payload_size\": " << PAYLOAD_SIZE << ",\n";
            json_file << "    \"num_switches_in_ring\": 1,\n";
            json_file << "    \"num_shards\": 1,\n";
            json_file << "    \"num_servers_per_shard\": 1,\n";
            json_file << "    \"git_hash\": \"1b0aec5ee4e12df008d835842cafe794b9e77615\",\n";
            json_file << "    \"system_name\": \"benchmark\"\n";
            json_file << "}\n";
            
            json_file.close();
            std::cout << "Successfully saved results to: " << filename << std::endl;
        } else {
            std::cerr << "Error: Failed to open " << filename << " for writing." << std::endl;
        }

    } else {
        std::cout << "No valid packets were successfully sent and received." << std::endl;
    }
    
    if (total_invalid_responses > 0) {
        std::cout << "Warning: " << total_invalid_responses << " invalid responses received." << std::endl;
    }

    return 0;
}
