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
#include <fstream>
#include <iomanip>

#include <linux/perf_event.h>
#include <sys/syscall.h>

#define SERVER_PORT 8888
#define STORAGE_PORT 9999
#define PAYLOAD_SIZE 13

struct ThreadStats {
    long long total_packets = 0;
    double total_rtt_ms = 0.0;
    long long invalid_responses = 0;
};

// void configure_event(struct perf_event_attr *pe, uint32_t type, uint64_t config){
//     memset(pe, 0, sizeof(struct perf_event_attr));
//     pe->type = type;
//     pe->size = sizeof(struct perf_event_attr);
//     pe->config = config;
//     pe->read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
//     pe->sample_freq = 1000;
//     pe->freq = 1;
//     pe->disabled = 1;
//     pe->sample_type = PERF_SAMPLE_IP | PERF_SAMPLE_CALLCHAIN;
//     pe->exclude_kernel = 0;
//     pe->exclude_user = 0;
//     pe->exclude_hv = 1;
//     pe->wakeup_events = 1; // TODO: wake us up for every 1 sample event?
// }

// int setup_perf(pid_t tid) {
//      (void) tid;
//     //int flags = 0;
//     int fd;
//     //long long count;
//     struct perf_event_attr pe;
//     configure_event(&pe, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CPU_CLOCK);

//     int cpu = -1; // We want to measure perf for this tid on any CPU
//     int group_fd = -1;

//     fd = syscall(SYS_perf_event_open, &pe, tid, cpu, group_fd, 0);
//     return fd;
// }

void client_worker(int duration, ThreadStats& stats, int thread_id, std::string self_ip, std::string server_ip) {
    int sockfd;
    struct sockaddr_in server_addr, client_addr;

    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) return;

    // std::cout << "APPEND REQUEST thread starting with tid = " << thread_id << std::endl;

    // // PERF HERE
    // pid_t tid = syscall(SYS_gettid);
    // int perf_fd = setup_perf(tid);
    // if (perf_fd < 0) {
    //     perror("Error opening perf event");
    //     // Ensure /proc/sys/kernel/perf_event_paranoid allows non-root access if this fails
    //     exit(EXIT_FAILURE);
    // }

    struct timeval tv = {1, 0};
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

    memset(&client_addr, 0, sizeof(client_addr));
    client_addr.sin_family = AF_INET;
    client_addr.sin_port = htons(0); 
    client_addr.sin_addr.s_addr = inet_addr(self_ip.c_str());

    if (bind(sockfd, (const struct sockaddr *)&client_addr, sizeof(client_addr)) < 0) return;

    // --- NEW: Ask the kernel what ephemeral port we were assigned ---
    socklen_t addr_len = sizeof(client_addr);
    getsockname(sockfd, (struct sockaddr *)&client_addr, &addr_len);
    uint32_t my_ip = client_addr.sin_addr.s_addr;
    uint16_t my_port = client_addr.sin_port;

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT); // TODO: Change for whether it sends to server or store
    server_addr.sin_addr.s_addr = inet_addr(server_ip.c_str()); // TODO: Change to change whether it sends to server or store

    char payload[PAYLOAD_SIZE];
    memset(payload, 0xFF, PAYLOAD_SIZE);
    
    // --- NEW: Embed the IP and Port directly into the payload ---
    memcpy(&payload[4], &my_ip, sizeof(my_ip));
    memcpy(&payload[8], &my_port, sizeof(my_port));

    char buffer[8192];
    auto start_time = std::chrono::steady_clock::now();
    auto end_time = start_time + std::chrono::seconds(duration);

    while (std::chrono::steady_clock::now() < end_time) {
        payload[0] = 0xAA;

        auto send_time = std::chrono::steady_clock::now();
        sendto(sockfd, payload, PAYLOAD_SIZE, 0, (const struct sockaddr *)&server_addr, sizeof(server_addr));

	struct sockaddr_in reply_addr;
        socklen_t len = sizeof(reply_addr);
        int n = recvfrom(sockfd, buffer, sizeof(buffer), 0, (struct sockaddr *)&reply_addr, &len);

        if (n > 0) {
            auto recv_time = std::chrono::steady_clock::now();
            // if ((unsigned char)buffer[0] == 0xBB) {
                std::chrono::duration<double, std::milli> rtt = recv_time - send_time;
                stats.total_rtt_ms += rtt.count();
                stats.total_packets++;
            // } 
            // else {
            //     stats.invalid_responses++;
            // }
        }
    }
    close(sockfd);
}

int main(int argc, char* argv[]) {
    if (argc != 6) {
        std::cerr << "Usage: " << argv[0] << " <DURATION_SECONDS> <NUM_THREADS> <SELF_IP> <SERVER_IP>" << std::endl;
        return -1;
    }

    int duration = std::stoi(argv[1]);
    int num_threads = std::stoi(argv[2]);
    std::string self_ip = std::string(argv[3]);
    std::string server_ip = std::string(argv[4]);
    std::string json_name = std::string(argv[5]);

    if (duration <= 0 || num_threads <= 0) {
        std::cerr << "Duration and number of threads must be greater than 0." << std::endl;
        return -1;
    }

    std::vector<std::thread> threads;
    std::vector<ThreadStats> thread_stats(num_threads);

    std::cout << "Spawning " << num_threads << " threads for " << duration << " seconds..." << std::endl;

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(client_worker, duration, std::ref(thread_stats[i]), i, self_ip, server_ip);
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
        std::string filename = json_name + "_" + self_ip + ".json";
        std::ofstream json_file(filename);

        if (json_file.is_open()) {
            // Write the JSON template mapping our variables into the requested fields
            json_file << "{\n";
            json_file << "    \"throughput\": " << std::fixed << throughput_pps << ",\n";
            json_file << "    \"avg_latency\": " << avg_rtt << ",\n";
            json_file << "    \"num_clients\": " << num_threads << ",\n";
            json_file << "    \"batch_size\": 1,\n";
            json_file << "    \"payload_size\": " << PAYLOAD_SIZE << ",\n";
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
