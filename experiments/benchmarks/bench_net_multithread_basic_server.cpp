#include <iostream>
#include <cstring>
#include <vector>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sched.h>

#include <linux/perf_event.h>
#include <sys/syscall.h>

#define PORT 8888
#define NUM_THREADS 20 // Adjust this based on your CPU cores

void configure_event(struct perf_event_attr *pe, uint32_t type, uint64_t config){
    memset(pe, 0, sizeof(struct perf_event_attr));
    pe->type = type;
    pe->size = sizeof(struct perf_event_attr);
    pe->config = config;
    pe->read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
    pe->sample_freq = 1000;
    pe->freq = 1;
    pe->disabled = 1;
    pe->sample_type = PERF_SAMPLE_IP | PERF_SAMPLE_CALLCHAIN;
    pe->exclude_kernel = 0;
    pe->exclude_user = 0;
    pe->exclude_hv = 1;
    pe->wakeup_events = 1; // TODO: wake us up for every 1 sample event?
}

int setup_perf(pid_t tid) {
    struct perf_event_attr pe;
    configure_event(&pe, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CPU_CLOCK);

    int cpu = -1; // Measure on any CPU
    int group_fd = -1;

    // Pass 'tid' instead of hardcoded 0
    int fd = syscall(SYS_perf_event_open, &pe, tid, cpu, group_fd, 0);
    return fd;
}

void pin_current_thread_linux(int core_id) {
    long num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (num_cores <= 0) {
        num_cores = 1; // Fallback sanity check
    }

    // Create a CPU set structure and clear it
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    // Add the desired core to the CPU set
    // int final_core_id = core_id % 32;
    int final_core_id = core_id % num_cores;
    std::cout << core_id << " " << num_cores << " " << final_core_id << std::endl;
    CPU_SET(final_core_id, &cpuset);

    pthread_t current_thread = pthread_self();

    pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

// Function that each worker thread will run
void worker_server(int thread_id) {
    int sockfd;
    struct sockaddr_in server_addr, client_addr;
    // Each thread gets its own independent buffer to avoid data races
    char buffer[8192]; 

    std::cout << "APPEND REQUEST thread starting with tid = " << thread_id << std::endl;

    // PERF HERE
    // pid_t tid = syscall(SYS_gettid);
    // int perf_fd = setup_perf(tid);
    // if (perf_fd < 0) {
    //     perror("Error opening perf event");
    //     // Ensure /proc/sys/kernel/perf_event_paranoid allows non-root access if this fails
    //     exit(EXIT_FAILURE);
    // }

    pin_current_thread_linux(thread_id);

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

    struct timeval timeout;      
    timeout.tv_sec = 2;  // 5 seconds
    timeout.tv_usec = 0; // 0 microseconds
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        close(sockfd);
	    return;
    }

    // 3. Bind to the specified network interface
    const char* iface = "ens1f1np1"; 
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
            // buffer[0] = 0xBB;

            // Echo the modified packet back to the client
            sendto(sockfd, buffer, n, 0, (const struct sockaddr *)&client_addr, len);
        }
    }

    close(sockfd);
}

int main() {
    // Container to hold our threads
    std::vector<std::thread> threads;

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
