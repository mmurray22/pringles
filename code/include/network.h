#include <queue>
#include <vector>
#include <string>
#include <cstdint>
#include <mutex>
#include <thread>
#include <condition_variable>

class Network {
    public:
        Network(uint64_t maxThreads, std::string ip_file);
        ~Network();
        void add_to_send_queue(char* buf);
        char* read_from_recv_queue();
        void run_send();
        void run_recv();
        std::string update_ip_addrs();
        // TODO add: message formatting, custom header creation
        
    private:
        // Thread
        std::mutex lock_terminate;
        bool terminate = false;
        uint64_t total_num_threads;
        void stop_threads();

        // Send Thread pool
        std::vector<std::thread> send_threads;
        std::mutex lock_send_thread_queue;
        std::condition_variable mutex_condition;
       
        // Recv Thread pool
        const uint64_t MAX_POLL_TIME = 100; // milliseconds
        std::vector<std::thread> recv_threads;
        std::mutex lock_recv_thread_queue;

        // IP Address Management
        std::string chosen_ip_addr; // Needs to be external IP
        std::vector<std::string> all_ip_addrs;
        uint64_t curr_ip_addrs_idx;
        std::mutex ip_addrs_idx_mutex;
               
        // Packet queues & processing
        /* Send Packet queue
         * Assumption: All packets in the queue are of size > 0
         */
        std::queue<char*> send_pkt;
        std::mutex send_queue_mutex;
        std::queue<char*> rcv_pkt;
        std::mutex rcv_queue_mutex;
        void send(char* buf);
        bool pkts_in_queue();

        // Socket handling
        const uint64_t BACKLOG = 5;
        int setup_socket(std::string curr_ip, bool recv_socket);
        void destroy_socket(int s_fd);
};
