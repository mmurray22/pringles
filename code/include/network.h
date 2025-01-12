#include <queue>
#include <vector>
#include <string>
#include <cstdint>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <utility>

class Network {
    public:
        Network(uint64_t maxThreads, 
                std::string ip_file, 
                std::string send_port, 
                uint64_t protocol_id);
        ~Network();
        void add_to_send_queue(std::unique_ptr<std::string> buf, std::string packet_type = "", int64_t nonce = -1, int64_t cid = -1);
        std::unique_ptr<std::string> read_from_recv_queue();
        void done();
        std::string update_ip_addrs();
        // TODO add: message formatting, custom header creation
        
    private:
        // Thread
        const uint64_t BUF_SIZE = 1000;
        std::mutex lock_terminate;
        bool terminate = false;
        uint64_t total_num_threads;
        void stop_threads();
        void run_send();
        void run_recv();

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
               
        // Packet queues & relevant processing data structures
        /* Send Packet queue
         * Assumption: All packets in the queue are of size > 0
         */
        std::queue<std::pair<std::string, std::unique_ptr<std::string>>> send_pkt;
        std::mutex send_queue_mutex;

        std::queue<std::unique_ptr<std::string>> rcv_pkt;
        std::mutex rcv_queue_mutex;
        
        bool pkts_in_queue();

        // Socket handling
        const uint64_t BACKLOG = 5;
        std::string SEND_PORT;
        int setup_listener_socket(std::string curr_ip, bool recv_socket);
        int setup_talker_socket(std::string curr_ip, bool recv_socket, std::unique_ptr<struct addrinfo>& it);
        void destroy_socket(int s_fd);

        // Headers
        uint64_t protocol_id; // Networking protocol you are running
};
