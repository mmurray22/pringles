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
#include <map>
#include "structs.h"

class Network {
    public:
        Network(uint64_t maxThreads,
                std::string seq_ip,
                std::string storage_multicast_addr,
                std::string send_port, 
                std::string recv_port,
                std::string socket_type,
                uint64_t log_level,
                uint64_t batch_size,
                std::string send_interface,
                std::string src_ip,
                std::vector<std::string> pkt_types);
        ~Network();
        void add_to_send_queue(std::unique_ptr<std::string> buf, std::string packet_type);
        std::unique_ptr<std::string> read_from_recv_queue();
        void done();
        //std::string update_ip_addrs();
        
        // TODO add: message formatting, custom header creation
        
    private:
        const int64_t CUSTOM_IP_PROTOCOL = 4;          
        ClientType protocol_type;
        // Headers
        
        unsigned short checksum(unsigned short *buf, int nwords); // checksum for IP packet header construction
        
        // Thread
        const uint64_t BUF_SIZE = 1000;
        std::mutex lock_terminate;
        bool terminate = false;
        uint64_t total_num_threads;
        void stop_threads();
        void run_send(std::string pkt_type);
        void run_recv(int s_fd);

        // Send Thread pool
        std::vector<std::thread> send_threads;
        std::mutex lock_send_thread_queue;
        std::condition_variable mutex_condition;
       
        // Recv Thread pool
        const uint64_t MAX_POLL_TIME = 100; // milliseconds
        std::vector<std::thread> recv_threads;
        std::mutex lock_recv_thread_queue;

              
        // Packet queues & relevant processing data structures
        /* Send Packet queues
         * Assumption: All packets in the queue are of size > 0
         */
        std::map<std::string, std::queue<std::unique_ptr<std::string>>> send_pkt_qs;
        std::mutex send_pkt_qs_mutex;
        
        std::queue<std::unique_ptr<std::string>> rcv_pkt;
        std::mutex rcv_queue_mutex;
        
        bool pkts_in_queue();
        
        // IP Address + Socket Management
        std::string send_interface;
        std::string src_ip;
        std::shared_ptr<struct addrinfo> seq_it; 
        std::string seq_ip;
        std::string storage_multicast_addr;
        //std::vector<std::string> storage_ip_addrs;
        std::shared_ptr<struct addrinfo> storage_it;
        int storage_socket;
        int storage_recv_socket;
        //std::vector<int> storage_sockets;
        int seq_socket;
        int seq_recv_socket;
        //std::mutex ip_addrs_idx_mutex;
         
        // Socket handling
        std::string socket_type; // Networking protocol you are running
        bool check_socket_type(std::string socket_type);
        const uint64_t BACKLOG = 5;
        std::string SEND_PORT;
        std::string RECV_PORT;
        int setup_listener_socket(std::string curr_ip);
        int setup_talker_socket(std::string curr_ip, std::shared_ptr<struct addrinfo>& it);
        int setup_raw_talker_socket();
        void destroy_socket(int s_fd);
        int get_socket(std::string pkt_type, ClientType protocol_type);
        std::shared_ptr<struct addrinfo> get_it(int s_fd);
        std::string get_ip(int s_fd);
        std::unique_ptr<struct ethhdr> create_eth_hdr(int s_fd, std::string pkt_type);
        std::unique_ptr<struct iphdr> create_ip_hdr(std::string dst_ip, size_t size_of_hdr, unsigned short* pkt);

        // Batching
        uint64_t batch_size;
};
