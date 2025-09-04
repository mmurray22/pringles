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

/*
 * Networking library which implements the low-level connectivity
 * necessary for the log to function
 *
 * It is designed to have multiple senders and one receiver.
 */
class Network {
    public:
        /*
         * Network object constructor
         *
         * Arguments
         * ---------
         * maxThreads - Size of the threadpool for sending threads TODO might be deprecated
         * seq_ip - IP address of the sequencer
         * storage_multicast_addr - address to allow for multicast to all storage nodes
         * send_port - single send port TODO only one?
         * recv_port - single receive port
         * socket_type - indicates whether we want the OS to create a UDP pkt or we create a RAW pkt
         * log_level - indicates how many logging statements should be printed to console
         * batch_size - maximum size of  the a batch in bytes 
         * send_interface - the network device the network instance will use 
         * src_ip - the IP of the machine this network object currently lives on
         * pkt_types - these are the classes that packets will be sorted into when sent/received
         */
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

        /*
         * Appends buf, a pointer to the payload, to the queue
         *
         * Arguments
         * ---------
         * buf - A pointer to a protobuf message which will be the payload of the outgoing pkt
         * packet_type - Packet classifier label which will determine how the packet is 
         *               stored in the queue
         */
        void add_to_send_queue(std::unique_ptr<std::string> buf, std::string packet_type);

        /*
         * TODO Need to review how the receive queue works
         */
        std::unique_ptr<std::string> read_from_recv_queue();

        /*
         * Artificially indicates to Network object that no more requests will be issued
         */
        void done();

        /*
         * Update the packet classifiers
         * Useful if the classifiers are receiver IPs and some receivers fail/are changed
         */
        void add_pkt_type(std::string pkt_type);
        bool remove_pkt_type(std::string pkt_type);
        
    private:
        // Logging protocol the network object is being used for
        ClientType protocol_type;
        
        // checksum for IP packet header construction
        unsigned short checksum(unsigned short *buf, int nwords);         

        // Lock to serialize access to terminate boolean
        std::mutex lock_terminate;
        // Boolean which indicates to sending and receiving threads to cease operation
        bool terminate = false;
        
        uint64_t total_num_threads;

        // Goes through the steps of stopping and cleaning up all the threads
        void stop_threads();

        // Sender thread function, parameterized by the packet type the sender is responsible for
        void run_send(std::string pkt_type);
        // Receiver thread 
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
        
        /** IP Address + Socket Management **/
        std::string send_interface;
        std::string src_ip;
        std::shared_ptr<struct addrinfo> seq_it; 
        std::string seq_ip;
        std::string storage_multicast_addr;
        std::shared_ptr<struct addrinfo> storage_it;
        int storage_socket;
        int storage_recv_socket;
        int seq_socket;
        int seq_recv_socket;
        bool validate_ip_address(const std::string &ip_addr);
         
        /** Socket handling **/
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

        /** Batching **/
        uint64_t batch_size;
};
