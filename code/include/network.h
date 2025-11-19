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
#include <unordered_map>
#include <netinet/if_ether.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <net/if.h>
#include <net/ethernet.h>
#include <sys/ioctl.h>
#include <linux/ip.h>
#include <linux/if_packet.h>
#include <sys/epoll.h>

#include "concurrentqueue.h"
#include "readerwriterqueue.h"
#include "atomicops.h"

/*
 * Networking library which implements the low-level connectivity
 * necessary for the log to function
 *
 * It is designed to have multiple senders and one receiver.
 */

const uint64_t MAX_PACKET_SIZE = 8192; // TODO put in the yaml

class Network {
    public:
        /*
         * Network object constructor
         *
         * Arguments TODO update this comment
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
        Network(std::string send_port, 
                std::string recv_port,
                std::string socket_type,
                uint64_t log_level,
                uint64_t batch_size,
                bool batch_on,
                std::string send_interface,
                std::string self_ip,
		uint64_t num_pkt_type,
		bool run_threads);
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
        void add_to_send_queue(std::unique_ptr<char[]> buf, uint64_t packet_type, uint64_t packet_size);

        /*
         * Returns a unique pointer to the received packet at the front of the queue.
	 * Further handling/queueing/manipulation needs to be done at the client/storage server/etc..
         */
        char* read_from_recv_queue();

        /*
         * Artificially indicates to Network object that no more requests will be issued
         */
        void done();
	
	char* recv_packet();


	std::string get_recv_port();

	bool send_udp_packet(std::unique_ptr<char[]> send_packet, uint64_t pkt_len, uint64_t pkt_type, int eth_type, std::string dst_ip, std::string dst_port);
 	bool send_packet(std::unique_ptr<char[]> send_packet, uint64_t pkt_len, uint64_t pkt_type, int eth_type, std::array<uint8_t,6> dst_mac, in_addr_t dst_ip);
 

        /*
         * Update the packet classifiers
         * Useful if the classifiers are receiver IPs and some receivers fail/are changed
         */
        void add_pkt_type(uint64_t pkt_type);
        bool remove_pkt_type(uint64_t pkt_type);
        
    private:

	/* ALL THE ACTUALLY USED NETWORK VARS AND FXNS - MUCH SIMPLER*/ // TODO
        int recv_socket;
        int send_socket;
        struct sockaddr_ll sin;
	std::unique_ptr<struct iphdr> send_ip_hdr; 
	std::unique_ptr<struct ethhdr> send_eth_hdr; 
        std::string send_interface;
        std::string self_ip;

	bool run_threads; 
	std::thread send_thread;
	std::thread recv_thread;

        char* norm_buf;
        std::string socket_type; // Networking protocol you are running
        bool check_socket_type(std::string socket_type);
        const uint64_t BACKLOG = 5;
        std::string SEND_PORT;
        std::string RECV_PORT;
        int setup_listener_socket(std::string curr_ip);
        int setup_talker_socket(std::string dst_ip, std::string dst_port, std::shared_ptr<struct addrinfo>& it);
        int setup_raw_talker_socket();
        void destroy_socket(int s_fd);
        std::shared_ptr<struct addrinfo> get_it(int s_fd);
        std::string get_ip(uint64_t pkt_type, int idx);
        std::unique_ptr<struct ethhdr> create_eth_hdr(int s_fd);
        std::unique_ptr<struct iphdr> create_ip_hdr();

        /** Batching **/
        uint64_t batch_size;
	bool batch_on;

	// Boolean which indicates to sending and receiving threads to cease operation
        bool terminate = false;

	// Sender thread function, parameterized by the packet type the sender is responsible for
        void run_send();
        // Receiver thread 
        void run_recv(int s_fd);
	// checksum for IP packet header construction
        unsigned short checksum(unsigned short *buf, int nwords);         
        // Goes through the steps of stopping and cleaning up all the threads
        void stop_threads();

	/*OLD INFO*/

	std::unordered_map<uint64_t, struct sockaddr_ll> sin_map;
	std::unordered_map<uint64_t, std::vector<std::unique_ptr<struct ethhdr>>> eth_hdr_map; 


        // Lock to serialize access to terminate boolean
        std::mutex lock_terminate;

	bool rcv_q_available = false;
	std::condition_variable rcv_cond;

	std::mutex lock_num_sends_done;
	uint64_t num_sends_done = 0;
	std::mutex lock_num_recv_done;
	uint64_t num_recv_done = 0;
	uint64_t MAX_CLEANUP_TIME = 10;
        uint64_t total_num_threads;


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
        std::unordered_map<uint64_t, std::queue<std::pair<uint64_t, std::unique_ptr<char[]>>>> send_pkt_qs;
        std::mutex send_pkt_qs_mutex;
        
        
        //std::queue<char*> rcv_pkt;
	moodycamel::ConcurrentQueue<char*> rcv_pkt;
        std::mutex rcv_queue_mutex;

	std::map<uint64_t, std::vector<std::string>> pkt_type_to_ip;
	std::unordered_map<uint64_t, std::vector<int>> pkt_type_to_fd;

	std::vector<std::array<uint8_t,6>> mac_addrs;

        uint64_t num_pkt_type = 0;


	std::unordered_map<uint64_t, std::shared_ptr<struct addrinfo>> fd_to_it;
        
        bool pkts_in_queue();
        std::string seq_ip;
        std::string storage_multicast_addr;
        
        bool validate_ip_address(const std::string &ip_addr);
         
        /** Socket handling **/
        };
