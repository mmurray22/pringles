#include <vector>
#include <optional>
#include <set>
#include "network.h"
#include "measure.h"
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_unordered_set.h>

const uint64_t MAX_TIMEOUT = 100;
const uint64_t MAX_POLL_TIME = 100;
//const uint64_t MAX_PACKET_SIZE = 5000;

class LogSoftwareSwitch {

    public:
	LogSoftwareSwitch(std::string input_file, uint64_t switch_id);
	~LogSoftwareSwitch();

	bool recover_switch();
	bool store_sub(std::string client_ip, std::string recv_port, uint32_t stream_id);
	std::string get(uint64_t idx);
	void change_view(uint64_t new_view_num);
	void wait_to_finish();
	void run_client_send();

    private:
        uint64_t switch_id;
        std::shared_ptr<Network> net;
        
        std::string switch_ip;
	std::array<uint8_t,6> switch_mac;
	std::string switch_recv_port;
	std::string stor_receive_port;
	std::vector<std::string> stor_ips;
	bool use_store;
	bool use_shard;

	// Shards
	bool use_shards;
	uint64_t next_available_shard = 0;
        tbb::concurrent_vector<tbb::concurrent_vector<std::string>> all_shards;
        tbb::concurrent_vector<std::string> all_shards_multicast;
        tbb::concurrent_hash_map<uint64_t, uint64_t> stream_id_to_shard_id;
        tbb::concurrent_hash_map<uint64_t, uint64_t> seq_idx_to_shard_id;

	// Stream variables 
	bool use_streams;
        tbb::concurrent_hash_map<uint64_t, tbb::concurrent_unordered_set<uint64_t>> concurrent_stream_tracker;

	uint64_t view_num;
	uint64_t max_duration;
	bool end_thread = false;

	std::thread recv_thread;
	std::thread append_resp_thread;
	std::thread read_req_thread;
	std::thread read_resp_thread;
	std::thread tail_req_thread;
	std::thread sub_thread;
	std::atomic<uint64_t> max_idx; 
	tbb::concurrent_vector<std::thread> send_threads;
	tbb::concurrent_vector<std::thread> append_req_threads;
	uint64_t num_append_req_threads;

	tbb::concurrent_vector<std::vector<std::string>> subscribe_stor;
	tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<std::vector<std::string>>> stream_subscribe_stor;

	// Acknowledgement counting
	tbb::concurrent_hash_map<uint64_t, uint64_t> ack_map;
	uint64_t ack_threshold;
	
	tbb::concurrent_vector<int> batch_socket_vec;	
	std::condition_variable append_req_cv;
	std::mutex append_req_q_mutex;
	tbb::concurrent_queue<char*> append_req_q;

	std::condition_variable read_req_cv;
	std::mutex read_req_q_mutex;
	tbb::concurrent_queue<char*> read_req_q;
	
	std::condition_variable append_resp_cv;
	std::mutex append_resp_q_mutex;
	tbb::concurrent_queue<char*> append_resp_q;

	std::condition_variable read_resp_cv;
	std::mutex read_resp_q_mutex;
	tbb::concurrent_queue<char*> read_resp_q;

	std::condition_variable tail_req_cv;
	std::mutex tail_req_q_mutex;
	tbb::concurrent_queue<char*> tail_req_q;
	
	std::condition_variable sub_cv;
	std::mutex sub_q_mutex;
	tbb::concurrent_queue<char*> sub_q;

	std::condition_variable cli_send_cv;
	std::mutex cli_send_q_mutex;
	tbb::concurrent_queue<char*> cli_send_q;

	// Thread functions
	void receiver();
        void append_request(int append_port, std::unique_ptr<Network> append_net);
	void append_response(int append_port, std::unique_ptr<Network> append_net);
	void read_request();
	void read_response();
	void tail_request();
	void subscribe_request();
	
	// Helper functions
	std::string get_quad_ip(uint32_t ip_addr);
	void send_subscriber_pkts(tbb::concurrent_vector<std::vector<std::string>> sub_it, size_t num_subscribers, uint64_t reply_pkt_size, char* recv_ptr);
};
