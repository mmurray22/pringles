/*
 * Core assumption: The ringclient protobuf has an object called Entry
 */
#include <vector>
#include <queue>
#include <map>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <chrono>
#include <atomic>
#include <string>
#include <cstdint>
#include <memory>
#include <queue>

#include "network.h"
#include "measure.h"

/* Client class */
class LogClient {
    public:
	LogClient(std::string input_file, uint64_t thread_id, uint64_t recv_port_offset);
	~LogClient();
        
	// Append entries to the log
	uint64_t append(std::string entry);
        // Read from idx in the log
	std::string read(uint64_t idx);
        // Get latest committed entry
        uint64_t getTail();
        // Subscribe to get all log updates after supplied index
        void subscribe(uint64_t idx);
        // Garbage collect all log entries up to some index
        bool trim(uint64_t idx);
	
	
	uint64_t append_stream(std::string entry, uint32_t stream_id);
	std::string read_stream(uint64_t idx, uint32_t stream_id);
	void subscribe_stream(uint32_t stream_id);

        void wait_to_warmup();
        void wait_to_cooldown();
        void wait_to_finish(bool is_append);
	bool experiment_status();
	void launch_append_execute();

    private:
	/**** Variables ****/
	uint64_t cid;
	uint64_t thread_id;
	std::shared_ptr<Network> net;

	/* Cluster Information */
	bool use_switch;
	std::string switch_ip;
	std::vector<std::string> stor_ips;
	std::array<uint8_t,6> switch_mac;
	std::array<uint8_t,6> seq_mac;
	std::string seq_ip;
	std::string switch_receive_port;
	std::string stor_receive_port;
	std::string client_recv_port;

	uint64_t min_matching_acks = 0;
	uint64_t num_pkt_types = 0; 
	
	/* Receive queue which slots messages */
	bool end_thread = false;
	bool started_append = false;

	std::string payload;
	uint64_t payload_size;
	uint64_t batch_size;
	uint64_t num_work_threads;

	uint64_t dummy_idx;
	std::mutex dummy_idx_lock;

	std::mutex num_ready_bytes_lock;
	uint64_t num_ready_bytes;
	std::condition_variable batch_ready_cond;

	std::mutex append_entries_lock;
	std::vector<std::string> append_entries;
	std::condition_variable append_cond;

	std::mutex next_idx_lock;
	bool message_available;
	std::unordered_map<int64_t, int64_t> append_nonce_idx_map; // TODO revisit types
	std::map<int64_t, std::mutex> append_nonce_lock_map; // TODO revisit types
	std::map<int64_t, std::condition_variable> append_cond_map; // TODO revisit types
	//std::map<int64_t, std::pair<uint64_t, std::map<uint64_t, uint64_t>>> append_ack_map;
	//std::unordered_map<int64_t, std::unordered_map<uint64_t, uint64_t>> append_ack_map;
	
	std::unordered_map<uint32_t, uint64_t> append_ack_map;
	
	/* Variables for receving thread */
	std::thread recv_thread;
	std::condition_variable append_resp_cv;
	std::mutex append_resp_q_mutex;
	tbb::concurrent_queue<char*> append_resp_q;

	std::condition_variable read_resp_cv;
	std::mutex read_resp_q_mutex;
	tbb::concurrent_queue<char*> read_resp_q;

	std::condition_variable subscribe_resp_cv;
	std::mutex subscribe_resp_q_mutex;
	tbb::concurrent_queue<char*> subscribe_resp_q;

	std::condition_variable tail_resp_cv;
	std::mutex tail_resp_q_mutex;
	tbb::concurrent_queue<char*> tail_resp_q;

	/* Variables for subscribe thread */
	std::thread subscribe_thread; // TODO using same thread for streams and non streams
	bool subscribe_thread_running;

        /* Local list of appended and read log entries and corresponding lock*/
	std::map<uint64_t, std::string> cached_log_entries; // TODO concurrent_hash_map
	std::mutex cached_log_lock;

	/* Protocol types */
	uint64_t ring_view;

	std::thread append_test_thread;
	bool testing_append;
	std::thread read_test_thread;
	bool testing_read;
	std::thread duration_thread;
	std::thread execution_thread;
	std::vector<std::thread> cli_threads;
	std::vector<std::thread> recv_threads;

	/* Information for Experiments */
	std::unique_ptr<Stats> append_stat;
	std::unique_ptr<Stats> read_stat;
	uint64_t max_duration;
	uint64_t warm_up;
	uint64_t cool_down;
	std::atomic<bool> collect_stats; 
	uint64_t global_thread_id;
	uint64_t dur;

	// Append Experiments
	uint32_t append_nonce;
	uint64_t append_cntr;
	uint64_t highest_idx_seen;
	std::unique_ptr<struct ring_type> append_type_hdr;
	std::unique_ptr<struct ring_append_entry> append_entry_hdr;

	// Read Experiments
	uint32_t read_nonce;
	uint64_t read_cntr;
        std::unique_ptr<struct ring_type> read_type_hdr;
	std::unique_ptr<struct ring_read_entry> read_entry_hdr;

	// Subscribe
        std::unique_ptr<struct ring_type> sub_type_hdr;
	std::unique_ptr<struct ring_subscribe_entry> sub_entry_hdr;

	// Tail
	uint32_t tail_nonce;
        std::unique_ptr<struct ring_type> tail_type_hdr;
	std::unique_ptr<struct ring_tail_req> tail_req_hdr;
	
	/**** Functions ****/
	void receiver();
        void wait_for_subscribe(uint64_t idx);
        void wait_for_stream_subscribe(uint32_t stream_id);
        void execute_append();
};
