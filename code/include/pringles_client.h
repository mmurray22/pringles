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

#include "base_client.h"
#include "measure.h"

const std::chrono::seconds MAX_WAIT_TIME(5);

enum SequencerType {
	DUMMY,
	NETWORK,
	MACHINE
};

enum PacketType {
    	append,
    	readentry,
    	gettail,
	trim,
	dummyappendstream,
	appendstream,
	subscribe
};

/* Client class */
class LogClient : public BaseClient {
    public:
	LogClient(std::string input_file, uint64_t thread_id, uint64_t num_threads);
	~LogClient();
        
	// Append entries to the log
	uint32_t append(std::string payload);
        // Read from idx in the log
	std::string read(uint64_t idx);
        // Get latest committed entry
        uint64_t getTail();
        // Subscribe to get all log updates after supplied index
        void subscribe(uint64_t idx);
        // Garbage collect all log entries up to some index
        bool trim(uint64_t idx);

	void update_stats(bool update_stats_collection);	
	void finish();
        uint64_t get_client_payload_size();

        void wait_to_warmup();
        void wait_to_cooldown();
        void wait_to_finish();

    private:
	
	/**** Variables ****/
	
	// Client identification
	uint64_t cid;
	uint64_t global_thread_id;

	// Network communication information
	std::string switch_ip;
	std::string switch_recv_port;
	std::string self_ip;
	uint64_t client_recv_port;
	std::vector<std::string> stor_ips;
	std::string stor_recv_port;
	uint64_t send_port;
	bool use_switch;
	std::shared_ptr<Network> net;

	// Ack tracking
	uint64_t min_matching_acks = 0;
	uint64_t ack_cntr = 0;

	// Stats tracking
	std::unique_ptr<Stats> stat;
	uint64_t max_duration;
	uint64_t warm_up;
	uint64_t cool_down;
	std::string json_name;
	std::atomic<bool> collect_stats; 
	uint64_t total_packet_cntr;
	uint64_t highest_idx_seen;
	bool end_thread = false;

	// Packet info 
	uint64_t payload_size;
	uint64_t batch_size;
	bool batch_on;
	uint64_t num_work_threads;

	// Subscribe variables
	std::thread subscribe_thread;
	bool subscribe_thread_running = false;
	std::queue<char*> subscribe_queue;
	std::mutex subscribe_queue_lk;

        /* Local list of appended and read log entries and corresponding lock*/
	std::map<uint64_t, std::string> cached_log_entries;
	std::mutex cached_log_lock;

	std::thread recv_thread;
	std::thread duration_thread;

	/**** Functions ****/

        void run_append();
        void wait_for_subscribe(uint64_t idx);

	void pringles_recv_queue();
		

	std::vector<int> get_pkt_eth_types();
	size_t get_size_of_hdr(uint64_t pkt_type);
};
