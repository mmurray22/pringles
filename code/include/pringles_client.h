/*
 * Core assumption: The ringclient protobuf has an object called Entry
 */
#include <vector>
#include <queue>
#include <map>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <chrono>

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
	LogClient(std::string input_file, uint64_t cli_id, uint64_t thread_id);
	~LogClient();
        
	// Append entries to the log
	uint32_t append(std::string entry);
        // Read from idx in the log
	std::string read(uint64_t idx);
        // Get latest committed entry
        uint64_t getTail();
        // Subscribe to get all log updates after supplied index
        void subscribe(uint64_t idx);
        // Garbage collect all log entries up to some index
        bool trim(uint64_t idx);
	
	bool experiment_status();
    private:
	/**** Variables ****/
	uint64_t min_matching_acks = 0;
	uint64_t num_pkt_types = 0; 
	/* Receive queue which slots messages */
	std::mutex pkt_q_lock;
	std::map<PacketType, std::queue<std::unique_ptr<char[]>>> pkt_q;
	std::vector<std::string> pkt_types;
	bool end_thread = false;


	std::mutex next_idx_lock;
	bool message_available;
	std::condition_variable message_cond;
	std::map<int64_t, uint64_t> append_nonce_idx_map; // TODO change int64_t to uint64_t
	std::map<int64_t, std::pair<uint64_t, std::map<uint64_t, uint64_t>>> append_ack_map;

	/* Hash/ID of pending append entries */
        std::vector<uint64_t> pending_append_entries;
        std::vector<uint64_t> pending_read_entries;

        /* Local list of appended and read log entries and corresponding lock*/
	std::map<uint64_t, std::string> cached_log_entries;
	std::mutex cached_log_lock;

	/* Protocol types */
	SequencerType seq;
	//StorageType stor;

	std::thread recv_thread;
	std::thread duration_thread;

	std::unique_ptr<Stats> stat;
	uint64_t max_duration;
	
	/**** Functions ****/
	uint32_t wait_for_append(PacketType pkt_type, uint32_t nonce);       
        void wait_for_subscribe(uint64_t idx, uint64_t pkt_type);
        void wait_for_finish();
	std::unique_ptr<char[]> create_pkt(PacketType pkt_type, 
		                           uint32_t nonce,
				           std::optional<std::string> entry = std::nullopt,
			   	           std::optional<int64_t> idx = 0);
	void pringles_recv_queue();
		

	std::vector<int> get_pkt_eth_types();
	size_t get_size_of_hdr(uint64_t pkt_type);
	int get_eth_type(uint64_t pkt_type);
};
