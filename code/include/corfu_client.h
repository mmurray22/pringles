#pragma once

#include "corfu_storage.h"
#include "corfu_sequencer.h"
#include "network.h"
#include "trace.h"
#include "utils.h"
#include "structs.h"
#include <vector>
#include <map>
#include <cstdint>
#include <cstddef>
#include <chrono>

#define TIMEOUT std::chrono::seconds(10)
const std::chrono::seconds MAX_WAIT_TIME(5);

enum SequencerType {
	DUMMY,
	NETWORK,
	MACHINE
};

enum PacketType {
    	append,
    	readentry,
		trim,
        seal,
        gettoken
};

class CorfuClient {
protected:
	uint64_t cid;
	uint64_t curr_epoch = 0;

	// map from map[epoch --> map[ranges --> replica sets in that extent (by ssid)]]
	std::map<uint64_t, std::map<std::pair<uint64_t, uint64_t>, std::vector<std::vector<uint64_t>>>> auxiliary;
    bool projection_sealed = false;

	std::shared_ptr<Network> net;

	uint64_t min_matching_acks = 0;

	std::mutex pkt_q_lock;
	std::map<PacketType, std::queue<std::unique_ptr<char[]>>> pkt_q;
	std::vector<std::string> pkt_types;
	bool end_thread = false;
	bool started_append = false;

	uint64_t payload_size;
	uint64_t batch_size;
	uint64_t num_work_threads;

	std::string send_port;
	std::string recv_port;

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
	std::thread append_thread;
	std::thread duration_thread;
	std::thread execution_thread;
	std::vector<std::thread> cli_threads;
	std::vector<std::thread> recv_threads;

	uint64_t max_duration;
	uint64_t warm_up;
	uint64_t cool_down;
	std::atomic<bool> collect_stats; 

	uint64_t global_thread_id;

	std::array<uint8_t,6> seq_mac;
	std::string seq_ip;

	std::vector<int> get_pkt_eth_types();
	int get_eth_type(uint64_t pkt_type);

	std::pair<uint64_t, std::vector<std::vector<uint64_t>>> map(uint64_t log_idx);

	std::vector<std::string> seq_ips;
	std::vector<std::string> storage_ips;

	uint64_t num_m_per_extent;
	uint64_t num_m_per_rep_set;
	uint64_t extent_size;

	void setup_auxiliary();

public:
	CorfuClient(std::string input_file, uint64_t thread_id);
	~CorfuClient();

	void reconfigure(uint64_t log_idx, CorfuStorage& failing_unit);
	uint32_t append(std::string entry);
	std::string read(uint64_t log_idx);
	bool trim(uint64_t log_idx);
	uint64_t fill(uint64_t idx);

	void wait_to_warmup();
	void wait_to_cooldown();
	void wait_to_finish();
	bool experiment_status();
	void execute(uint64_t thread_id);

	uint64_t getTail();
    void subscribe(uint64_t idx);
};