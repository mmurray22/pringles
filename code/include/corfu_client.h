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
#include "measure.h"

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
	bool started_append = false;

	std::string send_port;
	std::string recv_port;

	/* Protocol types */
	SequencerType seq;
	//StorageType stor;

	std::thread recv_thread;
	std::thread append_thread;
	std::thread duration_thread;
	
	std::vector<std::thread> cli_threads;
	std::vector<std::thread> recv_threads;

	uint64_t global_thread_id;
	uint64_t thread_id;

	std::vector<int> get_pkt_eth_types();
	int get_eth_type(uint64_t pkt_type);

	std::pair<uint64_t, std::vector<std::vector<uint64_t>>> map(uint64_t log_idx);

	std::string seq_ip;
	std::vector<std::string> storage_ips;

	uint64_t num_m_per_extent;
	uint64_t num_m_per_rep_set;
	uint64_t extent_size;

	void setup_auxiliary();

	uint64_t cnt;

	std::string seq_recv_port;
	std::string stor_recv_port;

public:
	CorfuClient(std::string input_file, uint64_t thread_id);
	~CorfuClient();

	void reconfigure(uint64_t log_idx, CorfuStorage& failing_unit);
	uint32_t append(const std::string& entry);
	std::string read(uint64_t log_idx);
	bool trim(uint64_t log_idx);
	uint64_t fill(uint64_t idx);

	void launch_append_execute();
	void wait_to_warmup();
	void wait_to_cooldown();
	void wait_to_finish(bool is_append);
	bool experiment_status();
	void execute(uint64_t thread_id);

	uint64_t getTail();
    void subscribe(uint64_t idx);

	uint64_t append_cntr;
	uint64_t payload_size;
	uint64_t batch_size;
	uint64_t num_work_threads;
	std::unique_ptr<Stats> stat;
	uint64_t max_duration;
	uint64_t warm_up;
	uint64_t cool_down;
	bool collect_stats;
	bool testing_append;
	bool end_thread;
	std::thread execution_thread;
};