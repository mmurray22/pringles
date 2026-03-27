#pragma once

#include "base_client.h"
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

class CorfuClient : public BaseClient {
protected:
        uint64_t cid;

        CorfuSequencer* sequencer;
        uint64_t curr_epoch = 0;

        std::map<uint64_t, std::map<std::pair<uint64_t, uint64_t>, std::vector<std::shared_ptr<CorfuStorage>>>> auxiliary;
        bool projection_sealed = false;

        /**** Variables ****/
	std::string switch_ip;
	std::array<uint8_t,6> switch_mac;


	uint64_t min_matching_acks = 0;
	uint64_t num_pkt_types = 0; 
	/* Receive queue which slots messages */
	std::mutex pkt_q_lock;
	std::map<PacketType, std::queue<std::unique_ptr<char[]>>> pkt_q;
	std::vector<std::string> pkt_types;
	bool end_thread = false;
	bool started_append = false;

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
	
	/**** Functions ****/

        void run_append();
        void wait_for_subscribe(uint64_t idx, uint64_t pkt_type);

	void pringles_recv_queue();
		

	std::vector<int> get_pkt_eth_types();
	int get_eth_type(uint64_t pkt_type);

public:
        CorfuClient(std::string input_file, uint64_t thread_id, CorfuSequencer& sequencer);
        ~CorfuClient();

        void reconfigure(uint64_t log_idx, CorfuStorage& failing_unit);
        uint64_t append(std::unique_ptr<std::string> entry);
        std::string read(uint64_t log_idx);
        bool trim(uint64_t log_idx);
        uint64_t fill(uint64_t idx);

        void wait_to_warmup();
        void wait_to_cooldown();
        void wait_to_finish();
	bool experiment_status();
        void execute(uint64_t thread_id);
};