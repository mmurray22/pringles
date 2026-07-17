#pragma once
#include <atomic>
#include <thread>
#include <string>
#include <memory>
#include "yaml-cpp/yaml.h"
#include "utils.h"
#include "spdlog/spdlog.h"
#include "network.h"
#include "trace.h"

#include "structs.h"

#define TIMEOUT std::chrono::seconds(10)

enum SeqPacketType {
    sendtoken
};

class CorfuSequencer {
    public:
        CorfuSequencer(std::string input_file);
        ~CorfuSequencer();
        
        uint64_t assign_next_idx(); // Assigns the next sequence number
        uint64_t get_current_idx(); // gets the current sequencer index

        std::string IP;

        void wait_to_finish();
        bool use_performance;

    protected:
        void run_sequencer_thread();

    private:
        std::shared_ptr<Network> net;
        std::thread sequencer_thread;
        std::atomic<uint64_t> curr_idx{0};
        std::atomic<bool> terminate{false};

        std::vector<std::string> cli_ips;

        uint64_t send_port;

        uint64_t num_pkt_types;

        uint64_t max_duration;
	    bool end_thread = false;

        uint64_t max_num_threads;
};