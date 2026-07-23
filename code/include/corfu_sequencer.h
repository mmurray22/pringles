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
#include "corfu_headers.h"

#include "structs.h"

#define TIMEOUT std::chrono::seconds(10)

class CorfuSequencer {
    public:
        CorfuSequencer(std::string input_file);
        ~CorfuSequencer();
        
        uint32_t assign_next_idx(); // Assigns the next sequence number
        uint32_t get_current_idx(); // gets the current sequencer index

        void wait_to_finish();

    protected:
        void run_sequencer_thread();

    private:
        std::shared_ptr<Network> net;
        std::thread sequencer_thread;
        std::atomic<uint64_t> curr_idx;
        std::atomic<bool> terminate;

        std::vector<std::string> cli_ips;

        std::unique_ptr<struct corfu_seq_header> header;
        std::unique_ptr<struct corfu_gettoken_reply> gettoken_reply;

        uint64_t send_port;

        uint64_t num_pkt_types;

        uint64_t max_duration;
        uint64_t max_num_threads;
        bool end_thread = false;

        bool use_performance;
};