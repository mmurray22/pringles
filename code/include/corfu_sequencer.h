#pragma once

// #include <cstdint>
// #include <string>
#include <atomic>
// #include <memory>
// #include <spdlog/spdlog.h>
#include "yaml-cpp/yaml.h"
#include <thread>

#include "utils.h"
#include "spdlog/spdlog.h"

#include "network.h"
#include "trace.h"

constexpr uint64_t MAX_WAIT_TIME = 1000;

class CorfuSequencer {
    public:
        CorfuSequencer(YAML::Node config);
        ~CorfuSequencer();
        
        uint64_t assign_next_idx(); // Assigns the next sequence number
        uint64_t get_current_idx(); // gets the current sequencer index

        std::string IP;

    protected:
        void run_sequencer_thread();

    private:
        std::shared_ptr<Network> net;
        std::thread sequencer_thread;
        std::atomic<uint64_t> curr_idx{0};
        std::atomic<bool> terminate{false};
};