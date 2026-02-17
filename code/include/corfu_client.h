#pragma once

#include "base_client.h"
#include "corfu_storage.h"
#include "corfu_sequencer.h"
#include "network.h"
#include "trace.h"
#include "utils.h"
// #include <vector>
// #include <map>
// #include <cstdint>
// #include <cstddef>
// #include <chrono>

#define TIMEOUT std::chrono::seconds(10)

class CorfuClient : public BaseClient {
    protected:
        void recv_pkt();
        std::thread recv_thread;

        std::atomic<bool> terminate;
        bool pending_appends_updated;
        uint64_t cid;

        CorfuSequencer sequencer;
        uint64_t curr_epoch = 0;

        std::map<uint64_t, std::map<std::pair<uint64_t, uint64_t>, std::vector<std::shared_ptr<CorfuStorage>>>> auxiliary;
        bool projection_sealed = false;

        uint64_t wait_for_read_acks;
        uint64_t wait_for_write_acks;

        std::shared_ptr<Trace<std::string>> trace;

    public:
        CorfuClient(YAML::Node config);
        ~CorfuClient();

        void reconfigure(uint64_t log_idx, CorfuStorage& failing_unit);
        uint64_t append(std::unique_ptr<std::string> entry);
        std::unique_ptr<std::string> read(uint64_t /*log_idx*/);
        uint64_t fill(uint64_t /*log_idx*/);
        bool trim(uint64_t log_idx);
};