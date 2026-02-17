#pragma once

// #include <string>
// #include <cstdint>
#include <unordered_map>
#include "base_storage.h"
#include <atomic>

#include "network.h"
#include "trace.h"

// #include <thread>
// #include <chrono>
// #include <memory>

// #include <spdlog/spdlog.h>


class CorfuStorage : public BaseStorage {
    public:
        CorfuStorage(uint64_t ssid, std::shared_ptr<Network> net);
        ~CorfuStorage();

        bool sync_store(uint64_t idx, std::string entry) override;
        bool lazy_store(uint64_t idx, std::string entry) override;
        std::string get(uint64_t idx) override;

        // response functions for when the server receives certain packets over the network
        // used as helpers in the server function
        void read(corfuclient::Payload msg);
        void write(corfuclient::Payload msg);
        void storage_delete(corfuclient::Payload msg);
        void seal(corfuclient::Payload msg);

        uint64_t ssid = 0;  // storage server ID
        int64_t s_epoch = 0;  // current epoch
        uint64_t mark = 0;  // highest written address

        void server();

    protected:
        struct map_entry {
            bool deleted;
            std::string contents;
        };

        std::unordered_map<uint64_t, map_entry> storage_map;

        std::shared_ptr<Network> net;
        std::thread server_thread;
        std::atomic<bool> terminate{false};
};