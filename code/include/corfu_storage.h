#pragma once
#include <unordered_map>
#include <atomic>
#include <memory>
#include <thread>
#include "base_storage.h"
#include "network.h"
#include "trace.h"


enum StorageType {
    ack,
    err_sealed,
    err_unwritten,
    err_written,
    err_deleted,
    store_read,
    store_seal
};

class CorfuStorage : public BaseStorage {
    public:
        CorfuStorage(uint64_t ssid, std::string input_file);
        ~CorfuStorage();

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

        bool store(uint64_t idx, std::string entry) override;
        std::string get(uint64_t idx) override;

    protected:
        struct map_entry {
            bool deleted;
            std::string contents;
        };

        std::thread storage_thread;

        std::unordered_map<uint64_t, map_entry> storage_map;
        std::vector<std::string> cli_ips;

        std::shared_ptr<Network> net;
        std::thread server_thread;
        std::atomic<bool> terminate{false};

        std::string send_port;
        uint64_t num_pkt_types;
};