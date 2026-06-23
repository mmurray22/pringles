#pragma once
#include <unordered_map>
#include <atomic>
#include <memory>
#include <thread>
#include "network.h"
#include "trace.h"


enum StorageType {
	MEM_KV,
	NOSTORE,
	DISK_KV,
	HASHMAP
};

enum StoragePacketType {
    ack,
    store_read,
	err_sealed,
    err_deleted,
    err_written,
    err_unwritten,
    store_seal
};

class CorfuStorage {
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

        bool store(uint64_t idx, std::string entry);
        std::string get(uint64_t idx);

        void wait_to_finish();

    private:
        void error_sealed(int cid, std::string req_type);
        void error_deleted(int cid, std::string req_type);
        void send_ack(int cid, std::string req_type);
        std::pair<std::string, bool> get_entry(uint64_t idx);

        void server();
        
        std::thread storage_thread;
        StorageType stor;

        uint64_t max_duration;
	    bool end_thread = false;

        std::vector<std::string> cli_ips;

        std::shared_ptr<Network> net;
        std::thread server_thread;
        std::atomic<bool> terminate{false};

        uint64_t send_port;
        uint64_t num_pkt_types;

        // In-memory Key-Value Store
        std::map<uint64_t, std::pair<std::string, bool>> kv_store;
        // Key-Value Store Lock
        std::mutex kv_store_lock;
};