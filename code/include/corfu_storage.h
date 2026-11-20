#pragma once
#include <unordered_map>
#include <atomic>
#include <memory>
#include <thread>
#include "network.h"
#include "trace.h"
#include "corfu_headers.h"

#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_unordered_set.h>


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
        void write();
        void storage_delete(corfuclient::Payload msg);
        void seal(corfuclient::Payload msg);

        uint64_t ssid = 0;  // storage server ID
        int64_t s_epoch = 0;  // current epoch
        std::atomic<uint64_t> mark{0};  // highest written address

        bool store(uint64_t idx, std::string entry);
        std::string get(uint64_t idx);

        void wait_to_finish();
        std::atomic<uint64_t> curr_idx;

    private:
        void error_sealed(int cid, int thread_id, std::string req_type);
        void error_deleted(int cid, int thread_id, std::string req_type);
        void send_ack(int cid, int thread_id, std::string req_type);
        std::pair<std::string, bool> get_entry(uint64_t idx);

        void server(int append_port, std::unique_ptr<Network> append_net, uint64_t thread_id);
        
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
        using KVMap = tbb::concurrent_hash_map<uint64_t, std::pair<std::string, bool>>;
        // using KVMap = std::unordered_map<uint64_t, std::pair<std::string, bool>>;
        KVMap kv_store;
        // Key-Value Store Lock
        std::mutex kv_store_lock;

        uint64_t max_num_threads;
        bool just_storage;

        tbb::concurrent_vector<std::thread> append_req_threads;
        uint64_t num_append_req_threads;
        std::unique_ptr<struct corfu_storage_header> corfu_storage_header;
        std::vector<int> append_socket;
        struct in_addr addr;
};