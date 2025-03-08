// Simple client
#include <map>
#include "base_client.h"

/*
 * SimpleClient
 *
 * This is a basic client class which is used largely for testing 
 * the logging infrastructure. The client adds string objects to the 
 * log. This is a simpler class because it is NOT made for multiple 
 * distributed clients. It is only safe if you only have one instance
 * of SimpleClient running.
 */
class SimpleClient : public BaseClient {
    public:
        SimpleClient(YAML::Node config);
        void append_trace();
        void read_trace();

    private:
        /*** Functions ***/
        uint64_t new_seq_no();
        uint64_t get_seq_no();
        uint64_t recv_pkt(uint64_t idx);

        uint64_t hash(std::string entry);


        /*** Variables ***/
        std::mutex seq_no_lock;
        uint64_t seq_no;
        bool terminate;

        // Timeouts
        uint64_t wait_for_read_acks;
        uint64_t wait_for_write_acks;
        
        uint64_t num_storage_servers;

        std::mutex pending_appends_lock;
        std::condition_variable palCV;
        std::unordered_map<uint64_t, uint64_t> pending_appends;
        bool pending_appends_updated;

        std::mutex pending_read_lock;
        std::condition_variable prCV;
        std::unordered_map<uint64_t, uint64_t> pending_reads;
        bool pending_reads_updated;
       
        // Receive threads
        std::thread recv;

        // Junk
        char arr[100];

        // Subscribe + Cached log entries 
        std::thread sub;
        uint64_t sub_timeout = 0;
        int64_t sub_start_idx;
        int64_t curr_sub_idx;
        std::mutex curr_sub_idx_lk;
        std::mutex local_log_lock;
        std::unordered_map<uint64_t, std::string> local_log;
}
