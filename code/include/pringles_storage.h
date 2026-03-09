#include <vector>
#include <optional>
#include <set>
#include <tbb/concurrent_unordered_set.h>
#include "network.h"
#include "base_storage.h"
#include "measure.h"

enum StorageType {
	MEM_KV,
	NOSTORE,
	DISK_KV,
	HASHMAP
};

const uint64_t MAX_WAIT_TIME = 100;
class LogStorage : public BaseStorage {

    public:
	LogStorage(std::string input_file, uint64_t storage_id);
	~LogStorage();

	bool recover_storage_server();
	bool store(uint64_t idx, std::string entry);
	std::string get(uint64_t idx);
	void wait_to_finish();
	void change_view(uint64_t new_view_num);

    private:
	/* ACTUAL STORAGE SERVER INFO */
        uint64_t ssid;
        std::shared_ptr<Network> net;
        std::unordered_map<uint64_t, std::string> storage = {};
	
	// Maps stream ID -> {set of sequence numbers for that ID}
	bool use_streams;
	std::string multicast_addr;

        tbb::concurrent_hash_map<uint64_t, std::string> concurrent_stor;

	// Shards
	bool use_shards;      
	uint64_t shard_id;
	uint64_t shard_switch_id;
	uint64_t view_num;
	bool end_thread = false;


	std::string switch_ip;
	std::string switch_recv_port;
	bool use_switch;
	std::thread recv_thread;
	std::thread append_thread;
	std::thread read_thread;
	uint64_t max_duration;
	uint64_t append_cntr = 0;
	uint64_t read_cntr = 0;
	uint64_t max_append_idx = 0;
	

	std::array<uint8_t,6> switch_mac;

	std::condition_variable append_req_cv;
	std::mutex append_req_q_mutex;
	tbb::concurrent_queue<char*> append_req_q;

	std::condition_variable read_req_cv;
	std::mutex read_req_q_mutex;
	tbb::concurrent_queue<char*> read_req_q;


	// Functions
	void receiver();
	void append_server();
	void read_server();
};
