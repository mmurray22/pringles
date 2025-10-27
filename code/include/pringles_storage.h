#include <vector>

#include <optional>
#include "network.h"
#include "base_storage.h"

enum StorageType {
	MEM_KV,
	NOSTORE,
	DISK_KV,
	HASHMAP
};

enum PacketType {
    	append,
	dummyread,
    	readentry,
	dummyappendstream,
	appendstream
};


class LogStorage : public BaseStorage {

    public:
	LogStorage(std::string input_file, uint64_t storage_id);
	~LogStorage();

	bool recover_storage_server();
	bool store(uint64_t idx, std::string entry);
	std::string get(uint64_t idx);
	void wait_to_finish();

    private:
	// Storage server ID
        uint64_t ssid;
	StorageType stor;
        // Network object
        std::unique_ptr<Network> net;
	uint64_t num_pkt_types;
        
	
	// In-memory Key-Value Store
        std::map<uint64_t, std::string> kv_store;
        // Key-Value Store Lock
        std::mutex kv_store_lock;
	
	// Mutable
	uint64_t shard_id;
	uint64_t shard_switch_id;
	uint64_t view_num;
	bool end_thread = false;
	std::thread recv_thread;

	// Immutable
	uint64_t max_duration;
	
	// Function
	void change_view(uint64_t new_view_num);
	uint64_t index_project(uint64_t idx);
	void pringles_recv_queue();
	std::unique_ptr<char[]> create_pkt(PacketType pkt_type, 
		                           uint32_t nonce,
				           std::optional<std::string> entry = std::nullopt,
			   	           std::optional<int64_t> idx = 0);
	std::vector<int> get_pkt_eth_types();
	size_t get_size_of_hdr(uint64_t pkt_type);
	int get_eth_type(uint64_t pkt_type);

};
