#include <vector>

template<class Entry>
class LogStorageServer {
public:
	bool append_log_entry(class Entry);
	bool recover_storage_server();
	void receive_packets();
private:
	// Immutable
	uint64_t storage_server_id;
	
	// Mutable
	uint64_t shard_id;
	uint64_t shard_switch_id;
	uint64_t view_num;
	std::vector<struct StorageServer> storage_server_info;
	
	// Function
	void change_view(uint64_t new_view_num);
	uint64_t index_project(uint64_t idx);
}
