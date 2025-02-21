#include <vector>

class LogStorageServer : public BaseStorage {

    public:
	    bool recover_storage_server();

    private:
	    // Mutable
	    uint64_t shard_id;
	    uint64_t shard_switch_id;
	    uint64_t view_num;
	    std::vector<struct StorageServer> storage_server_info;
	
	    // Function
	    void change_view(uint64_t new_view_num);
	    uint64_t index_project(uint64_t idx);
}
