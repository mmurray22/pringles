#include <cstdint>
#include <mutex> 
#include <map>

class BaseStorage {
    public:
        // /** Class Creation **/
	    // BaseStorage(uint64_t ssid) = 0;
        // ~BaseStorage() = 0;
        explicit BaseStorage(uint64_t ssid) : ssid(ssid) {}
        virtual ~BaseStorage() = 0;

        /** Storage Functions **/
        virtual bool sync_store(uint64_t idx, std::string entry) = 0;
        virtual bool lazy_store(uint64_t idx, std::string entry) = 0;
        virtual std::string get(uint64_t idx) = 0;

	private:
		// Storage server ID
        uint64_t ssid;
        // Network object
        std::unique_ptr<Network> net;
        // Key-Value Store
        std::map<uint64_t, std::string> kv_store;
        // Key-Value Store Lock
        std::mutex kv_store_lock;
};

inline BaseStorage::~BaseStorage() {}
