#include <cstdint>
#include <mutex> 
#include <map>

template <typename T> class BaseStorage {
    public:
        /** Class Creation **/
	    virtual BaseStorage(uint64_t ssid) = 0;
        virtual ~BaseStore() = 0;

        /** Storage Functions **/
        bool sync_store(uint64_t idx, T entry) = 0;
        bool lazy_store(uint64_t idx, T entry) = 0;
        T get(uint64_t idx) = 0;

	private:
		// Storage server ID
        uint64_t ssid;
        // Network object
        std::unique_ptr<Network> net;
        // Key-Value Store
        std::map<uint64_t, T> kv_store;
        // Key-Value Store Lock
        std::mutex kv_store_lock;
}
