// #include <string>
// #include <cstdint>
#include <unordered_map>
#include "base_storage.h"
#include <atomic>

// #include <thread>
// #include <chrono>
// #include <memory>

// #include <spdlog/spdlog.h>


class CorfuStorage : public BaseStorage {
    public:
        CorfuStorage(uint64_t ssid, std::shared_ptr<Network> net);
        ~CorfuStorage();

        bool sync_store(uint64_t idx, std::string entry) override;
        bool lazy_store(uint64_t idx, std::string entry) override;
        std::string get(uint64_t idx) override;

        // response functions for when the server receives certain packets over the network
        // used as helpers in the server function
        void read(std::string msg);
        void write(std::string msg);
        void storage_delete(std::string msg);
        void seal(std::string msg);

        void server();

    protected:
        struct map_entry {
            bool deleted;
            std::string contents;
        };

        uint64_t ssid = 0;  // storage server ID
        uint64_t s_epoch = 0;  // current epoch
        uint64_t mark = 0;  // highest written address

        std::unordered_map<uint64_t, map_entry> storage_map;

        std::shared_ptr<Network> net;
        std::thread server_thread;
        std::atomic<bool> terminate{false};
};