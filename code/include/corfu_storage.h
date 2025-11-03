#include <string>
#include <cstdint>
#include <unordered_map>
#include "base_storage.h"


class CorfuStorage : public BaseStorage {
    public:
        CorfuStorage(uint64_t ssid);
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

        void server(std::shared_ptr<Network> net);

    protected:
        struct map_entry {
            bool deleted;
            std::string contents;
        };

        uint64_t s_epoch = 0;      // initially 0, used to tell client if their mapping is out of date
        std::unordered_map<uint64_t, map_entry> storage_map;     // maps log pos -> status bit + content
        uint64_t mark = 0;      // before this address, there are no unwritten addresses (updated in write, used for seal)
};