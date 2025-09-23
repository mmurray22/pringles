#include <string>
#include <cstdint>
#include <unordered_map>


class CorfuStorage : public BaseStorage {
    public:
        CorfuStorage(uint64_t ssid);
        ~CorfuStorage() override;

        bool sync_store(uint64_t idx, std::string entry) override;
        bool lazy_store(uint64_t idx, std::string entry) override;
        std::string get(uint64_t idx) override;

        // corfu storage specific functions
         read();
        void write();
        void storage_delete();
        void seal();

    protected:
        struct map_entry {
            bool deleted;
            std::string contents;
        }

        uint64_t s_epoch = 0      // initially 0, used to tell client if their mapping is out of date
        std::unordered_map<uint64_t, map_entry> storage_map;     // maps log pos -> status bit + content
        uint64_t mark = 0      // before this address, there are no unwritten addresses (updated in write, used for seal)
}