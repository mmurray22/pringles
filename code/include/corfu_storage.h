#include <string>
#include <cstdint>
#include <unordered_map>


class CorfuStorage : public BaseStorage {
    public:
        uint64_t ssid;
        void read();
        void write();
        void storage_delete();
        void seal();

    protected:
        uint64_t s_epoch = 0      // initially 0, used to tell client if their mapping is out of date
        std::unordered_map<uint64_t, uint64_t> address_map;     // for mapping virtual to physical addresses
        uint64_t mark = 0      // before this address, there are no unwritten addresses (updated in write, used for seal)
}