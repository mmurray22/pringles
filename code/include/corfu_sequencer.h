#include <cstdint>
#include <string>
#include <mutex>

class CorfuSequencer {
    public:
        CorfuSequencer();
        // Assigns the next sequence number
        uint64_t assign_next_idx();

        // Gets the current max assigned sequence number
        uint64_t get_current_idx();

    private:
        std::mutex curr_idx_lock;
        uint64_t curr_idx;
}
