#include <cstdint>
#include <string>
#include <mutex>
#include <atomic>

class CorfuSequencer {
    public:
        CorfuSequencer();
        
        uint64_t assign_next_idx(); // Assigns the next sequence number
        uint64_t get_current_idx(); // gets the current sequencer index

    private:
        std::atomic<uint64_t> curr_idx{0};
}
