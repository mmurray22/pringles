#include <cstdint>
#include <string>
#include <mutex>
#include <atomic>

#define CORFU_GETTOKEN_PROTO_TYPE 6
#define CORFU_GETTOKEN_REPLY_PROTO_TYPE 14

class CorfuSequencer {
    public:
        CorfuSequencer();
        ~CorfuSequencer();
        
        uint64_t assign_next_idx(); // Assigns the next sequence number
        uint64_t get_current_idx(); // gets the current sequencer index

    protected:
        void run_sequencer_thread();

    private:
        std::thread sequencer_thread;
        std::atomic<uint64_t> curr_idx{0};
}
