#include <cstdint>
#include <string>
#include <mutex>

/*
 * Corfu Sequencer
 *
 * This is a sequencer class used when handling appends to the log.
 * Clients request the sequencer for
 */

class BaseSequencer {
    public:
        // Assigns the next sequence number
        uint64_t assign_next_idx();

        // Gets the current max assigned sequence number
        uint64_t get_current_idx();

    private:
        std::mutex sequencer_lock;
        uint64_t next_idx = 0;
}