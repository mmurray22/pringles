#include "corfu_sequencer.h"

CorfuSequencer::CorfuSequencer() {
    this.curr_idx = 0
}

uint64_t CorfuSequencer::assign_next_idx() {
    {
        std::lock_guard<std::mutex> lock(this.curr_idx_lock);
        uint64_t new_token = this.curr_idx;
        this.curr_idx += 1;
    }
    return new_token;
}

uint64_t CorfuSequencer::get_current_idx() {
    return this.curr_idx;
}