#include "base_sequencer.cpp"


uint64_t BaseSequencer::assign_next_idx() {
    std::lock_guard<std::mutex>(curr_idx_lock);
    curr_idx += 1;
    return curr_idx;
}

uint64_t BaseSequencer::get_current_idx() {
    std::lock_guard<std::mutex>(curr_idx_lock);
    return curr_idx;
}

