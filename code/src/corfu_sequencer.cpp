// Corfu sequencer
/*
Pseudocode for Corfu Sequencer

class CorfuSequencer {
    private int next_token = 0 (initial value starts at 0)

    public int get_next_token() {
        acquire lock
        int new_token = this.next_token
        this.next_token++
        return new_token
        release lock
    }
}
*/
#include "corfu_client.h"



CorfuSequencer::CorfuSequencer(YAML::Node config) {
    configObj = std::make_unqiue<Config>();
}

uint64_t CorfuSequencer::assign_next_idx() {
    lock_guard<mutex> lock(sequencer_lock);
    this->next_idx++;
    return this->next_idx - 1;
}

uint64_t CorfuSequencer::get_current_idx() {
    lock_guard<mutex> lock(sequencer_lock);
    return this->next_idx - 1;
}
