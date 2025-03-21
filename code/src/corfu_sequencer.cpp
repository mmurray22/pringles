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

/*
 * Corfu Sequencer
 This is a sequencer class used when handling appends to the log.
 Clients request the sequencer for
 */
