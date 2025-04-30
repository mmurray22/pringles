// Corfu client node

#include "corfu_client.h"
#include "trace.h"

/*
Pseudocode for Corfu Client

struct FlashID {
    std::string IP,
    uint64_t ID
};


class CorfuClient {
    private List<std::pair<index, {entry, epoch}>> corfuLog // shared log mapping indices to flash pages
    private static Map<Integer, Boolean> junk // map storing whether log position correpsonding to
                                              // index holds junk
    private static List<Map<Range, List<FlashID>>> auxiliary
    private static int currEpoch = 0 // starts at 0, acts as curr_index for auxiliary
    private static bool projectionSealed = false

    private int getTail() {
        // Called in write
        // returns index of next position in the log
        return sequencer.get_next_token()
    }

    // NOTE: MUST SET projectionSealed TO TRUE BEFORE RECONFIGURATION
    // QUESTION: shouldn't we also increase the server epoch counters upon reconfiguration?

    private int reconfigure(int index, FlashID failingUnit) {
        if (!projectionSealed || reconfigure_time > TIMEOUT) {
            for (currRange : auxiliary[currEpoch].keySet()) {
                if (index in currRange) {
                    auxiliary[currEpoch + 1] = auxiliary[currEpoch] // copy over to new epoch
                    Range splitRangeLeft = Range(currRange.left, index)
                    Range splitRangeRight = Range(index, currRange.right)
                    
                    auxiliary[currEpoch + 1].remove(currRange)
                    auxiliary[currEpoch + 1][splitRangeLeft] = auxiliary[currEpoch][currRange]
                    auxiliary[currEpoch + 1][splitRangeLeft].remove(failingUnit)
                    auxiliary[currEpoch + 1][splitRangeRight] = auxiliary[currEpoch + 1][splitRangeLeft]
                    
                    FlashID newFlashUnit = getNewFlashUnit();
                    auxiliary[currEpoch + 1][splitRangeRight].append(newFlashUnit)

                    currEpoch++
                    projectionSealed = false

                    return 0 // indicates reconfiguration was successful
                }
            }
        }
        return 1 // failure
    }
*/

CorfuClient::CorfuClient(YAML::Node config) {
    configObj = std::make_unqiue<Config>();
}

uint64_t CorfuClient::reconfigure(uint64_t index, FlashID failingUnit) {
    auto start = std::chrono::high_resolution_clock::now();

    // Keep track of time taken to reconfigure
    this->reconfigure_time = std::chrono::high_resolution_clock::now() - start;

    // Begin reconfiguration if projection is not already sealed or
    // if previous reconfiguration taking too long
    if (!this->projection_sealed || this->reconfigure_time > this->RECONFIGURATION_TIMEOUT) {
        this->projection_sealed = true; // seal projection to begin reconfiguration

        // loop over configuration in current epoch
        for (const auto& config_kv : this->auxiliary[this->currEpoch]) {
            std::pair<uint64_t, uint64_t> curr_range = config_kv.first;
            std::vector<FlashID> flashIDs_in_range = config_kv.second;

            // check if current range has index of failure
            if (index >= curr_range.first && index <= curr_range.second) {
                // create a new epoch with same configuration as previous epoch
                this->auxiliary.emplace_back(this->auxiliary[currEpoch]);

                std::pair<uint64_t, uint64_t> split_range_left = {curr_range.first, index};
                std::pair<uint64_t, uint64_t> split_range_right = {index + 1, curr_range.second};
                
                // split the range containing failing flash unit into two ranges
                // left range contains indices before failure
                // right range contains indices after failure
                this->auxiliary[this->currEpoch + 1].erase(curr_range);
                this->auxiliary[this->currEpoch + 1][split_range_left] = (
                    this->auxiliary[this->currEpoch][curr_range]
                );

                std::vector<FlashID> left_split_flash = (
                    this->auxiliary[this->currEpoch + 1][split_range_left]
                );

                left_split_flash.erase(remove(
                    left_split_flash.begin(),
                    left_split_flash.end(),
                    failingUnit
                ));
                this->auxiliary[this->currEpoch + 1][split_range_right] = (
                    this->auxiliary[this->currEpoch + 1][split_range_left]
                );
                
                FlashID newFlashUnit = getNewFlashUnit(); // to be implemented

                this->auxiliary[this->currEpoch + 1][split_range_right].emplace_back(newFlashUnit);

                this->currEpoch++;
                this->projectionSealed = false;

                return 0; // indicates reconfiguration was successful
            }
        }
    }

    return 1; // failure
}

/*
// QUESTION: how are we sure we're sending to correct storage server here?

    public byte[] read(int index) {
        if (index < corfu_log.size() && !junk.getOrDefault(i, false)) {
            Packet readPacket = createPacket("read", index)
            
            byte[] readData = new byte[]


            /*

            // Send packet: 
            // 1)  Construct payload
            // 2) Specify which storage server to send to
           
            if (i > tail) {
                return "nope, too large"
            }

            std::vector<FlashID> send_machines = auxiliary[currEpoch].in_range(i);
            // if current epoch doesn't store index i
            for (epoch_range : auxiliary.reverse) { // for in reverse 
                // check each prior epoch
            }

            if (index_seen.has(i)) {
                send_machines[0] <-- just need to issue to a single storage server
                // Do everything in for loop
                return entry;
            }


            for (sm : send_machines) {
                add_to_send_queue(readPacket, sm.IP);
                string* msg = NULL;
                while(TIMEOUT) {
                    sleep(5);
                    msg = read_from_recv_queue(sm.IP);
                    if (msg != NULL) {
                        break;
                    }
                }
                if (TIMEOUT reached) {
                    // BIG ERROR - reconfiguration
                    return;
                }
                // Process msg
                if (msg.err == err_unwritten) { // good message is msg.err = none
                    fill(msg.entry)
                }
            }


            */
           /*
            add_to_send_queue(readData, readPacket)
            
            String msg = read_from_recv_queue(readPacket)
            while (time < TIMOUT && msg == null) {
                msg = read_from_recv_queue(readPacket)
            }

            if (msg == null) {
                FlashID failingUnit = auxiliary[currEpoch][index][0]

                reconfigure(index, failingUnit)
                
                return new byte[] // empty byte array indicates failure to read
            }

            return readData
        }
    }
    */

std::string CorfuClient::read(uint64_t index) {
    // Make sure index has been previously assigned and is not junk
    if (index < this->corfu_log.size() && (!this->junk.find(index) || this->junk[index])) {
        Packet readPacket = createPacket("read", index);
        
        std::string read_data;

        // Send packet: 
        // 1)  Construct payload
        // 2) Specify which storage server to send to
        
        if (index > this->sequencer.get_current_idx()) {
            return read_data; // return empty byte vector in case of read error
        }

        std::vector<FlashID> send_machines; // machines that need to be read from

        bool range_found = false; // Marker to keep track whether valid send machines have been found

        // start with current epoch to find corresponding send machines
        // keep going through previous epochs until machines corresponding to index are found
        for (uint64_t i = this->curr_epoch; i >= 0; i--) {
            for (const auto& config_kv : this->auxiliary[i]) {
                std::pair<uint64_t, uint64_t> curr_range = config_kv.first;
                std::vector<FlashID> flashIDs_in_range = config_kv.second;

                if (index >= curr_range.first && index < curr_range.second) {
                    send_machines = flashIDs_in_range;
                    range_found = true;
                    break;
                }
            }

            if (range_found) {
                break;
            }
        }

        // return empty byte array if send machines are not found
        if (!range_found) {
            return read_data;
        }

        // if (index_seen.has(i)) {
        //     send_machines[0] <-- just need to issue to a single storage server
        //     // Do everything in for loop
        //     return entry;
        // }

        // Ensure that machine exists and the corresponding append was successful
        if (send_machine[0] == nullptr) {
            return read_data;
        }

        auto start = std::chrono::high_resolution_clock::now();
        this->read_time = std::chrono::high_resolution_clock::now() - start;

        add_to_send_queue(readPacket, sm.ip);
        std::string msg = nullptr;

        while (this->read_time < this->READ_TIMEOUT) {
            sleep(5);

            msg = Trace::deserialize_str_entry(read_from_recv_queue(send_machine[0].IP), 2);
            // Exit loop if nothing left to read
            if (msg == NULL) {
                break;
            }

            read_data += msg;

            this->read_time = std::chrono::high_resolution_clock::now() - start;
        }

        // Initiate reconfiguration if timeout is reached
        while (this->read_time >= this->READ_TIMEOUT) {
            this->reconfigure_time = 0
            CorfuClient::reconfigure(index, send_machine[0]);

            // portion that re-runs reconfiguration if timeout exceeded should be handled by
            // different thread
            if (this->reconfigure_time > this->RECONFIGURATION_TIMEOUT) {
                // Assume that reconfiguration is done atomically (i.e. if reconfiguration does
                // not complete, epoch number is not updated)
                this->reconfigure_time = 0;

            } else {
                // retry read if reconfiguration is successful
                return CorfuClient::read(index);
            }
        }

        return read_data;

    } else {
        // Return null if read is invalid
        return nullptr;
    }
}

    // public int fill(int index) {
    //     junk[index] = true
    //     return 0 // success
    // }

uint64_t CorfuClient::fill(uint64_t index) {
    this->junk[index] = true;
    return 0;
}


    /*
    // QUESTION: when appending do we want to go through the corfuLog instance or the auxiliary[currEpoch] instance?
    // Wouldn't the auxiliary[currEpoch] instances have the current mapping of pages? Why do we need a separate log?

    public int append(byte[] input) {
        // go to sequencer for token
        int index = getTail()

        // for each (server, addr) in P(idx):
        for (currRange : auxiliary[currEpoch].keySet()) {
            if (index in currRange) {

                // create a write packet to send to the storage server
                Packet writePacket = createPacket("write", index)
                add_to_send_queue(input, writePacket)

                // wait for server reply
                String msg = read_from_recv_queue()
                while (time < TIMOUT && msg == null) {
                    msg = read_from_recv_queue()
                }

                // if we don't recv a message back, this storage unit is assumed to have failed, must reconfigure
                if (msg == null) {
                    FlashID failingUnit = auxiliary[currEpoch][index][0]

                    reconfigure(index, failingUnit)
                    
                    return 1 // return 1 indicates there was a failure and we should try to append again
                }

                if (msg == err_sealed) {
                    FlashID failingUnit = axiliary[currEpoch][index][0]
                    reconfigure(index, failingUnit)
                    return 1 // indicates there was a failure and we should try to append again
                }

                if (msg == err_deleted || msg == err_unwritten) {
                    return msg
                }
            }
        }
        return index    
    }

    public int trim(int index) {
        for (currRange : auxiliary[currEpoch].keySet()) {
            if (index in currRange) {

                // indicate no valid data is at index
                Packet deletePacket = createPacket("delete", index)
                byte[] deleteBuf = new byte[]
                add_to_send_queue(deleteBuf, deletePacket)

                // wait for server ack
                String msg = read_from_recv_queue()
                while (time < TIMOUT && msg == null) {
                    msg = read_from_recv_queue()
                }
            }
        }
    }

}
*/