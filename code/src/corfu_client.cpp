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

    public int fill(int index) {
        junk[index] = true
        return 0 // success
    }


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