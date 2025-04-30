#include "corfu_client.h"

/*
Pseudocode for Corfu Client

class CorfuClient {

    private static List<HashSet<FlashID>> corfuLog // use this as a cache for entries
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

    public byte[] read(int index) {
        if (index < corfu_log.size() && !junk.getOrDefault(i, false)) {
            Packet readPacket = createPacket("read", index)
            
            byte[] readData = new byte[]
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

    public int append(byte[] input) {
        // go to sequencer for token
        int index = getTail()

        // for each (server, addr) in P(idx):
        for (currRange : auxiliary[currEpoch].keySet()) {
            if (index in currRange) {

                // create a write packet to send to the storage server
                Packet writePacket = createPacket("write", currEpoch, sm.IP, input)
                add_to_send_queue(writePacket, sm.IP)

                // wait for server reply
                String msg = read_from_recv_queue(sm.IP)
                while (time < TIMOUT && msg == null) {
                    msg = read_from_recv_queue(sm.IP)
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
                add_to_send_queue(deletePacket, sm.IP)

                // wait for server ack
                String msg = read_from_recv_queue(sm.IP)
                while (time < TIMOUT && msg == null) {
                    msg = read_from_recv_queue(sm.IP)
                }
            }
        }
    }

}
*/

CorfuClient::CorfuClient(YAML::Node config) {
    
}

uint64_t CorfuClient::get_tail() {
    return sequencer.assign_next_idx;
}

uint64_t CorfuClient::append(std::unique_ptr<std::string> entry) {
    uint64_t log_idx = get_tail();
    // loop through all of the replicas that have this log position
    for (const auto& curr_range : auxiliary[curr_epoch].keySet()) {
        if (curr_range.contains(log_idx)) {
            sm = __

            std::unique_ptr<std::string> write_packet = serialize_str_entry(entry, CORFU_PROTO_TYPE);
            net->add_to_send_queue(write_packet, sm.IP);

            std::string msg;
            // get curr time
            while (time < TIMEOUT && msg == NULL) {
                msg = net->read_from_recv_queue(sm.IP);
            }

            // must reconfigure if there's no response
            if (msg == NULL) {
                FlashID failing_unit = auxiliary[curr_epoch][log_idx][0];
                reconfigure(log_idx, failingUnit);
                return 1; // return 1 indicates there was a failure and we should try to append again
            }

            if (msg == err_sealed) {
                FlashID failingUnit = axiliary[curr_epoch][log_idx][0];
                reconfigure(log_idx, failing_unit);
                return 1; // indicates there was a failure and we should try to append again
            }

            if (msg == err_deleted || msg == err_unwritten) {
                return msg;
            }
        }
    }
    return log_idx;
}

std::unique_ptr<std::string> SimpleClient::read(uint64_t idx) {

}

uint64_t CorfuClient::fill(uint64_t idx) {

}

uint64_t CorfuClient::trim(uint64_t idx) {
    for (const auto& curr_range : auxiliary[curr_epoch].keySet()) {
        if (idx in curr_range) {
            sm = __

            std::unique_ptr<std::string> delete_packet = serialize_str_entry(entry, CORFU_PROTO_TYPE);
            net->add_to_send_queue(delete_packet, sm.IP);

            std::string msg;
            // get curr time
            while (time < TIMEOUT && msg == NULL) {
                msg = net->read_from_recv_queue(sm.IP);
            }
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string input_file = std::string(argv[1]);
    YAML::Node config = YAML::LoadFile(input_file);

    std::shared_ptr<Network> custom_net = std::make_shared<Network>(get_threads(config), 
                                                                    get_ips(config), 
                                                                    get_port(config), 
                                                                    get_protocol(config), 
                                                                    get_log_level(config));
    set_spdlog_level(get_log_level(config));
    spdlog::info("Simple Network! Sending on localhost 127.0.0.1");
    std::shared_ptr<Trace<std::string>> trace = std::make_shared<Trace<std::string>>(get_trace_file(config));

    std::thread server_thread(custom_server, custom_net, trace);
    std::thread client_thread(custom_client, custom_net, trace);
    client_thread.join();
    server_thread.join();
    custom_net->done();

    return 0;
}