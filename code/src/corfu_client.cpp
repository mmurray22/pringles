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
    return this->sequencer.assign_next_idx;
}

uint64_t CorfuClient::append(std::unique_ptr<std::string> entry) {
    uint64_t log_idx = get_tail();
    // loop through all of the replicas that have this log position
    std::vector<CorfuStorage> send_machines;
    // use current epoch to find corresponding send machines
    for (const auto& config_kv : this->auxiliary[this->curr_epoch]) {
        std::pair<uint64_t, uint64_t> curr_range = config_kv.first;
        std::vector<CorfuStorage> server_range = config_kv.second;

        if (log_idx >= curr_range.first && log_idx < curr_range.second) {
            send_machines = server_range;
            break;
        }
    }

    if (send_machines.empty()) {
        spdlog::critical("Unable to find send machines");
        return 1; // return 1 indicates there was a failure and we should try again
    }

    for (CorfuStorage sm : send_machines) {
        std::unique_ptr<std::string> write_packet = serialize_str_entry(entry, CORFU_PROTO_TYPE);
        net->add_to_send_queue(write_packet, sm.IP);

        std::string msg;

        // start timer
        auto start_time = std::chrono::high_resolution_clock::now();
        auto read_time = std::chrono::high_resolution_clock::duration::zero();
        while (read_time < TIMEOUT && msg.empty()) {
            msg = net->read_from_recv_queue(sm.IP);
            read_time = std::chrono::high_resolution_clock::now() - start_time;
        }

        // must reconfigure if there's no response
        if (msg.empty() || read_time < TIMEOUT) {
            spdlog::info("Must reconfigure because there was no response");
            CorfuStorage failing_unit = auxiliary[curr_epoch][log_idx][0];
            reconfigure(log_idx, failing_unit);
            spdlog::info("Just reconfigured, you should attempt to append again");
            return 1; // return error
        }

        if (msg == err_sealed) {
            spdlog::info("Must reconfigure because the current epoch was sealed");
            CorfuStorage failing_unit = auxiliary[curr_epoch][log_idx][0];
            reconfigure(log_idx, failing_unit);
            spdlog::info("Just reconfigured, you should attempt to append again");
            return 1; // return error
        }

        if (msg == err_deleted) {
            spdlog::critical("We got an err_deleted and now we're returning the error code");
            return err_deleted;
        }

        if (msg == err_unwritte) {
            spdlog::critical("We got an err_unwritten and now we're returning the error code");
            return err_unwritten;
        }
    }
    return log_idx;
}

std::unique_ptr<std::string> SimpleClient::read(uint64_t idx) {
    return nullptr;
}

uint64_t CorfuClient::fill(uint64_t idx) {
    return 0;
}

uint64_t CorfuClient::trim(uint64_t log_idx) {
    // loop through all of the replicas that have this log position
    std::vector<CorfuStorage> send_machines;
    // use current epoch to find corresponding send machines
    for (const auto& config_kv : this->auxiliary[this->curr_epoch]) {
        std::pair<uint64_t, uint64_t> curr_range = config_kv.first;
        std::vector<CorfuStorage> server_range = config_kv.second;

        if (log_idx >= curr_range.first && log_idx < curr_range.second) {
            send_machines = server_range;
            break;
        }
    }

    if (send_machines.empty()) {
        spdlog::critical("Unable to find send machines");
        return 1; // return 1 indicates there was a failure and we should try again
    }

    for (CorfuStorage sm : send_machines) {
        std::unique_ptr<std::string> delete_packet = serialize_str_entry("delete", CORFU_PROTO_TYPE);
        net->add_to_send_queue(delete_packet, sm.IP);

        std::string msg;
        // start timer
        auto start_time = std::chrono::high_resolution_clock::now();
        auto read_time = std::chrono::high_resolution_clock::duration::zero();
        while (read_time < TIMEOUT && msg.empty()) {
            msg = net->read_from_recv_queue(sm.IP);
            read_time = std::chrono::high_resolution_clock::now() - start_time;
        }

        // never received ack
        if (msg.empty() || read_time < TIMEOUT) {
            spdlog::critical("We did not receive an ack :(");
            return 1; // failure
        }
    }
    return 0; // success
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

    // what to put here instead of custom_server, custom_client ?
    std::thread server_thread(custom_server, custom_net, trace);
    std::thread client_thread(custom_client, custom_net, trace);
    client_thread.join();
    server_thread.join();
    custom_net->done();

    return 0;
}