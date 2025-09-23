#include "corfu_sequencer.h"

CorfuSequencer::CorfuSequencer() {
    sequencer_thread = std::thread(&CorfuSequencer::run_sequencer_thread, this);
}

CorfuSequencer::~CorfuSequencer() {
    terminate = true;
    sequencer_thread.join();
}

void CorfuSequencer::run_sequencer_thread() {
    std::unique_ptr<std::string> rcv_str;
    uint64_t wait_time = 10;
    while (true) {
        if (wait_time >= MAX_WAIT_TIME) {
            spdlog::debug("!!!!!!!!!!!!!!No more packets to receive.");
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
        wait_time += 10;
        rcv_str = net->read_from_recv_queue();
        if (!rcv_str) {
            continue;
        }
        std::string packet_contents = deserialize_str_entry(rcv_str, CORFU_PROTO_TYPE);
        if (packet_contents == "gettoken") {
            spdlog::info("sequencer received a token request");
            uint64_t idx = assign_next_idx();

            std::unique_ptr<std::string> token_packet = serialize_str_entry(std::to_string(idx), CORFU_PROTO_TYPE);
            uint64_t client_ip = packet_contents->clientIP; // each packet comes with a client ip, but better solution should be found

            net->add_to_send_queue(token_packet, client_ip);
        }
        wait_time = 10;
    }
}

uint64_t CorfuSequencer::assign_next_idx() {
    return curr_idx.fetch_add(1);
}

uint64_t CorfuSequencer::get_current_idx() {
    return curr_idx.load();
}