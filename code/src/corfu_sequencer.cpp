#include "corfu_sequencer.h"

CorfuSequencer::CorfuSequencer(YAML::Node config) {
    net = std::make_unique<Network>(get_threads(config), 
                                                                    get_send_port(config), 
                                                                    get_recv_port(config),
								    get_socket_type(config),
                                                                    get_log_level(config),
								    get_batch_size(config),
								    get_batch_on(config),
								    get_interface(config),
								    get_self_ip(config),
								    get_packet_types(config));

    sequencer_thread = std::thread(&CorfuSequencer::run_sequencer_thread, this);
}

CorfuSequencer::~CorfuSequencer() {
    terminate = true;
    net->done();
    sequencer_thread.join();
}

void CorfuSequencer::run_sequencer_thread() {
    std::unique_ptr<std::string> rcv_str;
    uint64_t wait_time = 10;
    while (!terminate) {
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

        corfuclient::Payload packet_contents = Trace<std::string>::corfu_client_deserialize_str_entry(std::move(rcv_str));

        if (packet_contents.token_req().reqtoken()) {
            spdlog::info("sequencer received a token request");
            uint64_t idx = assign_next_idx();

            std::unique_ptr<std::string> token_packet = Trace<std::string>::corfu_sequencer_serialize_str_entry(CORFU_GETTOKEN_REPLY_PROTO_TYPE, idx);
            net->add_to_send_queue(std::move(token_packet), packet_contents.clientid());
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