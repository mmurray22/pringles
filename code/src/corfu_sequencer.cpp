#include "corfu_sequencer.h"

CorfuSequencer::CorfuSequencer(std::string input_file) {
    YAML::Node config = YAML::LoadFile(input_file);
    this->num_pkt_types = get_num_pkt_types(config);

    this->cli_ips = get_cli_ip(config);

   std::string multicast_ip = "";

    net = std::make_shared<Network>(
        std::to_string(get_send_port(config)), 
        std::to_string(get_recv_port(config)),
		get_socket_type(config),
        get_log_level(config),
		get_batch_size(config),
		get_batch_on(config),
		get_batch_timeout(config),
	    get_interface(config),
		get_self_ip(config),
		multicast_ip,
		false,
		false);

    this->send_port = get_send_port(config);
    this->max_duration = get_experiment_duration(config);

    terminate = false;
    curr_idx.store(0);

    sequencer_thread = std::thread(&CorfuSequencer::run_sequencer_thread, this);
}

CorfuSequencer::~CorfuSequencer() {
    terminate = true;
    if (sequencer_thread.joinable()) {
        sequencer_thread.join();
    }
    spdlog::debug("Sequencer thread joined!");
}

void CorfuSequencer::run_sequencer_thread() {
    spdlog::info("Sequencer active polling thread started.");

    while (!terminate) {
        char* recv_ptr = net->recv_packet();
        
        if (!recv_ptr) {
            continue; 
        }

        // we received a packet!!!!!
        auto rcv_str = std::make_unique<std::string>(recv_ptr);
        corfuclient::Payload packet_contents = corfu_client_deserialize_str_entry(std::move(rcv_str));

        if (packet_contents.has_token_req() && packet_contents.token_req().reqtoken()) {
            spdlog::debug("Sequencer received a token request from client {}", packet_contents.clientid());
            
            uint64_t idx = assign_next_idx();
            std::unique_ptr<std::string> token_packet = corfu_sequencer_serialize_str_entry(CORFU_GETTOKEN_REPLY_PROTO_TYPE, idx);
            
            uint64_t allocated_packet_size = token_packet->length() + 1;
            std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
            memcpy(packet.get(), token_packet->c_str(), allocated_packet_size);
            packet[token_packet->length()] = '\0';

            int cid = packet_contents.clientid();

            net->send_client_udp_packet(
                std::move(packet), 
                allocated_packet_size, 
                cli_ips[cid],
                std::to_string(send_port + cid)
            );
            spdlog::debug("Sequencer gave index {} to cid {}", idx, packet_contents.clientid());
        } else {
            spdlog::error("PACKET DROPPED: not asking for a token");
        }
    }
}

uint64_t CorfuSequencer::assign_next_idx() {
    return curr_idx.fetch_add(1);
}

uint64_t CorfuSequencer::get_current_idx() {
    return curr_idx.load();
}

void CorfuSequencer::wait_to_finish() {
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
    spdlog::critical("End thread is bool: {}", end_thread);
}