#include "corfu_sequencer.h"
#include <linux/perf_event.h>

CorfuSequencer::CorfuSequencer(std::string input_file) {
    YAML::Node config = YAML::LoadFile(input_file);
    
    this->cli_ips = get_cli_ip(config);

   std::string multicast_ip = "";

    net = std::make_shared<Network>(
        std::to_string(get_send_port(config)),
        get_seq_recv_port(config),
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

    this->send_port = get_recv_port(config);
    this->max_duration = get_experiment_duration(config);
    this->max_num_threads = get_num_client_threads(config);

    this->use_performance = get_use_performance(config);
    SPDLOG_INFO("Sequencer Thread starting with TID = {}", gettid());

    end_thread = false;
    curr_idx.store(1);

    this->header = std::make_unique<struct corfu_seq_header>();
    header.get()->proto_type = htons(CORFU_GETTOKEN_REPLY_PROTO_TYPE);
    this->gettoken_reply = std::make_unique<struct corfu_gettoken_reply>();
    gettoken_reply.get()->log_idx = htonl(0);

    sequencer_thread = std::thread(&CorfuSequencer::run_sequencer_thread, this);
}

CorfuSequencer::~CorfuSequencer() {
    // terminate = true;
    // if (sequencer_thread.joinable()) {
    //     sequencer_thread.join();
    // }
    // spdlog::debug("Sequencer thread joined!");
}

void CorfuSequencer::run_sequencer_thread() {
    spdlog::info("Sequencer active polling thread starting.");

    int perf_fd;
    if (use_performance) {
        perf_fd = setup_perf(gettid());
        if (perf_fd < 0) {
            perror("Error opening perf event");
            exit(EXIT_FAILURE);
        }
    }

    while (!end_thread) {
        char* recv_ptr = net->recv_packet();
        
        if (!recv_ptr) {
            continue; 
        }

        // we received a packet!!!!!
        struct corfu_cli_header* recv_header = (struct corfu_cli_header*)recv_ptr;

        if (ntohs(recv_header->proto_type) == CORFU_GETTOKEN_PROTO_TYPE) {
            uint32_t idx = assign_next_idx();
            uint32_t cid = ntohl(recv_header->client_id);
            uint32_t thread_id = ntohl(recv_header->thread_id);
            spdlog::debug("Sequencer received a token request from client {}:{}", cid, thread_id);

            
            size_t size_of_reply = get_corfu_gettoken_reply_size();
            size_t size_of_hdr = get_corfu_seq_header_size();
            spdlog::debug("Corfu gettoken reply, size of: {}, header size: {}", size_of_reply, size_of_hdr);
            uint64_t allocated_packet_size = size_of_reply + size_of_hdr + 1;

            // Create packet buffer which will be sent  
            std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
            gettoken_reply->log_idx = htonl(idx);

            memcpy(packet.get(), reinterpret_cast<const char*>(header.get()), size_of_hdr); 
            memcpy(packet.get() + size_of_hdr, reinterpret_cast<const char*>(gettoken_reply.get()), size_of_reply + 1);

            spdlog::debug("Sequencer is giving idx {} to client {}:{}", ntohl(gettoken_reply->log_idx), cid, thread_id);

            net->send_client_udp_packet(
                std::move(packet),
                allocated_packet_size, 
                cli_ips[cid],
                std::to_string(send_port + cid * max_num_threads + thread_id)
            );
        } else {
            spdlog::error("PACKET DROPPED: not asking for a token");
        }
    }
}

uint32_t CorfuSequencer::assign_next_idx() {
    return curr_idx.fetch_add(1);
}

uint32_t CorfuSequencer::get_current_idx() {
    return curr_idx.load();
}

void CorfuSequencer::wait_to_finish() {
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
    spdlog::critical("End thread is bool: {}, max seq = {}", end_thread, get_current_idx());
}