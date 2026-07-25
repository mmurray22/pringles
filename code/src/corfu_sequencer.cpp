#include "corfu_sequencer.h"
#include <linux/perf_event.h>

CorfuSequencer::CorfuSequencer(std::string input_file) {
    YAML::Node config = YAML::LoadFile(input_file);
    
    this->cli_ips = get_cli_ip(config);

    std::string multicast_ip = "";

    this->send_port = get_recv_port(config);
    this->max_duration = get_experiment_duration(config);
    this->max_num_threads = get_num_client_threads(config);

    this->use_performance = get_use_performance(config);
    SPDLOG_INFO("Sequencer Thread starting with TID = {}", gettid());

    end_thread = false;
    curr_idx.store(1);

    int append_req_port = 60008;
    // this->num_append_req_threads = get_append_req_threads(config);
    for (uint64_t i = 0; i < 1; i++) { // CURRENTLY HARDCODED
	    std::unique_ptr<Network> append_net = std::make_unique<Network>( 
            get_socket_type(config),
            get_log_level(config),
            get_batch_size(config), 
            get_batch_on(config),
            get_batch_timeout(config),
            get_interface(config),
            get_self_ip(config)
        );
        int random_socket = append_net->create_random_port_socket();
        if (random_socket < 0) {
            append_net->stop_batch_threads();
            return;
        }
	    append_socket.push_back(random_socket);
        append_req_threads.emplace_back(std::thread(&CorfuSequencer::run_sequencer_thread, this, append_req_port, std::move(append_net), i));
    }
}

CorfuSequencer::~CorfuSequencer() {
    // terminate = true;
    // if (sequencer_thread.joinable()) {
    //     sequencer_thread.join();
    // }
    // spdlog::debug("Sequencer thread joined!");
}

void CorfuSequencer::run_sequencer_thread(int append_port, std::unique_ptr<Network> append_net, uint64_t thread_id) {
    spdlog::info("Sequencer active polling thread starting.");

    int perf_fd;
    if (use_performance) {
        perf_fd = setup_perf(gettid());
        if (perf_fd < 0) {
            perror("Error opening perf event");
            exit(EXIT_FAILURE);
        }
    }

    int comms_socket = append_net->setup_batch_socket(append_port);
    if (comms_socket < 0) {
        append_net->stop_batch_threads();
	    return;
    }
    int send_socket = append_socket[thread_id];
    spdlog::debug("Created communication socket {}!", comms_socket);

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;

    while (!end_thread) {
        char* buf = append_net->recv_packet(comms_socket);
        if (!buf) {
            continue;
        }
        // we received a packet!!!!!
        struct corfu_cli_header* recv_header = (struct corfu_cli_header*)buf;

        if (ntohs(recv_header->proto_type) == CORFU_GETTOKEN_PROTO_TYPE) {
            uint32_t idx = assign_next_idx();
            uint32_t client_ip = recv_header->client_ip;
            // uint32_t thread_id = ntohl(recv_header->thread_id);
            uint16_t recv_port = ntohs(recv_header->recv_port);
            // spdlog::debug("Sequencer received a token request from client {}:{}", cid, thread_id);

            
            size_t size_of_reply = get_corfu_gettoken_reply_size();
            size_t size_of_hdr = get_corfu_seq_header_size();
            // spdlog::debug("Corfu gettoken reply, size of: {}, header size: {}", size_of_reply, size_of_hdr);
            uint64_t allocated_packet_size = size_of_reply + size_of_hdr + 1;

            // Create packet buffer which will be sent  
            // std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
            struct corfu_gettoken_reply* gettoken_reply = (struct corfu_gettoken_reply*)(buf + sizeof(struct corfu_seq_header));
	        struct corfu_seq_header* hdr = (struct corfu_seq_header*)(buf);
            gettoken_reply->log_idx = htonl(idx);
            hdr->proto_type = htons(CORFU_GETTOKEN_REPLY_PROTO_TYPE);

            // spdlog::debug("Sequencer is giving idx {} to client {}:{}", ntohl(gettoken_reply->log_idx), cid, thread_id);

            // net->send_client_udp_packet(
            //     std::move(packet),
            //     allocated_packet_size,
            //     cli_ips[cid],
            //     std::to_string(send_port + cid * max_num_threads + thread_id)
            // );

            server_addr.sin_port = htons(recv_port);
            server_addr.sin_addr.s_addr = client_ip;
            sendto(send_socket, buf, allocated_packet_size, 0, (struct sockaddr*)&server_addr, sizeof(server_addr));
        } else {
            // spdlog::error("PACKET DROPPED: not asking for a token");
        }
    }
    append_net->stop_batch_threads();
    close(comms_socket);
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