#include <chrono>
#include <thread>
#include <iostream>
#include <numeric>
#include <cstring>
#include <utility>
#include <cassert>
#include <fstream>
#include <semaphore>

#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/if_ether.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <net/if.h>
#include <net/ethernet.h>
#include <sys/ioctl.h>
#include <linux/ip.h>
#include <linux/if_packet.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdexcept>
#include <cstring>
#include <cstdint>
#include <sched.h>

#include "spdlog/spdlog.h"
#include "utils.h"
#include "ring_headers.h"
#include "pringles_software_switch.h"

std::counting_semaphore signalAppendReq{0};
std::counting_semaphore signalClientNet{0};

LogSoftwareSwitch::LogSoftwareSwitch(std::string input_file, uint64_t switch_id) {
    spdlog::critical("Pringles Software Switch is starting!");
    YAML::Node config = YAML::LoadFile(input_file);

    this->use_shards = get_use_shards(config);


    this->max_duration = get_experiment_duration(config);
    // Initialize storage server identity variables
    this->switch_id = switch_id;
    this->use_store = get_use_stor(config);
    
    // Sharding information
    /*std::vector<std::vector<std::string>> yaml_shards = get_all_shards(config);
    for (uint64_t i = 0; i < yaml_shards.size(); i++) {
	tbb::concurrent_vector<std::string> shard;
        for (uint64_t j = 0; j < yaml_shards[i].size(); j++) {
	    shard.push_back(yaml_shards[i][j]);
	}
	this->all_shards.push_back(shard);
    } */
    if (this->use_shards) {
        std::vector<std::string> yaml_vec = get_all_shards_multicast(config);
        for (std::string entry : yaml_vec) {
            spdlog::critical("Shard multicast: {}", entry);
            this->all_shards_multicast.push_back(entry);
        }
    }

    // Streaming information
    this->use_streams = get_use_streams(config);

    // Acks
    this->ack_threshold = get_ack_threshold(config);

    // TODO use_client?
    this->view_num = 1;
    set_spdlog_level(get_log_level(config));
    this->max_idx = 0;
    this->stor_ips = get_stor_ips(config);
     


    // Routing
    this->switch_mac = get_switch_mac(config);
    this->switch_ip = get_switch_ip(config);
    this->switch_recv_port = get_switch_receive_port(config);
    this->stor_receive_port = get_stor_receive_port(config);

    // Thread
    //recv_thread = std::thread(&LogSoftwareSwitch::receiver, this);

    int append_req_port = 60008;
    this->num_append_req_threads = get_append_req_threads(config);
    for (uint64_t i = 0; i < num_append_req_threads; i++) {
	std::unique_ptr<Network> append_net = std::make_unique<Network>( 
				   get_socket_type(config),
                                   get_log_level(config),
				   get_batch_size(config), 
				   get_batch_on(config),
				   get_batch_timeout(config),
				   get_interface(config),
				   get_self_ip(config));
        int random_socket = append_net->create_random_port_socket();
        if (random_socket < 0) {
            append_net->stop_batch_threads();
            return;
        }
	append_socket.push_back(random_socket);
        append_req_threads.emplace_back(std::thread(&LogSoftwareSwitch::append_request, this, append_req_port, std::move(append_net), i));
    }
    
    int append_resp_port = 60009;
    this->num_append_resp_threads = 5; // get_append_resp_threads(config);
    for (uint64_t j = 0; j < num_append_resp_threads; j++) {
	std::unique_ptr<Network> append_net = std::make_unique<Network>( 
				   get_socket_type(config),
                                   get_log_level(config),
				   get_batch_size(config), 
				   get_batch_on(config),
				   get_batch_timeout(config),
				   get_interface(config),
				   get_self_ip(config));
        append_resp_threads.emplace_back(std::thread(&LogSoftwareSwitch::append_response, this, append_resp_port, std::move(append_net), j));
    }
}

LogSoftwareSwitch::~LogSoftwareSwitch() {
    for (uint64_t i = 0; i < num_append_resp_threads; i++) { // TODO toggle?
        append_resp_threads[i].join();
    }

    for (uint64_t i = 0; i < num_append_req_threads; i++) { // TODO toggle?
        append_req_threads[i].join();
    }
    //net->done();
}

void LogSoftwareSwitch::append_request(int append_port, std::unique_ptr<Network> append_net, uint64_t thread_id) {
    spdlog::critical("APPEND REQUEST thread starting with tid = {}", gettid());
    int batch_socket = append_net->setup_batch_socket(append_port);
    if (batch_socket < 0) {
        append_net->stop_batch_threads();
	return;
    }
    
    int send_socket = append_socket[thread_id];

    uint64_t pkt_req_cntr = 0;
    uint64_t num_batches = 0;
    uint64_t avg_num_received = 0;
    spdlog::debug("Created batch socket {}!", batch_socket);
    while (!end_thread) {
	int num_received = append_net->recv_many_packets(batch_socket);
	if (num_received < 0) {
	    continue;
	}
	avg_num_received += num_received;
	num_batches += 1;
	spdlog::debug("Received {} packets!", num_received);
        for (int i = 0; i < num_received; i++) {
	    char* buf = append_net->get_buf(i);
	    struct mmsghdr msg = append_net->get_msg(i);

	    if (buf != NULL) {
	        struct ring_append_entry* append_entry = (struct ring_append_entry*)(buf + sizeof(struct ring_type));
	        struct ring_type* type_hdr = (struct ring_type*)(buf);
		max_idx += 1;
		append_entry->g_idx = htonl(max_idx);
		spdlog::debug("Global sequence number is now: {}", ntohl(append_entry->g_idx));

		std::string dest_ip = "";
		uint16_t recv_port = 0;
	        if (use_store) {
		    dest_ip = stor_ips[0];
		    int tmp_port = std::stoi(stor_receive_port);
		    recv_port = static_cast<uint16_t>(tmp_port);
		    spdlog::debug("Store IP addrs: {}, Receive port: {}", dest_ip, recv_port);
    	        } else {
	    	    type_hdr->type = ntohs(ETH_APPEND_RESP);
		    dest_ip = get_quad_ip(append_entry->client_ip);
		    recv_port = ntohs(append_entry->recv_port);
		}
	        if (dest_ip.length() == 0) {
	            continue;
	        }
                struct sockaddr_in server_addr;
                memset(&server_addr, 0, sizeof(server_addr));
                server_addr.sin_family = AF_INET;
                server_addr.sin_port = htons(recv_port);
                server_addr.sin_addr.s_addr = inet_addr(dest_ip.c_str());
		if (msg.msg_len > 0) {
		    struct iovec* iovecs = append_net->get_iovecs();
                    iovecs[i].iov_len = msg.msg_len;
                }
		memcpy(msg.msg_hdr.msg_name, &server_addr, sizeof(server_addr));
	    	pkt_req_cntr += 1;
	    }
	}

	struct mmsghdr* msgs = append_net->get_msgs();
	struct iovec* iovecs = append_net->get_iovecs();
	spdlog::debug("Sending the batch of processed messages out!");
        sendmmsg(send_socket, msgs, num_received, 0); // REPLACED batch_socket with random_socket TODO
        for (int i = 0; i < num_received; i++) {
            msgs[i].msg_hdr.msg_namelen = sizeof(struct sockaddr_in);
            iovecs[i].iov_len = MAX_PACKET_SIZE;
        }
    }
    append_net->stop_batch_threads();
    close(batch_socket);
    spdlog::critical("Packet request counter: {}", pkt_req_cntr);
    uint64_t avg_batch_size = avg_num_received / num_batches;
    spdlog::critical("Average batch size append request: {} ,total num received: {} and num batches {} ", avg_batch_size, avg_num_received, num_batches);
}

void LogSoftwareSwitch::append_response(int append_port, std::unique_ptr<Network> append_net, uint64_t thread_id) {
    spdlog::critical("APPEND RESPONSE Thread starting with TID = {}", gettid());
    pin_current_thread_linux(2);
    (void) append_port; 

    int batch_socket = append_net->create_random_port_socket();
    if (batch_socket < 0) {
        append_net->stop_batch_threads();
        return;
    }

    uint64_t pkt_resp_cntr = 0;
    int recv_socket = append_socket[thread_id];
    
    spdlog::debug("Created batch socket {}!", batch_socket);
    while (!end_thread) {
	int num_received = append_net->recv_many_packets(recv_socket);
	if (num_received < 0) {
	    continue;
	}
	spdlog::debug("Received {} packets!", num_received);
        for (int i = 0; i < num_received; i++) {
	    char* buf = append_net->get_buf(i);
	    struct mmsghdr msg = append_net->get_msg(i);

	    if (buf != NULL) {
	        struct ring_append_entry* append_entry = (struct ring_append_entry*)(buf + sizeof(struct ring_type));
		std::string dest_ip = get_quad_ip(append_entry->client_ip);
		uint16_t recv_port = ntohs(append_entry->recv_port);
	        if (dest_ip.length() == 0) {
	            continue;
	        }
                struct sockaddr_in server_addr;
                memset(&server_addr, 0, sizeof(server_addr));
                server_addr.sin_family = AF_INET;
                server_addr.sin_port = htons(recv_port);
                server_addr.sin_addr.s_addr = inet_addr(dest_ip.c_str());
		if (msg.msg_len > 0) {
		    struct iovec* iovecs = append_net->get_iovecs();
                    iovecs[i].iov_len = msg.msg_len;
                }
		memcpy(msg.msg_hdr.msg_name, &server_addr, sizeof(server_addr));
	    	pkt_resp_cntr += 1;
	    }
	}

	struct mmsghdr* msgs = append_net->get_msgs();
	struct iovec* iovecs = append_net->get_iovecs();
	spdlog::debug("Sending the batch of processed messages out!");
        sendmmsg(batch_socket, msgs, num_received, 0);
        for (int i = 0; i < num_received; i++) {
            msgs[i].msg_hdr.msg_namelen = sizeof(struct sockaddr_in);
            iovecs[i].iov_len = MAX_PACKET_SIZE;
        }
    }
    append_net->stop_batch_threads();
    close(batch_socket);
    spdlog::critical("Packet response counter: {}", pkt_resp_cntr);
}

bool LogSoftwareSwitch::recover_switch() {
    return false; // TODO: Only being implemented in the hardware switch for now
}

void LogSoftwareSwitch::change_view(uint64_t new_view_num) {
    view_num = new_view_num;
}

// Threadpool send thread function
// Send packets as they are queued
// cli_send_q_mutex, cli_send_cv, cli_send_q
void LogSoftwareSwitch::wait_to_finish() {
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
    spdlog::critical("End thread is bool: {}", end_thread);
}

std::string LogSoftwareSwitch::get_quad_ip(uint32_t ip_addr) {
    // INET_ADDRSTRLEN is a standard constant (usually 16)
    char buffer[INET_ADDRSTRLEN];
    
    // Convert the 4 bytes into a dotted-quad string
    if (inet_ntop(AF_INET, &ip_addr, buffer, INET_ADDRSTRLEN) == nullptr) {
         spdlog::critical("UH OH UNABLE TO GET DOTTED_QUAD STRING");
         return "";
    }	
    std::string client_ip(buffer);
    return client_ip;
}
