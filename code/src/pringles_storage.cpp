#include "pringles_storage.h"
#include "spdlog/spdlog.h"
#include "ringclient.pb.h"
#include "ring_headers.h"
#include "yaml-cpp/yaml.h"
#include "utils.h"

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
#include <sched.h>

LogStorage::LogStorage(std::string input_file, uint64_t storage_id) {
    spdlog::critical("Pringles Storage is starting!");
    YAML::Node config = YAML::LoadFile(input_file);

    // Create network
    this->net = std::make_unique<Network>(std::to_string(get_send_port(config)), 
                                   get_stor_receive_port(config),
				   get_socket_type(config),
                                   get_log_level(config),
				   get_batch_size(config), 
				   get_batch_on(config),
				   get_batch_timeout(config),
				   get_interface(config),
				   get_self_ip(config),
				   get_num_pkt_types(config),
				   false); 

    // Initialize storage server identity variables
    this->shard_id = get_shard_id(config);
    this->shard_switch_id = get_shard_switch_id(config);
    this->view_num = 1;
    this->max_duration = get_experiment_duration(config);
    this->stor = StorageType(get_storage_type(config));
    this->ssid = storage_id;
    set_spdlog_level(get_log_level(config));

    // Routing
    this->use_switch = get_use_switch(config);
    this->switch_mac = get_switch_mac(config);
    this->switch_ip = get_switch_ip(config);
    this->switch_receive_port = get_switch_receive_port(config);

    // Thread
    this->recv_thread = std::thread(&receiver, net, get_switch_ip(config), get_switch_receive_port(config));

    this->append_cntr = 0;
}

LogStorage::~LogStorage() {
    append_req_cv.notify_all();
    read_req_cv.notify_all();
    recv_thread.join();
    net->done();
}

void LogStorage::store(uint64_t idx, std::string entry) {
    tbb::concurrent_hash_map<uint64_t, std::string>::accessor accessor;
    bool insert_succ = concurrent_stor.insert(accessor, idx);
    if (insert_succ) {
        accessor->second = entry;
    }
    accessor.release();
}

std::string LogStorage::get(uint64_t idx) {
    std::string entry = "";
    tbb::concurrent_hash_map<uint64_t, std::string>::accessor accessor;
    // 2. Attempt to find the key
    if (concurrent_stor.find(accessor, idx)) {
	entry = accessor->second;
    } else {
	spdlog::warn("Key {} not found!!!", idx);
    }
    return entry;
}

void LogStorage::change_view(uint64_t new_view_num) {
    view_num = new_view_num;
}

void LogStorage::wait_to_finish() {
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
}

void LogStorage::receiver() {
    spdlog::critical("network recv thread starting with tid = {}", gettid());
    while (!end_thread) {
        char* recv_ptr = net->recv_packet();
	if (!recv_ptr) {
	    continue;
	}
	struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
	if (type_hdr->type == ETH_APPEND_REQ) {
            struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
            char* pkt = (char*)std::malloc(ring->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ring->payload_size + 1));
            memcpy(pkt, recv_ptr, ring->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ring->payload_size + 1)); 
	    append_req_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(append_req_q_mutex);
            }
	    append_req_cv.notify_all();
	} else if (type_hdr->type == ETH_READ_REQ) {
            struct ring_read_entry* ring = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
            char* pkt = (char*)std::malloc(ring->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_read_entry) + ring->payload_size + 1));
            memcpy(pkt, recv_ptr, ring->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_read_entry) + ring->payload_size + 1)); 
	    read_req_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(read_req_q_mutex);
            }
	    read_req_cv.notify_all();
	} else {
	    spdlog::debug("TYPE UNKNOWN!!!");
	    continue;
	}
    }
}

void LogStorage::append_server() {
    spdlog::critical("Network Storage Thread starting with TID = {}", gettid());
    spdlog::info("Simple Net Server, about to start with {}!", !end_thread);
    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();

    while (!end_thread) {
     	bool got_quorum = false;

        while (!got_quorum) {
	    if (end_thread) {
	        break;
	    }

	    char* recv_ptr;
	    {
		std::unique_lock<std::mutex> lock(recv_q_mutex);
		append_cv.wait(lock, [] {return end_thread || !recv_q.empty();});
		if (!recv_q.try_pop(recv_ptr) || !recv_ptr) {
            	    net->send_udp_packet(NULL, 0, 0, ETH_APPEND_REQ, switch_ip, switch_recv_port); // TODO is this needed?
	            continue;
	        }
	    }
	    struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
	    uint64_t num_entries = ntohl(append_entry->num_entries);
	    
	    // Update the type of the type header
	    uint64_t recv_offset = 0;
	    spdlog::debug("Num entries {}, Payload size: {}", ntohl(append_entry->num_entries), ntohl(append_entry->payload_size));
	    for (uint64_t i = 0; i < num_entries; i++) {
	        struct ring_type* type_hdr = (struct ring_type*)(recv_ptr + recv_offset);
	        type_hdr->type = htons(ETH_APPEND_RESP);

	         // Create reply packet
 	        uint64_t reply_pkt_size = size_of_type_hdr + size_of_hdr;
     	        std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);

	        // Get the next append entry header to process and its payload
	        struct ring_append_entry* batch_append_entry = (struct ring_append_entry*)(recv_ptr + recv_offset + size_of_type_hdr);
	        char* entry = (char*)(recv_ptr + recv_offset + size_of_type_hdr + size_of_hdr);
	        
	        // Actually store the entry
	        std::string dummy(entry); 
	        store(ntohl(batch_append_entry->g_idx), dummy);
		max_append_idx = batch_append_entry->g_idx;
	        spdlog::debug("The updated index is: {}, Entry: {}, Recv port: {}", batch_append_entry->g_idx, dummy, batch_append_entry->recv_port);
	        
	        // Copy both the type header and the append entry header into the reply packet buffer
	        uint64_t old_payload_size = ntohl(batch_append_entry->payload_size);
		batch_append_entry->payload_size = htonl(0);
                memcpy(reply_packet.get(), recv_ptr + recv_offset, reply_pkt_size);
	        
	        if (use_switch) {
     	             net->send_udp_packet(std::move(reply_packet), reply_pkt_size, 0, ETH_APPEND_RESP, switch_ip, switch_recv_port);
	        } else {
		    char buffer[INET_ADDRSTRLEN];
    		    if (inet_ntop(AF_INET, &batch_append_entry->client_ip, buffer, INET_ADDRSTRLEN) == nullptr) {
		         spdlog::critical("UH OH UNABLE TO GET DOTTED_QUAD STRING");
		         memset(buffer, 0, INET_ADDRSTRLEN);
    		    }
		    std::string client_ip(buffer);
     	            net->send_client_udp_packet(std::move(reply_packet), reply_pkt_size, 0, ETH_APPEND_RESP, client_ip, std::to_string(batch_append_entry->recv_port));
	        }
		append_cntr += 1;
	        
	        recv_offset += (size_of_type_hdr + size_of_hdr + old_payload_size + 1);
	    }
	    got_quorum = true;
	}
	
	// Create packet buffer which will be sent  
	if (end_thread) {
	    break;
	}
    }
    spdlog::critical("=============== Number of append packets processed is {} with max append sequence number {} ===========================", append_cntr, append_idx);
}

void LogStorage::read_server() {
    spdlog::critical("Network Storage Thread starting with TID = {}", gettid());
    spdlog::info("Simple Net Server, about to start with {}!", !end_thread);
    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();

    while (!end_thread) {
     	bool got_quorum = false;

        while (!got_quorum) {
	    if (end_thread) {
	        break;
	    }

	    char* recv_ptr;
	    {
		std::unique_lock<std::mutex> lock(read_req_q_mutex);
		read_req_cv.wait(lock, [] {return end_thread || !read_req_q.empty();});
		if (!recv_q.try_pop(recv_ptr) || !recv_ptr) {
            	    net->send_udp_packet(NULL, 0, 0, ETH_READ_REQ, switch_ip, switch_recv_port); // TODO is this needed?
	            continue;
	        }
	    }
	    struct ring_read_entry* read_entry = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
	    uint64_t num_entries = ntohl(read_entry->num_entries);
	    
	    // Update the type of the type header
	    uint64_t recv_offset = 0;
	    spdlog::debug("Num entries {}, Payload size: {}", ntohl(read_entry->num_entries), ntohl(read_entry->payload_size));
	    for (uint64_t i = 0; i < num_entries; i++) {
	        struct ring_type* type_hdr = (struct ring_type*)(recv_ptr + recv_offset);
	        type_hdr->type = htons(ETH_READ_RESP);

	         // Create reply packet
 	        uint64_t reply_pkt_size = size_of_type_hdr + size_of_hdr;
     	        std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);

	        // Get the next append entry header to process and its payload
	        struct ring_read_entry* batch_read_entry = (struct ring_read_entry*)(recv_ptr + recv_offset + size_of_type_hdr);
		std::string entry = get(ntohl(batch_read_entry->g_idx));
	        spdlog::debug("The updated index is: {}, Entry: {}, Recv port: {}", batch_read_entry->g_idx, entry, batch_read_entry->recv_port);
	        
	        // Copy both the type header and the append entry header into the reply packet buffer
		batch_read_entry->payload_size = htonl(entry.length());
                memcpy(reply_packet.get(), recv_ptr + recv_offset, reply_pkt_size);
                memcpy(reply_packet.get(), recv_ptr + recv_offset + size_of_type_hdr + size_of_hdr, entry.c_str());
	        
	        if (use_switch) {
     	             net->send_udp_packet(std::move(reply_packet), reply_pkt_size, 0, ETH_READ_RESP, switch_ip, switch_recv_port);
	        } else {
		    char buffer[INET_ADDRSTRLEN];
    		    if (inet_ntop(AF_INET, &batch_read_entry->client_ip, buffer, INET_ADDRSTRLEN) == nullptr) {
		         spdlog::critical("UH OH UNABLE TO GET DOTTED_QUAD STRING");
		         memset(buffer, 0, INET_ADDRSTRLEN);
    		    }
		    std::string client_ip(buffer);
     	            net->send_client_udp_packet(std::move(reply_packet), reply_pkt_size, 0, ETH_READ_RESP, client_ip, std::to_string(batch_read_entry->recv_port));
	        }
		read_cntr += 1;
	        recv_offset += (size_of_type_hdr + size_of_hdr + 1);
	    }
	    got_quorum = true;
	}
	
	// Create packet buffer which will be sent  
	if (end_thread) {
	    break;
	}
    }

    spdlog::critical("=============== Number of read requests seen: {} ===========================", read_cntr);
}
