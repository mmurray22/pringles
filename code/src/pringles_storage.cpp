#include "pringles_storage.h"
#include "spdlog/spdlog.h"
#include "ringclient.pb.h"
#include "ring_headers.h"
#include "yaml-cpp/yaml.h"
#include "utils.h"

#include <errno.h>
#include <numeric>
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
    
    // Shards
    this->shard_id = get_shard_id(config);
    this->use_shards = get_use_shards(config);

    // Streams
    this->use_streams = get_use_streams(config);

    // Create network
    this->net = std::make_shared<Network>(std::to_string(get_send_port(config)), 
                                   get_stor_receive_port(config),
				   get_socket_type(config),
                                   get_log_level(config),
				   get_batch_size(config), 
				   get_batch_on(config),
				   get_batch_timeout(config),
				   get_interface(config),
				   get_self_ip(config),
				   get_multicast_addr(config),
				   //use_shards,
				   false,
				   false); 

    // Initialize storage server identity variables

    this->shard_switch_id = get_shard_switch_id(config);
    this->view_num = 1;
    this->max_duration = get_experiment_duration(config);
    this->ssid = storage_id;
    this->use_switch = get_use_switch(config);
    set_spdlog_level(get_log_level(config));

    // Routing
    this->use_switch = get_use_switch(config);
    this->switch_mac = get_switch_mac(config);
    this->switch_ip = get_switch_ip(config);
    this->switch_recv_port = get_switch_receive_port(config);

    // Thread
    recv_thread = std::thread(&LogStorage::receiver, this);
    append_thread = std::thread(&LogStorage::append_server, this);
    read_thread = std::thread(&LogStorage::read_server, this);
    this->append_cntr = 0;
    spdlog::critical("Done with the constructor!");
}

LogStorage::~LogStorage() {
    recv_thread.join();
    append_req_cv.notify_all();
    read_req_cv.notify_all();
    append_thread.join();
    read_thread.join();
    net->done();
}

bool LogStorage::store(uint64_t idx, std::string entry) {
    tbb::concurrent_hash_map<uint64_t, std::string>::accessor accessor;
    bool insert_succ = concurrent_stor.insert(accessor, idx);
    if (insert_succ) {
        accessor->second = entry;
    }
    accessor.release();
    return insert_succ;
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
    pin_current_thread_linux(0);
    while (!end_thread) {
        char* recv_ptr = net->recv_packet();
	if (!recv_ptr) {
	    continue;
	}
	struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
	if (ntohs(type_hdr->type) == ETH_APPEND_REQ) {
	    spdlog::debug("ETH_APPEND_REQ");
            struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
	    uint64_t pkt_size = ntohs(type_hdr->num_entries)*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ntohl(ring->payload_size) + 1);
            char* pkt = (char*)std::malloc(pkt_size);
            memcpy(pkt, recv_ptr, pkt_size); 
	    append_req_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(append_req_q_mutex);
            }
	    append_req_cv.notify_all();
	} else if (ntohs(type_hdr->type) == ETH_READ_REQ) {
	    spdlog::debug("ETH_READ_REQ");
            struct ring_read_entry* ring = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
	    uint64_t pkt_size = ntohs(type_hdr->num_entries)*(sizeof(struct ring_type) + sizeof(struct ring_read_entry) + ntohl(ring->payload_size) + 1);
            char* pkt = (char*)std::malloc(pkt_size);
            memcpy(pkt, recv_ptr, pkt_size); 
	    read_req_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(read_req_q_mutex);
            }
	    read_req_cv.notify_all();
	} else {
	    spdlog::debug("TYPE UNKNOWN: {}!!!", ntohs(type_hdr->type));
	    continue;
	}
    }
}

void LogStorage::append_server() {
    spdlog::critical("Network Storage Thread starting with TID = {}", gettid());
    spdlog::info("Simple Net Server, about to start with {}!", !end_thread);
    pin_current_thread_linux(1);
    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();
    std::vector<double> lats;

     while (!end_thread) {
         char* recv_ptr;
         {
     	     std::unique_lock<std::mutex> lock(append_req_q_mutex);
     	     append_req_cv.wait(lock, [this] {return end_thread || !append_req_q.empty();});
     	     if (!append_req_q.try_pop(recv_ptr) || !recv_ptr) {
                 net->send_udp_packet(NULL, 0, switch_ip, switch_recv_port, false); // TODO is this needed?
                 continue;
             }
         }

	 spdlog::debug("RECEIVED APPEND PACKET!!!");
         auto duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
	 double start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(duration_since_epoch).count();


         uint64_t recv_offset = 0;
         struct ring_type* type_hdr = (struct ring_type*)(recv_ptr);
         // Update the type of the type header
         type_hdr->type = htons(ETH_APPEND_RESP);
         uint64_t num_entries = ntohs(type_hdr->num_entries);
         spdlog::debug("Num entries {}", ntohs(type_hdr->num_entries));
         for (uint64_t i = 0; i < num_entries; i++) {
             // Get the append entry header
             struct ring_append_entry* batch_append_entry = (struct ring_append_entry*)(recv_ptr + recv_offset + size_of_type_hdr);
     	     // Get append payload
 	     uint64_t sequence_no = ntohl(batch_append_entry->g_idx);
             char* entry = (char*)(recv_ptr + recv_offset + size_of_type_hdr + size_of_hdr);
           
	      
     	     // Actually store the entry
             std::string string_to_store(entry); 
             store(sequence_no, string_to_store);
     	     max_append_idx = sequence_no;
             spdlog::debug("The updated index is: {}, Entry: {}, Recv port: {}", max_append_idx, string_to_store, ntohs(batch_append_entry->recv_port));
 
     	     // Create reply packet
             uint64_t reply_pkt_size = size_of_type_hdr + size_of_hdr + ntohl(batch_append_entry->payload_size) + 1;
             std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);


             // Copy both the type header and the append entry header into the reply packet buffer
             memcpy(reply_packet.get(), recv_ptr + recv_offset, reply_pkt_size);
             
             if (use_switch) {
     	         spdlog::debug("Sending to the switch! IP: {} and Port: {}", switch_ip, switch_recv_port);
                 net->send_udp_packet(std::move(reply_packet), reply_pkt_size, switch_ip, switch_recv_port, false);
             } else {
     	         char buffer[INET_ADDRSTRLEN];
     	         if (inet_ntop(AF_INET, &batch_append_entry->client_ip, buffer, INET_ADDRSTRLEN) == nullptr) {
     	             spdlog::critical("UH OH UNABLE TO GET DOTTED_QUAD STRING");
     	             memset(buffer, 0, INET_ADDRSTRLEN);
     		     throw;
     	         }
     	         std::string client_ip(buffer);
                 net->send_client_udp_packet(std::move(reply_packet), reply_pkt_size, client_ip, std::to_string(ntohs(batch_append_entry->recv_port)));
             }

	     duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
	     double end_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(duration_since_epoch).count();
	     double dur = end_time_s - start_time_s;
	     lats.push_back(dur);
     	     append_cntr += 1;
             recv_offset += reply_pkt_size;
         }
     }
     double final_avg_latency = std::accumulate(lats.begin(), lats.end(), 0.0) / lats.size();
     final_avg_latency *= 1000;
     spdlog::critical("=============== Number of append packets processed is {} with max append sequence number {} and avg latency {} ===========================", append_cntr, max_append_idx, final_avg_latency);

}

void LogStorage::read_server() {
    spdlog::critical("Network Storage Thread starting with TID = {}", gettid());
    spdlog::info("Simple Net Server, about to start with {}!", !end_thread);

    pin_current_thread_linux(2);
    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();

    while (!end_thread) {
     	bool got_quorum = false;

        while (!got_quorum) {
	    if (end_thread) {
	        break;
	    }

	    // Receive new packet
	    char* recv_ptr;
	    {
		std::unique_lock<std::mutex> lock(read_req_q_mutex);
		read_req_cv.wait(lock, [this] {return end_thread || !read_req_q.empty();});
		if (!read_req_q.try_pop(recv_ptr) || !recv_ptr) {
            	    net->send_udp_packet(NULL, 0, switch_ip, switch_recv_port, false); // TODO is this needed?
	            continue;
	        }
	    }

	    spdlog::debug("RECEIVED READ PACKET!!!");
	    // Read entry
	    struct ring_type* type_hdr = (struct ring_type*)(recv_ptr);
	    struct ring_read_entry* read_entry = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
	    
	    // Update the type of the type header
	    type_hdr->type = htons(ETH_READ_RESP);

	    // Get the next append entry header to process and its payload
	    std::string entry = get(ntohl(read_entry->g_idx));
	    read_entry->payload_size = htonl(entry.length());
	    spdlog::debug("Index is: {}, Entry: {}, Recv port: {}", ntohl(read_entry->g_idx), entry, ntohs(read_entry->recv_port));
	    

	    // Create reply packet
 	    uint64_t reply_pkt_size = size_of_type_hdr + size_of_hdr + ntohl(read_entry->payload_size) + 1;
     	    std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);
	    // Copy both the type header, read entry header, and payload into the reply packet buffer
            memcpy(reply_packet.get(), recv_ptr, size_of_type_hdr);
            memcpy(reply_packet.get() + size_of_type_hdr, recv_ptr + size_of_type_hdr, size_of_hdr);
            memcpy(reply_packet.get() + size_of_type_hdr + size_of_hdr, entry.c_str(), ntohl(read_entry->payload_size) + 1);
	    
	    if (use_switch) {
     	         net->send_udp_packet(std::move(reply_packet), reply_pkt_size, switch_ip, switch_recv_port, false);
	    } else {
	        char buffer[INET_ADDRSTRLEN];
    	        if (inet_ntop(AF_INET, &read_entry->client_ip, buffer, INET_ADDRSTRLEN) == nullptr) {
	             spdlog::critical("UH OH UNABLE TO GET DOTTED_QUAD STRING");
	             memset(buffer, 0, INET_ADDRSTRLEN);
    	        }
	        std::string client_ip(buffer);
     	        net->send_client_udp_packet(std::move(reply_packet), reply_pkt_size, client_ip, std::to_string(ntohs(read_entry->recv_port)));
	    }
	    read_cntr += 1;
	}
	
	// Create packet buffer which will be sent  
	if (end_thread) {
	    break;
	}
    }

    spdlog::critical("=============== Number of read requests seen: {} ===========================", read_cntr);
}
