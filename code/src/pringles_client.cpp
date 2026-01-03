#include <thread>
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
#include <atomic>

#include "ring_headers.h"
#include "ringclient.pb.h"
#include "pringles_client.h"
#include "utils.h"
#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"
#define RECEIVE_PORT 3149

LogClient::LogClient(std::string input_file, uint64_t thread_id, uint64_t num_threads) {
   YAML::Node config = YAML::LoadFile(input_file);
  
   // ID and log level
   this->cid = get_cli_id(config);
   set_spdlog_level(get_log_level(config));
   spdlog::info("Pringles Client: Only Append being tested");
   
   // Get network information 
   this->send_port = get_send_port(config) + thread_id;
   this->client_recv_port = get_recv_port(config) + num_threads + thread_id;	
   this->switch_ip = get_switch_ip(config); 
   this->switch_recv_port = get_switch_receive_port(config);
   this->stor_ips = get_stor_ips(config);
   this->stor_recv_port = get_stor_receive_port(config);
   this->self_ip = get_self_ip(config);
   this->use_switch = get_use_switch(config);
   
   this->payload_size = get_payload_size(config);
   this->batch_size = get_num_client_threads(config) * payload_size;
   spdlog::debug("Batch size: {}", batch_size);
   uint64_t client_batch_size = get_num_failures(config) + 1;	 // TODO naming?
   this->net = std::make_shared<Network>(std::to_string(send_port), 
                                  	   std::to_string(client_recv_port),
				  	   get_socket_type(config),
                                  	   get_log_level(config),
				  	   client_batch_size,
				  	   get_batch_on(config),
					   get_batch_timeout(config),
				  	   get_interface(config),
				           get_self_ip(config),
				  	   get_num_pkt_types(config),
				  	   false);

    // Updating the log 
    cached_log_entries = {};

    // Ack counting 
    this->min_matching_acks = get_num_failures(config) + 1;
    this->ack_cntr = 0;

    // Keeps track of stats
    this->max_duration = get_experiment_duration(config);
    this->warm_up = get_warm_up(config);
    this->cool_down = get_cool_down(config);
    this->global_thread_id = thread_id;
    this->json_name = get_json_name(config);
    this->stat = std::make_unique<Stats>(get_batch_size(config), get_batch_on(config), get_json_name(config), thread_id, get_self_ip(config));
    this->collect_stats = false;
    this->total_packet_cntr = 0;

    this->batch_on = get_batch_on(config);
    this->stor_ips = get_stor_ips(config);

    spdlog::critical("Network Client Thread starting with TID = {}, internal thread id {}", gettid(), thread_id);
}

LogClient::~LogClient() {
    spdlog::debug("Client is ending!");
    spdlog::critical("Highest index seen returned: {}", highest_idx_seen);
    spdlog::critical("SEND THREAD sent {} appends in {} seconds.", total_packet_cntr, max_duration);
    spdlog::critical("For SEND thread {}, the lat is: {}, tput: {}, total ops: {}", global_thread_id, stat->getAvgLatency(), stat->getThroughput(max_duration), stat->getTotalOps());

    stat->exportResultsToJson();
    
    if (subscribe_thread_running) {
        subscribe_thread.join();
	spdlog::debug("Subscribe thread joined!");
    }

    spdlog::debug("Joined the client threads!");
    net->done();
}

uint32_t LogClient::append(std::string payload) {
    spdlog::info("Simple Network: Sending/Receiving to remote host");

    uint64_t nonce = global_thread_id;
    uint64_t highest_idx = 0;

    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();

    std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(nonce, global_thread_id);
    hdr.get()->payload_size = payload_size;
    hdr.get()->num_entries = 1;
    hdr.get()->thread_id = global_thread_id;
    hdr.get()->recv_port = client_recv_port; // TODO
    hdr.get()->nonce = nonce;
    hdr.get()->cli_idx = cid;
    std::unique_ptr<struct ring_type> type_hdr = create_ring_type(ETH_APPEND_REQ);
    spdlog::debug("Type header: {}, size of: {} and type hdr: {}", type_hdr.get()->type, size_of_hdr, size_of_type_hdr);
    
    // Create packet buffer which will be sent  
    uint64_t allocated_packet_size = size_of_type_hdr + size_of_hdr + payload_size + 1;
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), reinterpret_cast<const char*>(type_hdr.get()), size_of_type_hdr);
    memcpy(packet.get() + size_of_type_hdr, reinterpret_cast<const char*>(hdr.get()), size_of_hdr);
    memcpy(packet.get() + size_of_type_hdr + size_of_hdr, payload.c_str(), payload.length() + 1);
    
    double start_time = collect_stats ? stat->getStartLat() : 0;
    bool res = false;
    if (use_switch) {
        spdlog::debug("Sending to the SWITCH at IP {} and port {} at port {} and idx {}", switch_ip, switch_recv_port, client_recv_port, cid);
        res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ, switch_ip, switch_recv_port);
    } else {
        for (uint64_t i = 0; i < stor_ips.size(); i++) {
            res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ, stor_ips[i], stor_recv_port);
        }
    }
    if (!res) {
        spdlog::critical("Packet failed to send!!");
    }
    
    total_packet_cntr += 1;

    bool got_quorum = false;
    while (!got_quorum) {
        if (end_thread) {
            break;
        }
    
        char* recv_ptr = net->recv_packet();
        if (!recv_ptr) {
            continue;
        }
       
        // TODO no ack counting
        
	struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
        struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
        if (type_hdr->type == ETH_APPEND_RESP && append_entry->nonce == nonce && collect_stats && start_time > 0) {
    	    highest_idx = append_entry->g_idx;
	    highest_idx_seen = highest_idx;
            stat->getDuration(start_time);
            stat->addOp();
            got_quorum = true;
    	    spdlog::debug("!!!!!!!!!!!!!!!GOT HERE IN THREAD {}", global_thread_id);
        }
    }
    return highest_idx; 
}

std::string LogClient::read(uint64_t idx) {
    /*spdlog::info("Simple Network: Sending/Receiving to remote host");

    uint64_t nonce = global_thread_id;
    uint64_t highest_idx = 0;

    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();

    std::unique_ptr<struct ring_read_entry> hdr = create_ring_read_entry(nonce, global_thread_id);
    hdr.get()->payload_size = payload_size;
    hdr.get()->num_entries = 1;
    hdr.get()->thread_id = global_thread_id;
    hdr.get()->recv_port = client_recv_port; // TODO
    hdr.get()->nonce = nonce;
    hdr.get()->cli_idx = cid;
    std::unique_ptr<struct ring_type> type_hdr = create_ring_type(ETH_APPEND_REQ);
    spdlog::debug("Type header: {}, size of: {} and type hdr: {}", type_hdr.get()->type, size_of_hdr, size_of_type_hdr);
    
    // Create packet buffer which will be sent  
    uint64_t allocated_packet_size = size_of_type_hdr + size_of_hdr + payload_size + 1;
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), reinterpret_cast<const char*>(type_hdr.get()), size_of_type_hdr);
    memcpy(packet.get() + size_of_type_hdr, reinterpret_cast<const char*>(hdr.get()), size_of_hdr);
    memcpy(packet.get() + size_of_type_hdr + size_of_hdr, payload.c_str(), payload.length() + 1);
    
    double start_time = collect_stats ? stat->getStartLat() : 0;
    bool res = false;
    if (use_switch) {
        spdlog::debug("Sending to the SWITCH at IP {} and port {} at port {} and idx {}", switch_ip, switch_recv_port, client_recv_port, cid);
        res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ, switch_ip, switch_recv_port);
    } else {
        for (uint64_t i = 0; i < stor_ips.size(); i++) {
            res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ, stor_ips[i], stor_recv_port);
        }
    }
    if (!res) {
        spdlog::critical("Packet failed to send!!");
    }
    
    total_packet_cntr += 1;

    bool got_quorum = false;
    while (!got_quorum) {
        if (end_thread) {
            break;
        }
    
        char* recv_ptr = net->recv_packet();
        if (!recv_ptr) {
            continue;
        }
       
        // TODO no ack counting
        
	struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
        struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
        if (type_hdr->type == ETH_APPEND_RESP && append_entry->nonce == nonce && collect_stats && start_time > 0) {
    	    highest_idx = append_entry->g_idx;
	    highest_idx_seen = highest_idx;
            stat->getDuration(start_time);
            stat->addOp();
            got_quorum = true;
    	    spdlog::debug("!!!!!!!!!!!!!!!GOT HERE IN THREAD {}", global_thread_id);
        }
    }*/

	
	
	
	return NULL;
}

// Get latest committed entry
uint64_t LogClient::getTail() {
	// TODO later
	return 0;
}

// Subscribe to get all log updates after supplied index
// TODO: Add vector that the subscribed entries will go to?
void LogClient::subscribe(uint64_t idx) {
    this->subscribe_thread = std::thread(&LogClient::wait_for_subscribe, this, idx);
    this->subscribe_thread_running  = true;
}

void LogClient::wait_for_subscribe(uint64_t idx) {
     (void) idx;
     // Communicate subscriber preference to storage servers
     while (!end_thread) {
	 // set up condition variable to wake up thread
	 // read off the subscribe queue
	 // push entry and index to cached log
	//cached_log_entries.insert(std::pair<uint64_t, std::string>(recv_idx, payload.mutable_append()->entry()));
     }
}


// TODO Garbage collect all log entries up to some index
bool LogClient::trim(uint64_t idx) {
	(void) idx;
	return false;
}

uint64_t LogClient::get_client_payload_size() {
    return payload_size;
}

void LogClient::update_stats(bool update_stats_collection) {
    collect_stats = update_stats_collection;
}

void LogClient::finish() {
    end_thread = true;
}

/* Experiment Logistics */
void LogClient::wait_to_finish() {
    collect_stats = true;
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    spdlog::debug("Collecting statistics!");
    stat->getAvgLatency();
    stat->getThroughput(max_duration);
    stat->getTotalOps();
    stat->exportResultsToJson();
    collect_stats = false;
} 

void LogClient::wait_to_warmup() {
    std::chrono::seconds sleep_duration(warm_up);
    std::this_thread::sleep_for(sleep_duration);
    collect_stats = true;
}

void LogClient::wait_to_cooldown() {
    std::chrono::seconds sleep_duration(cool_down);
    std::this_thread::sleep_for(sleep_duration);
    spdlog::debug("End thread: {}", end_thread);
    end_thread = true;
} 

/* Header functions */
size_t LogClient::get_size_of_hdr(uint64_t pkt_type) {
    if (PacketType(pkt_type) == PacketType::append) {
        return sizeof(struct ring_append_entry);
    }
    return 0;
}
