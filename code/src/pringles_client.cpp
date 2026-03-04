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

LogClient::LogClient(std::string input_file, uint64_t thread_id, uint64_t recv_port_offset) {
    this->thread_id = thread_id;
    YAML::Node config = YAML::LoadFile(input_file);
   
    /* Create network */
    uint64_t send_port = std::stoul(get_send_port(config)) + thread_id;
    uint64_t recv_port = std::stoul(get_recv_port(config)) + recv_port_offset + thread_id;	
    this->client_recv_port = std::to_string(recv_port);
    bool run_threads = false;
    std::string self_ip = get_self_ip(config);
    net = std::make_unique<Network>(std::to_string(send_port), 
                                   std::to_string(recv_port),
				   get_socket_type(config),
                                   get_log_level(config),
				   get_batch_size(config),
				   get_batch_on(config),
				   get_interface(config),
				   get_self_ip(config),
				   get_num_pkt_types(config),
				   run_threads);
     this->use_switch = get_use_switch(config);
     this->switch_receive_port = get_switch_receive_port(config);

    
     /* Threads */
     bool run_threads = false;
     this->num_work_threads = get_num_client_threads(config);

     /* Client information */ 
     cid = get_cli_id(config);
     set_spdlog_level(get_log_level(config));
     this->num_ready_bytes = 0;
     this->batch_size = num_work_threads * payload_size;

     /* Log Information */ 
     cached_log_entries = {};
     message_available = false; // TODO: Needed?
     
     /* Receiving */
     this->recv_thread = std::thread(&LogClient::receiver, this);

     /* Routing */   
     this->switch_mac = get_switch_mac(config);
     this->switch_ip = get_switch_ip(config); 
     this->stor_ips = get_stor_ips(config);
     
     struct in_addr addr;
     if (inet_pton(AF_INET, self_ip.c_str(), &addr) != 1) {
         throw std::runtime_error("Invalid IP address format");
     }
     uint32_t self_client_ip = addr.s_addr;
     
     ////// Experiment variables //////
     /* Logistics */
     this->collect_stats = false;
     this->dur = get_experiment_duration(config) - get_warm_up(config) - get_cool_down(config);
     this->num_pkt_types = get_num_pkt_types(config);
     this->ring_view = 1;

     /* Statistics gathering */
     this->append_stat = std::make_unique<Stats>(get_batch_size(config), get_batch_on(config), get_json_name(config), thread_id, self_ip);
     this->read_stat = std::make_unique<Stats>(get_batch_size(config), get_batch_on(config), get_json_name(config), thread_id, self_ip);
     this->max_duration = get_experiment_duration(config);
     this->warm_up = get_warm_up(config);
     this->cool_down = get_cool_down(config);

     /* Content */
     // Append
     this->payload_size = get_payload_size(config);
     this->payload = std::string(payload_size, 'x');
     this->append_nonce = get_cli_id(config);
     this->append_cntr = 0;
     this->started_append = false; 
     this->highest_idx_seen = 0;
     this->append_type_hdr = std::make_unique<struct ring_type>();
     append_type_hdr.get()->num_entries = htonl(1);
     append_type_hdr.get()->type = htons(ETH_APPEND_REQ);
     append_type_hdr.get()->shard_id = 0;
     append_type_hdr.get()->cid = htonl(cid);
     this->append_entry_hdr = std::make_unique<struct ring_append_entry>();
     append_entry_hdr.get()->batch_size = 0;
     append_entry_hdr.get()->timestamp = 0;
     append_entry_hdr.get()->status = htonl(1);
     append_entry_hdr.get()->cntrl_pkt_it = htonl(1);
     append_entry_hdr.get()->num_entries = htonl(1);
     append_entry_hdr.get()->payload_size = htonl(payload_size);
     append_entry_hdr.get()->thread_id = htonl(thread_id);
     append_entry_hdr.get()->recv_port = htons(recv_port);
     append_entry_hdr.get()->ring_view = htonl(ring_view);
     append_entry_hdr.get()->client_ip = self_client_ip;

     // Read
     this->read_nonce = get_cli_id(config);
     this->read_type_hdr = std::make_unique<struct ring_type>();
     read_type_hdr.get()->num_entries = htonl(1);
     read_type_hdr.get()->type = htons(ETH_READ_REQ);
     read_type_hdr.get()->shard_id = 0;
     read_type_hdr.get()->cid = htonl(cid);
     this->read_entry_hdr = std::make_unique<struct ring_read_entry>();
     read_entry_hdr.get()->timestamp = 0;
     read_entry_hdr.get()->status = htonl(1);
     read_entry_hdr.get()->thread_id = htonl(thread_id);
     read_entry_hdr.get()->recv_port = htons(recv_port);
     read_entry_hdr.get()->ring_view = htonl(ring_view);
     read_entry_hdr.get()->circs = 0;
     read_entry_hdr.get()->client_ip = self_client_ip;

     /* Software Client Ack tracking */
     min_matching_acks = get_num_failures(config) + 1;
     append_nonce_idx_map = {};
     append_ack_map = {};
}

LogClient::~LogClient() {
    append_resp_cv.notify_all();
    read_resp_cv.notify_all();
    subscribe_resp_cv.notify_all();
    tail_resp_cv.notify_all();
    if (subscribe_thread_running) {
        subscribe_thread.join();
    }
    recv_thread.join();
    net->done();
}

void LogClient::receiver() {
    spdlog::critical("network recv thread starting with tid = {}", gettid());
    while (!end_thread) {
        char* recv_ptr = net->recv_packet();
	if (!recv_ptr) {
	    continue;
	}
	struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
	if (type_hdr->type == ETH_APPEND_RESP) {
            struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
            char* pkt = (char*)std::malloc(ring->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ring->payload_size + 1));
            memcpy(pkt, recv_ptr, ring->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ring->payload_size + 1)); 
	    append_resp_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(append_resp_q_mutex);
            }
	    append_resp_cv.notify_all();
	} else if (type_hdr->type == ETH_READ_RESP) {
            struct ring_read_entry* ring = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
            char* pkt = (char*)std::malloc(ring->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_read_entry) + ring->payload_size + 1));
            memcpy(pkt, recv_ptr, ring->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_read_entry) + ring->payload_size + 1)); 
	    read_resp_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(read_resp_q_mutex);
            }
	    read_resp_cv.notify_all();
	} else if (type_hdr->type == ETH_SUBSCRIBE_ENTRY) {
            struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
            char* pkt = (char*)std::malloc(ring->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ring->payload_size + 1));
            memcpy(pkt, recv_ptr, ring->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ring->payload_size + 1)); 
	    subscribe_resp_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(subscribe_resp_q_mutex);
            }
	    subscribe_resp_cv.notify_all();
	} else if (type_hdr->type == ETH_TAIL_RESP) {
            struct ring_tail* ring = (struct ring_tail*)(recv_ptr + sizeof(struct ring_type));
            char* pkt = (char*)std::malloc(ring->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_tail)));
            memcpy(pkt, recv_ptr, (sizeof(struct ring_type) + sizeof(struct ring_tail))); 
	    tail_resp_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(tail_resp_q_mutex);
            }
	    tail_resp_cv.notify_all();
	} else {
	    spdlog::debug("TYPE UNKNOWN!!!");
	    continue;
	}
    }
}

uint64_t LogClient::append(std::string entry) {
    spdlog::info("Simple Network: Sending/Receiving to remote host");
    spdlog::critical("Network Client Thread starting with TID = {}, internal thread id {}", gettid());
    
    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();
    append_type_hdr.get()->type = htons(ETH_APPEND_REQ);
    spdlog::debug("Type header: {}, size of: {} and type hdr: {}", append_type_hdr.get()->type, size_of_hdr, size_of_type_hdr);
    append_entry_hdr.get()->g_idx = 0;
    uint64_t allocated_packet_size = size_of_type_hdr + size_of_hdr + payload_size + 1;

    // Only need to read from the map once to get the queue
    // Create packet buffer which will be sent  
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    double start_time = collect_stats ? append_stat->getStartLat() : 0;
    append_entry_hdr.get()->nonce = htonl(append_nonce);
    memcpy(packet.get(), reinterpret_cast<const char*>(append_type_hdr.get()), size_of_type_hdr);
    memcpy(packet.get() + size_of_type_hdr, reinterpret_cast<const char*>(append_entry_hdr.get()), size_of_hdr);
    memcpy(packet.get() + size_of_type_hdr + size_of_hdr, payload.c_str(), payload.length() + 1);

    spdlog::debug("Size of packet: {} and size of app info: {} and size of hdr: {}", allocated_packet_size, payload.length(), size_of_hdr);
    bool res = false;
    if (use_switch) {
        spdlog::debug("Sending to the SWITCH at IP {} and port {} at port {} and idx {} and nonce {}", switch_ip, switch_receive_port, client_recv_port, cli_idx, append_nonce);
        res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ, switch_ip, switch_receive_port);
    } else {
        for (uint64_t i = 0; i < stor_ips.size(); i++) {
            spdlog::debug("Sending to the STORAGE SERVER at IP {} and port {} at port {} and idx {}", stor_ip, stor_receive_port, client_recv_port, cli_idx);
            res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ, stor_ips[i], stor_receive_port);
        }
    }

    bool got_quorum = false;
    while (!got_quorum) {
        if (end_thread) {
            break;
        }

	// Wait to receive the packet 
	char* recv_ptr;
	{
	    std::unique_lock<std::mutex> lock(append_resp_q_mutex);
	    append_resp_cv.wait(lock, [] {return end_thread || !append_resp_q.empty();});
	    if (!append_resp_q.try_pop(recv_ptr) || !recv_ptr) {
	        continue;
	    }
	}

        struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
        struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
        spdlog::debug("Registering the time and operation with type {} and nonce {}!", ntohs(type_hdr->type), ntohl(append_entry->nonce));
        if (ntohs(type_hdr->type) == ETH_APPEND_RESP && ntohl(append_entry->nonce) == append_nonce) {
    	    if (collect_stats && start_time > 0) {
    	        highest_idx_seen = ntohl(append_entry->g_idx);
                append_stat->getDuration(start_time);
                append_stat->addOp();
    	        spdlog::debug("!!!!!!!!!!!!!!!GOT HERE IN THREAD {}", thread_id);
    	    }
            got_quorum = true;
        }
    }
    append_nonce += 1;
    append_cntr += 1;
}


std::string LogClient::read(uint64_t idx) {
    spdlog::info("Simple Network: Sending/Receiving to remote host");
    spdlog::critical("Network Client Thread starting with TID = {}, internal thread id {}", gettid());
    
    size_t size_of_hdr = get_ring_read_size();
    size_t size_of_type_hdr = get_ring_type_size();
    read_type_hdr.get()->type = htons(ETH_READ_REQ);
    spdlog::debug("Type header: {}, size of: {} and type hdr: {}", read_type_hdr.get()->type, size_of_hdr, size_of_type_hdr);
    read_entry_hdr.get()->g_idx = idx;


    // Only need to read from the map once to get the queue
    // Create packet buffer which will be sent  
    uint64_t allocated_packet_size = size_of_type_hdr + size_of_hdr + payload_size + 1;
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    double start_time = collect_stats ? read_stat->getStartLat() : 0;
    read_entry_hdr.get()->nonce = htonl(read_nonce);
    memcpy(packet.get(), reinterpret_cast<const char*>(read_type_hdr.get()), size_of_type_hdr);
    memcpy(packet.get() + size_of_type_hdr, reinterpret_cast<const char*>(read_entry_hdr.get()), size_of_hdr);
    memset(packet.get() + size_of_type_hdr + size_of_hdr, 0, payload.length() + 1);

    spdlog::debug("Size of packet: {} and size of hdr: {}", allocated_packet_size, size_of_hdr);
    bool res = false;
    if (use_switch) {
        spdlog::debug("Sending to the SWITCH at IP {} and port {} at port {} and nonce {}", switch_ip, switch_receive_port, client_recv_port, read_nonce);
        res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, 0, ETH_READ_REQ, switch_ip, switch_receive_port);
    } else {
        for (uint64_t i = 0; i < stor_ips.size(); i++) {
            res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, 0, ETH_READ_REQ, stor_ips[i], stor_receive_port);
        }
    }

    bool got_quorum = false;
    std::string return_str = "";
    while (!got_quorum) {
        if (end_thread) {
            break;
        }

	// Wait to receive the packet 
	char* recv_ptr;
	{
	    std::unique_lock<std::mutex> lock(read_resp_q_mutex);
	    read_resp_cv.wait(lock, [] {return end_thread || !read_resp_q.empty();});
	    if (!read_resp_q.try_pop(recv_ptr) || !recv_ptr) {
	        continue;
	    }
	}

        struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
        struct ring_read_entry* read_entry = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
        spdlog::debug("Register the time and operation with type {} and nonce {}!", ntohs(type_hdr->type), ntohl(read_entry->nonce));
        if (ntohs(type_hdr->type) == ETH_READ_RESP && ntohl(read_entry->nonce) == read_nonce) {
    	    if (collect_stats && start_time > 0) {
		char* entry = (char*)(recv_ptr + size_of_type_hdr + size_of_hdr);
		cached_log_entries.push_back[read_entry->g_idx] = entry;
                read_stat->getDuration(start_time);
                read_stat->addOp();
    	        spdlog::debug("!!!!!!!!!!!!!!!GOT HERE IN THREAD {}, {}", thread_id, entry);
		return_str = std::string(entry);
    	    }
            got_quorum = true;
        }
    }
    read_nonce += 1;
    read_cntr += 1;
    return return_str;
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
    subscribe_thread_running  = true;
}

void LogClient::wait_for_subscribe(uint64_t idx) {
     /* send subscribe request*/
     /* wait for new appends to roll in */
     while (true) {
	if (end_thread) {
            spdlog::debug("!!!!!!!!!!!!!!TIME to stop");
            break;
        }
	// Wait to receive the packet 
	char* recv_ptr;
	{
	    std::unique_lock<std::mutex> lock(subscribe_resp_q_mutex);
	    subscribe_resp_cv.wait(lock, [] {return end_thread || !subscribe_resp_q.empty();});
	    if (!subscribe_resp_q.try_pop(recv_ptr) || !recv_ptr) {
	        continue;
	    }
	}
	
	size_t size_of_hdr = get_ring_append_size();
    	size_t size_of_type_hdr = get_ring_type_size();
        struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
        struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
        spdlog::debug("Registering the time and operation with type {} and nonce {}!", ntohs(type_hdr->type), ntohl(append_entry->nonce));
	char* entry = (char*)(recv_ptr + size_of_type_hdr + size_of_hdr);
	cached_log_entries[append_entry->g_idx] = entry;
     }
}


// Garbage collect all log entries up to some index
bool LogClient::trim(uint64_t idx) {
	(void) idx;
	return false;
}

/* Experiment Logistics */
void LogClient::wait_to_finish(bool is_append) {
    collect_stats = true;
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    spdlog::debug("Collecting statistics!");
    if (is_append) {
	spdlog::critical("========================= CLIENT STATISTICS ================================");
    	spdlog::critical("APPEND Highest index seen is: {}", highest_idx_seen);
    	spdlog::critical("APPEND sent {} appends in {} seconds.", append_cntr, max_duration);
    	spdlog::critical("APPEND STATISTICS: lat is: {}, tput: {}, total ops: {}", append_stat->getAvgLatency(), append_stat->getThroughput(max_duration), append_stat->getTotalOps());
    	append_stat->getAvgLatency();
    	append_stat->getThroughput(max_duration);
    	append_stat->getTotalOps();
    	append_stat->exportResultsToJson();
    } else {
	spdlog::critical("READ sent {} appends in {} seconds.", read_cntr, max_duration);
    	spdlog::critical("READ STATISTICS: lat is: {}, tput: {}, total ops: {}", read_stat->getAvgLatency(), read_stat->getThroughput(max_duration), read_stat->getTotalOps());
	read_stat->getAvgLatency();
    	read_stat->getThroughput(max_duration);
    	read_stat->getTotalOps();
    	read_stat->exportResultsToJson();
    }
    collect_stats = false;
} 

void LogClient::wait_to_warmup() {
    std::chrono::seconds sleep_duration(warm_up);
    std::this_thread::sleep_for(sleep_duration);
}

void LogClient::wait_to_cooldown() {
    std::chrono::seconds sleep_duration(cool_down);
    std::this_thread::sleep_for(sleep_duration);
    spdlog::debug("End thread: {}", end_thread);
    end_thread = true;
} 

bool LogClient::experiment_status() {
    return !end_thread;
}
