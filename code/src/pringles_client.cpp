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
#include <chrono>

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
    uint16_t send_port = (uint16_t)get_send_port(config) + (uint16_t)thread_id;
    uint16_t recv_port = (uint16_t)get_recv_port(config) + (uint16_t)recv_port_offset + (uint16_t)thread_id;	
    this->client_recv_port = std::to_string(recv_port);
    std::string self_ip = get_self_ip(config);
    std::string multicast_ip = "";
    net = std::make_shared<Network>(std::to_string(send_port), 
                                   std::to_string(recv_port),
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
     this->use_switch = get_use_switch(config);
     this->switch_receive_port = get_switch_receive_port(config);
     this->stor_receive_port = get_stor_receive_port(config);

    
     /* Threads */
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
     recv_thread = std::thread(&LogClient::receiver, this);

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
     this->warm_up = get_warm_up(config);
     this->cool_down = get_cool_down(config);
     this->max_duration = get_experiment_duration(config) - warm_up - cool_down; // Duration of the actual experiment

     /* Content */
     // Append
     this->payload_size = get_payload_size(config);
     this->payload = std::string(payload_size, 'x');
     this->append_nonce = get_cli_id(config);
     this->append_cntr = 0;
     this->started_append = false; 
     this->highest_idx_seen = 0;
     this->append_type_hdr = std::make_unique<struct ring_type>();
     append_type_hdr.get()->num_entries = htons(1);
     append_type_hdr.get()->type = htons(ETH_APPEND_REQ);
     append_type_hdr.get()->shard_id = 0;
     append_type_hdr.get()->cid = htonl(cid);
     spdlog::debug("Append type num entries: {}, type: {}, shard_id: {}, cid: {}", ntohs(append_type_hdr.get()->num_entries), ntohs(append_type_hdr.get()->type), ntohl(append_type_hdr.get()->shard_id), ntohl(append_type_hdr.get()->cid));
     this->append_entry_hdr = std::make_unique<struct ring_append_entry>();
     append_entry_hdr.get()->batch_size = 0;
     append_entry_hdr.get()->timestamp = 0;
     append_entry_hdr.get()->status = htonl(1);
     append_entry_hdr.get()->cntrl_pkt_it = htonl(1);
     append_entry_hdr.get()->payload_size = htonl(payload_size);
     append_entry_hdr.get()->thread_id = htonl(thread_id);
     append_entry_hdr.get()->recv_port = htons(recv_port);
     append_entry_hdr.get()->ring_view = htonl(ring_view);
     append_entry_hdr.get()->client_ip = self_client_ip; // TODO
     testing_append = false;

     // Read
     this->read_nonce = get_cli_id(config);
     this->read_type_hdr = std::make_unique<struct ring_type>();
     read_type_hdr.get()->num_entries = htons(1);
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
     testing_read = false;

     // Subscribe
     this->sub_type_hdr = std::make_unique<struct ring_type>();
     sub_type_hdr.get()->num_entries = htonl(1);
     sub_type_hdr.get()->type = htons(ETH_SUBSCRIBE_ENTRY);
     sub_type_hdr.get()->shard_id = 0;
     sub_type_hdr.get()->cid = htonl(cid);
     this->sub_entry_hdr = std::make_unique<struct ring_subscribe_entry>();
     sub_entry_hdr.get()->subscribe = 0;
     sub_entry_hdr.get()->client_ip = self_client_ip;
     sub_entry_hdr.get()->recv_port = htons(recv_port);

     // Tail
     this->tail_type_hdr = std::make_unique<struct ring_type>();
     tail_type_hdr.get()->num_entries = htonl(1);
     tail_type_hdr.get()->type = htons(ETH_TAIL_REQ);
     tail_type_hdr.get()->shard_id = 0;
     tail_type_hdr.get()->cid = htonl(cid);
     this->tail_req_hdr = std::make_unique<struct ring_tail_req>();
     tail_req_hdr.get()->client_ip = self_client_ip;
     tail_req_hdr.get()->recv_port = htons(recv_port);
     tail_req_hdr.get()->hops = htonl(1);
     tail_req_hdr.get()->tail_seq_no = 0;

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
    if (testing_append) {
        append_test_thread.join();
    }
    if (testing_read) {
	read_test_thread.join();
    }
    if (subscribe_thread_running) {
	spdlog::debug("Subscribe is ending!");
        subscribe_thread.join();
    }
    spdlog::debug("Receive is ending!");
    recv_thread.join();
    spdlog::debug("Network is ending!");
    net->done();
    spdlog::debug("All done!");
}

void LogClient::launch_append_execute() {
    this->append_test_thread = std::thread(&LogClient::execute_append, this);
    testing_append = true;
}

void LogClient::execute_append() {
    spdlog::debug("At the beginning of execution here!");	
    spdlog::critical("Execute thread starting with TID = {}", gettid());
    std::string payload(payload_size, 'X');
    uint64_t cnt = 0;
    while (experiment_status()) {	    
 	uint64_t idx = append(payload);
        spdlog::debug("The entry was given index: {}", idx);
	cnt += 1;
    }
    spdlog::critical("Total number of sent appends (NOT necessarily successful): {} from thread {}", cnt, thread_id);
}

void LogClient::receiver() {
    spdlog::critical("network recv thread starting with tid = {}", gettid());
    while (!end_thread) {
        char* recv_ptr = net->recv_packet();
	if (!recv_ptr) {
	    continue;
	}
	struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
	if (ntohs(type_hdr->type) == ETH_APPEND_RESP) {
            struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
            char* pkt = (char*)std::malloc(ntohs(type_hdr->num_entries)*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ntohl(ring->payload_size) + 1));
            memcpy(pkt, recv_ptr, ntohs(type_hdr->num_entries)*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ntohl(ring->payload_size) + 1)); 
	    append_resp_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(append_resp_q_mutex);
            }
	    append_resp_cv.notify_all();
	} else if (ntohs(type_hdr->type) == ETH_READ_RESP) {
            struct ring_read_entry* ring = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
	    uint64_t pkt_size = ntohs(type_hdr->num_entries)*(sizeof(struct ring_type) + sizeof(struct ring_read_entry) + ntohl(ring->payload_size) + 1);
            char* pkt = (char*)std::malloc(pkt_size);
            memcpy(pkt, recv_ptr, pkt_size); 
	    read_resp_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(read_resp_q_mutex);
            }
	    read_resp_cv.notify_all();
	} else if (ntohs(type_hdr->type) == ETH_SUBSCRIBE_ENTRY) {
            struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
	    uint64_t pkt_size = ntohs(type_hdr->num_entries)*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ntohl(ring->payload_size) + 1);
            char* pkt = (char*)std::malloc(pkt_size);
            memcpy(pkt, recv_ptr, pkt_size); 
	    subscribe_resp_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(subscribe_resp_q_mutex);
            }
	    subscribe_resp_cv.notify_all();
	} else if (ntohs(type_hdr->type) == ETH_TAIL_RESP) {
            char* pkt = (char*)std::malloc((sizeof(struct ring_type) + sizeof(struct ring_tail_req)));
            memcpy(pkt, recv_ptr, (sizeof(struct ring_type) + sizeof(struct ring_tail_req))); 
	    tail_resp_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(tail_resp_q_mutex);
            }
	    tail_resp_cv.notify_all();
	} else {
	    spdlog::debug("TYPE UNKNOWN: {}!!!", ntohs(type_hdr->type));
	    continue;
	}
    }
}

uint64_t LogClient::append(std::string entry) {
    spdlog::info("Simple Network: Sending/Receiving to remote host");
    
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
    memcpy(packet.get() + size_of_type_hdr + size_of_hdr, entry.c_str(), payload_size + 1);

    spdlog::debug("Size of packet: {} and size of app info: {} and size of hdr: {} with num entries {}", allocated_packet_size, payload.length(), size_of_hdr, ntohs(append_type_hdr.get()->num_entries));
    bool res = false;
    if (use_switch) {
        spdlog::debug("Sending to the SWITCH at IP {} and port {} at port {} and nonce {}", switch_ip, switch_receive_port, client_recv_port, append_nonce);
        res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, switch_ip, switch_receive_port);
    } else {
        for (uint64_t i = 0; i < stor_ips.size(); i++) {
            spdlog::debug("Sending to the STORAGE SERVER at IP {} and port {} at port {}", stor_ips[i], stor_receive_port, client_recv_port);
            res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, stor_ips[i], stor_receive_port);
        }
    }
    if (!res) {
        spdlog::critical("Sending append to the system failed!");
	return 0;
    }

    bool got_quorum = false;
    uint64_t return_idx = 0;
    while (!got_quorum) {
        if (end_thread) {
            break;
        }

	// Wait to receive the packet 
	char* recv_ptr;
	{
	    std::unique_lock<std::mutex> lock(append_resp_q_mutex);
	    append_resp_cv.wait(lock, [this] {return end_thread || !append_resp_q.empty();});
	    if (end_thread) {
	        break;
	    }
	    if (!append_resp_q.try_pop(recv_ptr) || !recv_ptr) {
	        continue;
	    }
	}

        struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
        struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
        spdlog::debug("Append Registering the time and operation with type {} and nonce {} and seq no {}!", ntohs(type_hdr->type), ntohl(append_entry->nonce), ntohl(append_entry->g_idx));
        if (ntohs(type_hdr->type) == ETH_APPEND_RESP && ntohl(append_entry->nonce) == append_nonce) {
    	    highest_idx_seen = ntohl(append_entry->g_idx);
    	    return_idx = ntohl(append_entry->g_idx);
    	    if (collect_stats && start_time > 0) {
                append_stat->getDuration(start_time);
                append_stat->addOp();
    	        spdlog::debug("!!!!!!!!!!!!!!!GOT HERE IN THREAD {}", thread_id);
    	    }
            got_quorum = true;
        }
    }
    append_nonce += 1;
    append_cntr += 1;
    return return_idx;
}


std::string LogClient::read(uint64_t idx) {
    spdlog::info("Simple Network: Sending/Receiving to remote host");
    
    size_t size_of_hdr = get_ring_read_size();
    size_t size_of_type_hdr = get_ring_type_size();
    read_type_hdr.get()->type = htons(ETH_READ_REQ);
    spdlog::debug("Type header: {}, size of: {} and type hdr: {}", read_type_hdr.get()->type, size_of_hdr, size_of_type_hdr);
    read_entry_hdr.get()->g_idx = htonl(idx);


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
        res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, switch_ip, switch_receive_port);
    } else {
        for (uint64_t i = 0; i < stor_ips.size(); i++) {
            res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, stor_ips[i], stor_receive_port);
        }
    }
    if (!res) {
        spdlog::critical("Sending read to the system failed!");
        return 0;
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
	    read_resp_cv.wait(lock, [this] {return end_thread || !read_resp_q.empty();});
	    if (end_thread) {
	        break;
	    }
	    if (!read_resp_q.try_pop(recv_ptr) || !recv_ptr) {
	        continue;
	    }
	}

        struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
        struct ring_read_entry* read_entry = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
        spdlog::debug("Register the time and operation with type {} and nonce {}, original nonce {}!", ntohs(type_hdr->type), ntohl(read_entry->nonce), read_nonce);
        if (ntohs(type_hdr->type) == ETH_READ_RESP && ntohl(read_entry->nonce) == read_nonce) {
	    char* entry = (char*)(recv_ptr + size_of_type_hdr + size_of_hdr);
	    return_str = std::string(entry);
	    cached_log_entries[ntohl(read_entry->g_idx)] = return_str;
    	    if (collect_stats && start_time > 0) {
		read_stat->getDuration(start_time);
                read_stat->addOp();
    	        spdlog::debug("!!!!!!!!!!!!!!!GOT HERE IN THREAD {}, {}", thread_id, entry);

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
    size_t size_of_hdr = get_ring_tail_size();
    size_t size_of_type_hdr = get_ring_type_size();
    tail_type_hdr.get()->type = htons(ETH_TAIL_REQ);
    spdlog::debug("Type header: {}, size of: {} and type hdr: {}", tail_type_hdr.get()->type, size_of_hdr, size_of_type_hdr);
    tail_req_hdr.get()->nonce = ntohl(tail_nonce);
    uint64_t allocated_packet_size = size_of_type_hdr + size_of_hdr;

    // Only need to read from the map once to get the queue
    // Create packet buffer which will be sent  
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), reinterpret_cast<const char*>(tail_type_hdr.get()), size_of_type_hdr);
    memcpy(packet.get() + size_of_type_hdr, reinterpret_cast<const char*>(tail_req_hdr.get()), size_of_hdr);

    spdlog::debug("Size of packet: {} and size of hdr: {} with num entries {}", allocated_packet_size, size_of_hdr, ntohs(tail_type_hdr.get()->num_entries));
    bool res = false;
    if (use_switch) {
        spdlog::debug("Sending tail request to the SWITCH at IP {} and port {} at port {} and nonce {}", switch_ip, switch_receive_port, client_recv_port, tail_nonce);
        res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, switch_ip, switch_receive_port);
    }
    if (!res) {
        spdlog::critical("Sending getTail to the system failed!");
	return 0;
    }

    bool got_quorum = false;
    uint64_t return_idx = 0;
    while (!got_quorum) {
        if (end_thread) {
            break;
        }

	// Wait to receive the packet 
	char* recv_ptr;
	{
	    std::unique_lock<std::mutex> lock(tail_resp_q_mutex);
	    tail_resp_cv.wait(lock, [this] {return end_thread || !tail_resp_q.empty();});
	    if (end_thread) {
	        break;
	    }
	    if (!tail_resp_q.try_pop(recv_ptr) || !recv_ptr) {
	        continue;
	    }
	}

        struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
        struct ring_tail_req* tail_req = (struct ring_tail_req*)(recv_ptr + sizeof(struct ring_type));
        spdlog::debug("Registering the time and operation with type {} and nonce {}!", ntohs(type_hdr->type), ntohl(tail_req->nonce));
        if (ntohs(type_hdr->type) == ETH_TAIL_RESP && ntohl(tail_req->nonce) == tail_nonce) {
    	    return_idx = ntohl(tail_req->tail_seq_no);
            got_quorum = true;
        }
    }
    tail_nonce += 1;
    return return_idx;
}

// Subscribe to get all log updates after supplied index
// TODO: Add vector that the subscribed entries will go to?
void LogClient::subscribe(uint64_t idx) {
    this->subscribe_thread = std::thread(&LogClient::wait_for_subscribe, this, idx);
    subscribe_thread_running  = true;
}

void LogClient::wait_for_subscribe(uint64_t idx) {
     /* send subscribe request*/
     bool res = false;
     sub_entry_hdr.get()->g_idx = htonl(idx);
     size_t size_of_hdr = get_ring_subscribe_size();
     size_t size_of_type_hdr = get_ring_type_size();
     sub_type_hdr.get()->type = htons(ETH_SUBSCRIBE_ENTRY);
     spdlog::debug("Type header: {}, size of: {} and type hdr: {}", sub_type_hdr.get()->type, size_of_hdr, size_of_type_hdr);
     uint64_t allocated_packet_size = size_of_type_hdr + size_of_hdr;

     // Only need to read from the map once to get the queue
     // Create packet buffer which will be sent  
     std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
     memcpy(packet.get(), reinterpret_cast<const char*>(sub_type_hdr.get()), size_of_type_hdr);
     memcpy(packet.get() + size_of_type_hdr, reinterpret_cast<const char*>(sub_entry_hdr.get()), size_of_hdr);

     if (use_switch) {
         spdlog::debug("SUBSCRIBING to SWITCH at {}:{} from client receive port {}", switch_ip, switch_receive_port, client_recv_port);
         res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, switch_ip, switch_receive_port);
     } else {
	  spdlog::critical("No client<->storage subscribe support at this time.");
	  return;
     }
     if (!res) {
         spdlog::critical("Unable to send subscribe request!");
	 return;
     }
     auto duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
     double start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(duration_since_epoch).count();
    

     /* wait for new appends to roll in */
     uint64_t one_time = 0;
     while (true) {
	if (end_thread) {
            spdlog::debug("!!!!!!!!!!!!!!TIME to stop");
            break;
        }
	// Wait to receive the packet 
	char* recv_ptr;
	{
	    std::unique_lock<std::mutex> lock(subscribe_resp_q_mutex);
	    subscribe_resp_cv.wait(lock, [this] {return end_thread || !subscribe_resp_q.empty();});
	    if (end_thread) {
	        break;
	    }
	    if (!subscribe_resp_q.try_pop(recv_ptr) || !recv_ptr) {
	        continue;
	    }
	}
	
	size_t size_of_hdr = get_ring_append_size();
    	size_t size_of_type_hdr = get_ring_type_size();
        struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
        //spdlog::debug("SUBSCRIBER registering the time and operation with type {} and nonce {}!", ntohs(type_hdr->type), ntohl(append_entry->nonce));
	char* entry = (char*)(recv_ptr + size_of_type_hdr + size_of_hdr);
	std::string return_str = std::string(entry);
	cached_log_entries[ntohl(append_entry->g_idx)] = return_str;
        spdlog::debug("SUBSCRIBER Idx: {} and Entry: {}!", ntohl(append_entry->g_idx), entry);
	if (one_time < 1) {
    	    duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
	    double end_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(duration_since_epoch).count();
    	    double dur = end_time_s - start_time_s;
    	    spdlog::critical("Duration to first message: {}", dur);
	    one_time+=1;
	}
     }
}

/*======================================== vPringles: Streaming supported version of the protocol ==========================*/
// TODO read_stream, subscribe_stream <-- check vcorfu for more
uint64_t LogClient::append_stream(std::string entry, uint32_t stream_id) {
    spdlog::info("Simple Network: Sending/Receiving to remote host");
    
    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();
    append_type_hdr.get()->type = htons(ETH_APPEND_REQ);
    spdlog::debug("Type header: {}, size of: {} and type hdr: {}", append_type_hdr.get()->type, size_of_hdr, size_of_type_hdr);
    append_entry_hdr.get()->g_idx = 0;
    append_entry_hdr.get()->stream_id = htonl(stream_id);
    uint64_t allocated_packet_size = size_of_type_hdr + size_of_hdr + payload_size + 1;

    // Only need to read from the map once to get the queue
    // Create packet buffer which will be sent  
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    double start_time = collect_stats ? append_stat->getStartLat() : 0;
    append_entry_hdr.get()->nonce = htonl(append_nonce);
    memcpy(packet.get(), reinterpret_cast<const char*>(append_type_hdr.get()), size_of_type_hdr);
    memcpy(packet.get() + size_of_type_hdr, reinterpret_cast<const char*>(append_entry_hdr.get()), size_of_hdr);
    memcpy(packet.get() + size_of_type_hdr + size_of_hdr, entry.c_str(), payload_size + 1);

    spdlog::debug("Size of packet: {} and size of app info: {} and size of hdr: {} with num entries {}", allocated_packet_size, payload.length(), size_of_hdr, ntohs(append_type_hdr.get()->num_entries));
    bool res = false;
    if (use_switch) {
        spdlog::debug("Sending to the SWITCH at IP {} and port {} at port {} and nonce {}", switch_ip, switch_receive_port, client_recv_port, append_nonce);
        res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, switch_ip, switch_receive_port);
    } else {
        for (uint64_t i = 0; i < stor_ips.size(); i++) {
            spdlog::debug("Sending to the STORAGE SERVER at IP {} and port {} at port {}", stor_ips[i], stor_receive_port, client_recv_port);
            res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, stor_ips[i], stor_receive_port);
        }
    }
    if (!res) {
        spdlog::critical("Sending append to the system failed!");
	return 0;
    }

    bool got_quorum = false;
    uint64_t return_idx = 0;
    while (!got_quorum) {
        if (end_thread) {
            break;
        }

	// Wait to receive the packet 
	char* recv_ptr;
	{
	    std::unique_lock<std::mutex> lock(append_resp_q_mutex);
	    append_resp_cv.wait(lock, [this] {return end_thread || !append_resp_q.empty();});
	    if (end_thread) {
	        break;
	    }
	    if (!append_resp_q.try_pop(recv_ptr) || !recv_ptr) {
	        continue;
	    }
	}

        struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
        struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
        spdlog::debug("Append Registering the time and operation with type {} and nonce {} and seq no {}!", ntohs(type_hdr->type), ntohl(append_entry->nonce), ntohl(append_entry->g_idx));
        if (ntohs(type_hdr->type) == ETH_APPEND_RESP && ntohl(append_entry->nonce) == append_nonce) {
    	    highest_idx_seen = ntohl(append_entry->g_idx);
    	    return_idx = ntohl(append_entry->g_idx);
    	    if (collect_stats && start_time > 0) {
                append_stat->getDuration(start_time);
                append_stat->addOp();
    	        spdlog::debug("!!!!!!!!!!!!!!!GOT HERE IN THREAD {}", thread_id);
    	    }
            got_quorum = true;
        }
    }
    append_nonce += 1;
    append_cntr += 1;
    return return_idx;
}

std::string LogClient::read_stream(uint64_t idx, uint32_t stream_id) {
    spdlog::info("Simple Network: Sending/Receiving to remote host");
    
    size_t size_of_hdr = get_ring_read_size();
    size_t size_of_type_hdr = get_ring_type_size();
    read_type_hdr.get()->type = htons(ETH_READ_REQ);
    spdlog::debug("Type header: {}, size of: {} and type hdr: {}", read_type_hdr.get()->type, size_of_hdr, size_of_type_hdr);
    read_entry_hdr.get()->g_idx = htonl(idx);
    read_entry_hdr.get()->stream_id = htonl(stream_id);


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
        res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, switch_ip, switch_receive_port);
    } else {
        for (uint64_t i = 0; i < stor_ips.size(); i++) {
            res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, stor_ips[i], stor_receive_port);
        }
    }
    if (!res) {
        spdlog::critical("Sending read to the system failed!");
        return 0;
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
	    read_resp_cv.wait(lock, [this] {return end_thread || !read_resp_q.empty();});
	    if (end_thread) {
	        break;
	    }
	    if (!read_resp_q.try_pop(recv_ptr) || !recv_ptr) {
	        continue;
	    }
	}

        struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
        struct ring_read_entry* read_entry = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
        spdlog::debug("Register the time and operation with type {} and nonce {}, original nonce {}!", ntohs(type_hdr->type), ntohl(read_entry->nonce), read_nonce);
        if (ntohs(type_hdr->type) == ETH_READ_RESP && ntohl(read_entry->nonce) == read_nonce) {
	    char* entry = (char*)(recv_ptr + size_of_type_hdr + size_of_hdr);
	    return_str = std::string(entry);
	    cached_log_entries[ntohl(read_entry->g_idx)] = return_str;
    	    if (collect_stats && start_time > 0) {
		read_stat->getDuration(start_time);
                read_stat->addOp();
    	        spdlog::debug("!!!!!!!!!!!!!!!GOT HERE IN THREAD {}, {}", thread_id, entry);

    	    }
            got_quorum = true;
        }
    }
    read_nonce += 1;
    read_cntr += 1;
    return return_str;
}

void LogClient::subscribe_stream(uint32_t stream_id) {
    this->subscribe_thread = std::thread(&LogClient::wait_for_stream_subscribe, this, stream_id);
    subscribe_thread_running  = true;
}

void LogClient::wait_for_stream_subscribe(uint32_t stream_id) {
     /* send subscribe request*/
     bool res = false;
     sub_entry_hdr.get()->stream_id = htonl(stream_id);
     size_t size_of_hdr = get_ring_subscribe_size();
     size_t size_of_type_hdr = get_ring_type_size();
     sub_type_hdr.get()->type = htons(ETH_SUBSCRIBE_ENTRY);
     spdlog::debug("Type header: {}, size of: {} and type hdr: {}", sub_type_hdr.get()->type, size_of_hdr, size_of_type_hdr);
     uint64_t allocated_packet_size = size_of_type_hdr + size_of_hdr;

     // Only need to read from the map once to get the queue
     // Create packet buffer which will be sent  
     std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
     memcpy(packet.get(), reinterpret_cast<const char*>(sub_type_hdr.get()), size_of_type_hdr);
     memcpy(packet.get() + size_of_type_hdr, reinterpret_cast<const char*>(sub_entry_hdr.get()), size_of_hdr);

     if (use_switch) {
         spdlog::debug("SUBSCRIBING to SWITCH at {}:{} from client receive port {}", switch_ip, switch_receive_port, client_recv_port);
         res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, switch_ip, switch_receive_port);
     } else {
	  spdlog::critical("No client<->storage subscribe support at this time.");
	  return;
     }
     if (!res) {
         spdlog::critical("Unable to send subscribe request!");
	 return;
     }

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
	    subscribe_resp_cv.wait(lock, [this] {return end_thread || !subscribe_resp_q.empty();});
	    if (end_thread) {
	        break;
	    }
	    if (!subscribe_resp_q.try_pop(recv_ptr) || !recv_ptr) {
	        continue;
	    }
	}
	
	size_t size_of_hdr = get_ring_append_size();
    	size_t size_of_type_hdr = get_ring_type_size();
        struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
        struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
        spdlog::debug("SUBSCRIBER registering the time and operation with type {} and nonce {}!", ntohs(type_hdr->type), ntohl(append_entry->nonce));
	char* entry = (char*)(recv_ptr + size_of_type_hdr + size_of_hdr);
	std::string return_str = std::string(entry);
	cached_log_entries[ntohl(append_entry->g_idx)] = return_str;
        spdlog::debug("SUBSCRIBER Idx: {} and Entry: {}!", ntohl(append_entry->g_idx), entry);
     }
}



// Garbage collect all log entries up to index idx  TODO
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
	spdlog::critical("========================= CLIENT STATISTICS ================================");
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
