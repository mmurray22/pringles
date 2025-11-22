/*
 * Simple Network Test File
 *
 * This test is meant to test basic sending and receiving sockets on a single host.
 * Arguments:
 * - Path to yaml file 
 */

#include <chrono>
#include <thread>
#include <iostream>
#include <cstring>
#include <utility>
#include <cassert>
#include <fstream>

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

#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_hash_map.h>
#include "spdlog/spdlog.h"
#include "utils.h"
#include "measure.h"
#include "network.h"
#include "ring_headers.h"

const uint64_t MAX_WAIT_TIME = 100;
std::string sequence_pkt_type = "sequencer";
std::string storage_pkt_type = "storage";
bool end_thread = false;

std::unordered_map<uint64_t, std::string> storage = {};

tbb::concurrent_hash_map<uint64_t, std::string> concurrent_stor;
tbb::concurrent_queue<char*> recv_q;

void store(uint64_t idx, std::string entry) {
    storage.try_emplace(idx, entry);
}

/*void receiver(std::shared_ptr<Network> net) {
    spdlog::critical("Network Recv Thread starting with TID = {}", gettid());
    while (!end_thread) {
        char* recv_ptr = net->recv_packet(); // Make receive separate thread TODO
	if (!recv_ptr) {
	    continue;
	}
        struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
   	if (type_hdr->type != ETH_APPEND_REQ) {
	    continue;
	}

        struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
	char* pkt = (char*)std::malloc(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ring->payload_size);
        memcpy(pkt, recv_ptr, sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ring->payload_size); 
        recv_q.push(pkt);
    }
}*/

void custom_udp_server(std::shared_ptr<Network> net, 
		   std::string json_name, 
		   uint64_t thread_id, 
		   uint64_t batch_size, 
		   bool batch_on,
		   bool use_switch,
		   std::string switch_ip,
		   std::string switch_recv_port,
		   std::string cli_ip) {
    spdlog::critical("Network Storage Thread starting with TID = {}", gettid());
    (void) json_name;
    (void) thread_id;
    (void) batch_size;
    (void) batch_on;

    //std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id);
    spdlog::info("Simple Net Server, about to start with {}!", !end_thread);
    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();

    uint64_t idx = 1;
    uint64_t max_received_idx = 1;
    std::unique_ptr<struct ring_append_entry> resp_hdr = create_ring_append_entry(1, thread_id);
    while (!end_thread) {
     	bool got_quorum = false;
        resp_hdr.get()->num_entries = 0;
	resp_hdr.get()->payload_size = 0;

        while (!got_quorum) {
	     if (end_thread) {
	         break;
	     }

            char* recv_ptr = net->recv_packet(); // Make receive separate thread TODO
	    if (!recv_ptr) {
	        continue;
	    }
	    /////
	    /*char* recv_ptr;
	    if (!recv_q.try_pop(recv_ptr)) {
	        continue;
	    }*/
	    /////

	    struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
   	    if (type_hdr->type == ETH_APPEND_REQ) {
	        got_quorum = true;
		
		// Unpack the batch
		struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
		resp_hdr.get()->num_entries = append_entry->num_entries;
		resp_hdr.get()->payload_size = 0;
		
		//uint64_t reply_pkt_size = size_of_hdr + (size_of_hdr + resp_hdr.get()->payload_size) * append_entry->num_entries;
     	        //std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);
                //memcpy(reply_packet.get(), reinterpret_cast<const char*>(resp_hdr.get()), size_of_hdr);
		
		uint64_t recv_offset = size_of_type_hdr;
		//uint64_t send_offset = size_of_hdr;
		//spdlog::debug("Number of entries received: {}", append_entry->num_entries);
		for (uint64_t i = 0; i < 1 /*append_entry->num_entries*/; i++) {
 		    uint64_t reply_pkt_size = size_of_type_hdr + size_of_hdr;
     	            std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);

		    //spdlog::debug("WE ARE ON ITERATION: {} with offset {}", i, recv_offset);
		    struct ring_append_entry* batch_append_entry = (struct ring_append_entry*)(recv_ptr + recv_offset);
	            char* entry = (char*)(recv_ptr + recv_offset + sizeof(struct ring_append_entry));
	    
	    	    std::string dummy(entry); 
		    spdlog::debug("Dummy entry length: {}", dummy.length());
	    	    store(idx, dummy);
	    	    idx += 1;
		    batch_append_entry->g_idx = idx;
		    type_hdr->type = ETH_APPEND_RESP;
		    //spdlog::debug("The updated index is: {}, Entry: {}", idx, dummy);
		    //spdlog::debug("Batch append entry payload size: {}, num entries: {}", batch_append_entry->payload_size, batch_append_entry->num_entries);
		    //recv_offset += (size_of_hdr + batch_append_entry->payload_size + 1);
		    //batch_append_entry->payload_size = 0;
                    memcpy(reply_packet.get(), recv_ptr, reply_pkt_size);
		    if (use_switch) {
     	    	         net->send_udp_packet(std::move(reply_packet), reply_pkt_size, 0, ETH_APPEND_RESP, switch_ip, switch_recv_port);
		    } else {
     	    	         net->send_udp_packet(std::move(reply_packet), reply_pkt_size, 0, ETH_APPEND_RESP, cli_ip, std::to_string(batch_append_entry->recv_port));
		    }
		}

		//spdlog::debug("Send packet response with size {}!", reply_pkt_size);
     	    	//net->send_packet(std::move(reply_packet), reply_pkt_size, 0, ETH_APPEND_RESP, switch_mac, switch_in_addr);
	    }
	}

	// Create packet buffer which will be sent  
	if (end_thread) {
	    break;
	}
    }
    net->done();
    spdlog::critical("The number of indices given out is: {}", idx);
    spdlog::critical("The max received indices given out is: {}", max_received_idx);
}

void custom_server(std::unique_ptr<Network> net, 
		   std::string json_name, 
		   uint64_t thread_id, 
		   uint64_t batch_size, 
		   bool batch_on,
		   std::array<uint8_t,6> switch_mac,
		   std::string switch_ip) {
    spdlog::critical("Network Storage Thread starting with TID = {}", gettid());
    (void) json_name;
    (void) thread_id;
    (void) batch_size;
    (void) batch_on;

    in_addr_t switch_in_addr = inet_addr(switch_ip.c_str());
    //std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id);
    spdlog::info("Simple Net Server, about to start with {}!", !end_thread);
    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();
    uint64_t idx = 1;
    uint64_t max_received_idx = 1;
    while (!end_thread) {
     	bool got_quorum = false;
        std::unique_ptr<struct ring_append_entry> resp_hdr = create_ring_append_entry(1, thread_id);
        resp_hdr.get()->num_entries = 0;
	resp_hdr.get()->payload_size = 0;

        while (!got_quorum) {
	     if (end_thread) {
	         break;
	     }

            char* recv_ptr = net->recv_packet(); // Make receive separate thread TODO
	    if (!recv_ptr) {
	        continue;
	    }

	    struct ethhdr* eth = (struct ethhdr*)recv_ptr;
   	    if (ntohs(eth->h_proto) == ETH_APPEND_REQ) {
	        got_quorum = true;
		
		// Unpack the batch
		struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr));
		resp_hdr.get()->num_entries = append_entry->num_entries;
		resp_hdr.get()->payload_size = 0;
		uint64_t reply_pkt_size = size_of_hdr + (size_of_hdr + resp_hdr.get()->payload_size) * append_entry->num_entries;
     	        std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);

                memcpy(reply_packet.get(), reinterpret_cast<const char*>(resp_hdr.get()), size_of_hdr);
		uint64_t recv_offset = size_of_type_hdr;
		uint64_t send_offset = size_of_hdr;
		//spdlog::debug("Number of entries received: {}", append_entry->num_entries);
		for (uint64_t i = 0; i < append_entry->num_entries; i++) {
		    //spdlog::debug("WE ARE ON ITERATION: {} with offset {}", i, recv_offset);
		    struct ring_append_entry* batch_append_entry = (struct ring_append_entry*)(recv_ptr + recv_offset);
	            char* entry = (char*)(recv_ptr + recv_offset + sizeof(struct ring_append_entry));
	    
	    	    std::string dummy(entry); 
		    //spdlog::debug("Dummy entry length: {}", dummy.length());
	    	    store(idx, dummy);
	    	    idx += 1;
		    //spdlog::debug("The updated index is: {}, Entry: {}", idx, dummy);
		    //spdlog::debug("Batch append entry payload size: {}, num entries: {}", batch_append_entry->payload_size, batch_append_entry->num_entries);
		    recv_offset += (size_of_hdr + batch_append_entry->payload_size + 1);
		    batch_append_entry->payload_size = 0;
                    memcpy(reply_packet.get() + send_offset, reinterpret_cast<const char*>(batch_append_entry), size_of_hdr);
		    send_offset += size_of_hdr;

		}

		//spdlog::debug("Send packet response with size {}!", reply_pkt_size);
     	    	net->send_packet(std::move(reply_packet), reply_pkt_size, 0, ETH_APPEND_RESP, switch_mac, switch_in_addr);
	    }
	}

	// Create packet buffer which will be sent  
	if (end_thread) {
	    break;
	}
    }
    net->done();
    spdlog::critical("The number of indices given out is: {}", idx);
    spdlog::critical("The max received indices given out is: {}", max_received_idx);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }

    spdlog::critical("Network Main Thread starting with TID = {}", gettid());
    std::string input_file = std::string(argv[1]);
    YAML::Node config = YAML::LoadFile(input_file);
    std::vector<int> eth_types = {ETH_APPEND_REQ};
    std::shared_ptr<Network> net = std::make_unique<Network>(std::to_string(get_send_port(config)), 
                                   get_stor_receive_port(config),
				   get_socket_type(config),
                                   get_log_level(config),
				   get_batch_size(config),
				   get_batch_on(config),
				   get_interface(config),
				   get_self_ip(config),
				   get_num_pkt_types(config),
				   false); 
    set_spdlog_level(get_log_level(config));
    
    /*std::thread recv_thread(&receiver, net);
    pthread_t recv_native_handle = recv_thread.native_handle();
    // Create a CPU set and add the desired core
    cpu_set_t recv_cpuset;
    CPU_ZERO(&recv_cpuset);
    CPU_SET(std::thread::hardware_concurrency() - 1, &recv_cpuset); // Pin to core 'i'
    int recv_result = pthread_setaffinity_np(recv_native_handle, sizeof(cpu_set_t), &recv_cpuset);
    if (recv_result != 0) {
        std::cerr << "Error setting thread affinity for thread " << recv_thread.get_id() << ": " << recv_result << std::endl;
    }*/

    spdlog::info("Simple Network server");
    std::string json_name = "dummy";
    std::thread server_thread(custom_udp_server, std::move(net), json_name, 0, get_batch_size(config), get_batch_on(config), get_use_switch(config), get_switch_ip(config), get_switch_receive_port(config), get_cli_ip(config));

    pthread_t native_handle = server_thread.native_handle();

    // Create a CPU set and add the desired core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset); // Pin to core 'i'

    // Set thread affinity
    int result = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpuset);
    if (result != 0) {
        std::cerr << "Error setting thread affinity for thread " << server_thread.get_id() << ": " << result << std::endl;
    } 

    uint64_t max_duration = get_experiment_duration(config);

    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;

    server_thread.join();
    //recv_thread.join();
    return 0;
}

