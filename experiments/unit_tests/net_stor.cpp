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

std::mutex recv_q_mutex;
std::condition_variable cv;
tbb::concurrent_hash_map<uint64_t, std::string> concurrent_stor;
tbb::concurrent_queue<char*> recv_q;

void store(uint64_t idx, std::string entry) {
    tbb::concurrent_hash_map<uint64_t, std::string>::accessor accessor;
    bool insert_succ = concurrent_stor.insert(accessor, idx);
    if (insert_succ) {
        accessor->second = entry;
    }
    accessor.release();
}

void receiver(std::shared_ptr<Network> net, std::string switch_ip, std::string switch_receive_port) {
    spdlog::critical("Network Recv Thread starting with TID = {}", gettid());
    (void) switch_ip;
    (void) switch_receive_port;
    while (!end_thread) {
        char* recv_ptr = net->recv_packet();
	if (!recv_ptr) {
	    recv_q.push(NULL);
            {
	        std::unique_lock<std::mutex> lock(recv_q_mutex);
            }
	    cv.notify_all();
	    continue;
	}
	struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
	spdlog::debug("Received a packet: {}!", ntohs(type_hdr->type));
   	if (ntohs(type_hdr->type) != ETH_APPEND_REQ) { // Only supports append requests right now
	    continue;
	}
        struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
	spdlog::debug("Number of entries: {}", ntohl(ring->num_entries));
        char* pkt = (char*)std::malloc(ntohl(ring->num_entries)*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ntohl(ring->payload_size) + 1));
        memcpy(pkt, recv_ptr, ntohl(ring->num_entries)*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ntohl(ring->payload_size) + 1)); 
 	recv_q.push(pkt);
        {
	    std::unique_lock<std::mutex> lock(recv_q_mutex);
        }
	cv.notify_all();
    }
}

void custom_udp_server(std::shared_ptr<Network> net, 
		   std::string json_name, 
		   uint64_t thread_id, 
		   uint64_t batch_size, 
		   bool batch_on,
		   bool use_switch,
		   std::string switch_ip,
		   std::string switch_recv_port,
		   std::vector<std::string> cli_ips) {
    spdlog::critical("Network Storage Thread starting with TID = {}", gettid());
    (void) json_name;
    (void) thread_id;
    (void) batch_size;
    (void) batch_on;
    (void) cli_ips;

    //std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id);
    spdlog::info("Simple Net Server, about to start with {}!", !end_thread);
    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();

    uint64_t idx = 1;
    while (!end_thread) {
     	bool got_quorum = false;

        while (!got_quorum) {
	    if (end_thread) {
	        break;
	    }

	    char* recv_ptr;
	    {
		std::unique_lock<std::mutex> lock(recv_q_mutex);
		cv.wait(lock, [] {return end_thread || !recv_q.empty();});
		if (!recv_q.try_pop(recv_ptr) || !recv_ptr) {
            	    net->send_udp_packet(NULL, 0, 0, ETH_APPEND_REQ, switch_ip, switch_recv_port); // TODO TEST IF THIS IS THE ISSUE
	            continue;
	        }
	    }
	    /*recv_ptr = net->recv_packet();
	    if (!recv_ptr) {
                net->send_udp_packet(NULL, 0, 0, ETH_APPEND_REQ, switch_ip, switch_recv_port);
		continue;
	    }*/
	    struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
	    uint64_t num_entries = ntohl(append_entry->num_entries);
	    
	    // Update the type of the type header

	    uint64_t recv_offset = 0;
	    spdlog::debug("IN THE RECEIVE THREAD: Num entries {}, Payload size: {}", ntohl(append_entry->num_entries), ntohl(append_entry->payload_size));
	    for (uint64_t i = 0; i < num_entries; i++) {
	        struct ring_type* type_hdr = (struct ring_type*)(recv_ptr + recv_offset);
	        type_hdr->type = htons(ETH_APPEND_RESP);
	        spdlog::debug("Batch append entry payload size: {}, receive port: {}",  ntohl(((struct ring_append_entry*)(recv_ptr + recv_offset + sizeof(struct ring_type)))->payload_size), ntohl(((struct ring_append_entry*)(recv_ptr + recv_offset + sizeof(struct ring_type)))->recv_port));
	         // Create reply packet
 	        uint64_t reply_pkt_size = size_of_type_hdr + size_of_hdr;
     	        std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);

	        // Get the next append entry header to process and its payload
	        //spdlog::debug("WE ARE ON ITERATION: {} with offset {}", i, recv_offset);
	        //spdlog::debug("THIS PACKET HAS TYPE {}", ((struct ring_type*)(recv_ptr + recv_offset))->type);
	        struct ring_append_entry* batch_append_entry = (struct ring_append_entry*)(recv_ptr + recv_offset + size_of_type_hdr);
	        char* entry = (char*)(recv_ptr + recv_offset + size_of_type_hdr + size_of_hdr);
	        
	        // Actually store the entry
	        std::string dummy(entry); 
	        //spdlog::debug("Dummy entry length: {}", dummy.length());
	        store(idx, dummy);
	        //spdlog::debug("The updated index is: {}, Entry: {}, Recv port: {}", idx, dummy, batch_append_entry->recv_port);
	        
	        // Update append entry header with the assigned index
	        batch_append_entry->g_idx = htonl(idx);
	        idx += 1;

	        // Copy both the type header and the append entry header into the reply packet buffer
	        //spdlog::debug("Batch append entry payload size: {}, num entries: {}", batch_append_entry->payload_size, batch_append_entry->num_entries);
	        uint64_t old_payload_size = ntohl(batch_append_entry->payload_size);
		batch_append_entry->payload_size = htonl(0);
                memcpy(reply_packet.get(), recv_ptr + recv_offset, reply_pkt_size);
	        
	        //spdlog::debug("Eth header type: {}", ((struct ring_type*)recv_ptr)->type);
	        // Send the packet to the network library to send out
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
	        
	        recv_offset += (size_of_type_hdr + size_of_hdr + old_payload_size + 1);
	    }
	    got_quorum = true;
	}
	
	// Create packet buffer which will be sent  
	if (end_thread) {
	    break;
	}
    }

    spdlog::critical("The number of indices given out is: {}", idx);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }

    spdlog::critical("Network Main Thread starting with TID = {}", gettid());
    std::string input_file = std::string(argv[1]);
    YAML::Node config = YAML::LoadFile(input_file);
    std::vector<int> eth_types = {ETH_APPEND_REQ};
    std::vector<std::thread> server_threads;
    std::shared_ptr<Network> net = std::make_unique<Network>(std::to_string(get_send_port(config)), 
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
    set_spdlog_level(get_log_level(config));
    
    std::thread recv_thread(&receiver, net, get_switch_ip(config), get_switch_receive_port(config));
    pthread_t recv_native_handle = recv_thread.native_handle();
    // Create a CPU set and add the desired core
    cpu_set_t recv_cpuset;
    CPU_ZERO(&recv_cpuset);
    CPU_SET(std::thread::hardware_concurrency() - 1, &recv_cpuset); // Pin to core 'i'
    int recv_result = pthread_setaffinity_np(recv_native_handle, sizeof(cpu_set_t), &recv_cpuset);
    if (recv_result != 0) {
        std::cerr << "Error setting thread affinity for thread " << recv_thread.get_id() << ": " << recv_result << std::endl;
    }

    spdlog::info("Simple Network server");
    std::string json_name = "dummy";
    uint64_t NUM_THREADS = get_storage_server(config);

    for (uint64_t i = 0; i < NUM_THREADS; i++) {
        server_threads.emplace_back(std::thread(custom_udp_server, net, json_name, 0, get_batch_size(config), get_batch_on(config), get_use_switch(config), get_switch_ip(config), get_switch_receive_port(config), get_cli_ip(config)));

        pthread_t native_handle = server_threads[i].native_handle();

        // Create a CPU set and add the desired core
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i % NUM_THREADS, &cpuset); // Pin to core 'i'

        // Set thread affinity
        int result = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpuset);
        if (result != 0) {
            std::cerr << "Error setting thread affinity for thread " << server_threads[i].get_id() << ": " << result << std::endl;
        }
    }

    uint64_t max_duration = get_experiment_duration(config);

    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
    cv.notify_all();
    for (uint64_t i = 0; i < server_threads.size(); i++) {
         server_threads[i].join();
    }
    recv_thread.join();
    net->done();
    spdlog::critical("Joined all the threads!");
    return 0;
}

