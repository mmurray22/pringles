/*
 * Simple Network Test File
 *
 * This test is meant to test basic sending and receiving sockets on a single host.
 * Arguments:
 * - Path to yaml file 
 */

#include "network.h"
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
#include <queue>
#include <unordered_map>
#include <condition_variable>

#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_hash_map.h>
#include "spdlog/spdlog.h"
#include "utils.h"
#include "measure.h"
#include "ring_headers.h"

const uint64_t MAX_WAIT_TIME = 100;
uint16_t NUM_THREADS = 1;
std::string sequence_pkt_type = "sequencer";
std::string storage_pkt_type = "storage";
bool end_thread = false;
std::atomic<bool> collect_stats = true;

std::mutex recv_q_mutex;
tbb::concurrent_hash_map<uint64_t, tbb::concurrent_queue<char*>> recv_q;

//std::unordered_map<uint64_t, std::queue<char*>> recv_q;
std::condition_variable cv;

void receiver(int recv_socket,
              char* recv_ptr) {

    spdlog::critical("Network Recv Thread starting with TID = {}", gettid());
    while (!end_thread) {
        int numbytes = 0;
        struct sockaddr_storage src_addr;
        socklen_t addr_len = sizeof src_addr;
        if ((numbytes = recvfrom(recv_socket, recv_ptr, MAX_PACKET_SIZE, 0, (struct sockaddr *)&src_addr, &addr_len)) < 0) {
            continue;
        }

        if (!recv_ptr) {
            continue;
        }
        struct ethhdr* eth = (struct ethhdr*)recv_ptr;
        if (ntohs(eth->h_proto) != ETH_APPEND_RESP) {
            continue;
        }

	// Unpack batch
        size_t size_of_hdr = get_ring_append_size();
	struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr));
	uint64_t recv_offset = sizeof(struct ethhdr) + sizeof(iphdr) + size_of_hdr;
	//spdlog::debug("Received NEW packet with num entries = {}", append_entry->num_entries);
	for (uint64_t i = 0; i < append_entry->num_entries; i++) {
	    struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + recv_offset);
	    //spdlog::debug("Received packet with num entries = {}, The payload size: {}, thread id: {}", ring->num_entries, ring->payload_size, ring->thread_id);
	    char* pkt = (char*)std::malloc(sizeof(struct ring_append_entry) + ring->payload_size);
            memcpy(pkt, (char*)(recv_ptr + recv_offset), sizeof(struct ring_append_entry) + ring->payload_size); 
        
    	    tbb::concurrent_hash_map<uint64_t, tbb::concurrent_queue<char*>>::accessor acc;
 	    if (!recv_q.find(acc, ring->thread_id % NUM_THREADS)) {
	    	spdlog::debug("UNKNOWN THREAD ID {}, skipping", ring->thread_id);
	    }
            tbb::concurrent_queue<char*>& push_q = acc->second;
	    acc.release();
	    //spdlog::debug("THREAD ID {}", ring->thread_id);


	    push_q.push(pkt);
	    recv_offset += (size_of_hdr + ring->payload_size);
	}

	{
	    std::unique_lock<std::mutex> lock(recv_q_mutex);
        }
	//spdlog::debug("NOTFYING THE CONDITION VARIABLE!");
	cv.notify_all();
    }
}

void custom_client_receiver(std::shared_ptr<Network> net, 
		   uint64_t thread_id,
		   std::string json_name,
		   uint64_t batch_size,
		   bool batch_on,
		   std::string self_ip) {
    uint64_t nonce = thread_id;
    uint64_t highest_idx = 0;

    spdlog::info("Simple Network: Sending/Receiving to remote host");
    spdlog::critical("Network Client Receiver Thread starting with TID = {}, internal thread id {}", gettid(), thread_id);
    std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id, self_ip);

    auto start_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
    double start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(start_duration_since_epoch).count();
    
    // Only need to read from the map once to get the queue
    while (!end_thread) {
     	// Create packet buffer which will be sent  
     	bool got_quorum = false;
        double start_time = stat->getStartLat();
        while (!got_quorum) {
	    if (end_thread) {
                break;
	    }
	
	    char* recv_ptr = net->recv_packet();
	    if (!recv_ptr) {
	        continue;
	    }
	    
	    spdlog::debug("Registering the time and operation!");
	    struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
            struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
	    if (type_hdr->type == ETH_APPEND_RESP && (append_entry->nonce - nonce) % NUM_THREADS == 0) {
		spdlog::debug("Append RESPONSE RECEIVED from nonce {}!!!", append_entry->nonce);
	        stat->getDuration(start_time);
	        stat->addOp();
	        got_quorum = true;
	    }
        }
    }
    auto end_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
    double end_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(end_duration_since_epoch).count();
    double dur = end_time_s - start_time_s;
    spdlog::debug("Made it out of the loop!");
    spdlog::critical("Highest index seen is: {}", highest_idx);
    spdlog::critical("For  RECEIVER thread {}, the lat is: {}, tput: {}, total ops: {}", thread_id, stat->getAvgLatency(), stat->getThroughput((uint64_t)dur), stat->getTotalOps());
    stat->exportResultsToJson();
    net->done();
}

void custom_client(std::shared_ptr<Network> net, 
		   uint64_t thread_id,
		   std::string json_name,
		   uint64_t batch_size,
		   bool batch_on,
		   uint64_t payload_size,
		   std::string switch_ip,
		   std::vector<std::string> stor_ips,
		   std::string stor_receive_port,
		   std::string switch_receive_port,
		   uint16_t client_recv_port,
		   bool use_switch,
		   bool use_stor,
		   std::string self_ip,
		   uint64_t cli_idx,
		   uint64_t max_duration) {
    (void) use_stor;
    uint64_t nonce = thread_id;
    uint64_t highest_idx = 0;

    std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id, self_ip);


    
    spdlog::info("Simple Network: Sending/Receiving to remote host");
    spdlog::critical("Network Client Thread starting with TID = {}, internal thread id {}", gettid(), thread_id);
    
    //std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id, self_ip);
    uint64_t cntr = 0;
    std::string payload(payload_size, 'x');
    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();
    std::unique_ptr<struct ring_append_entry> hdr = std::make_unique<struct ring_append_entry>();
    std::unique_ptr<struct ring_type> type_hdr = std::make_unique<struct ring_type>();
    type_hdr.get()->type = htons(ETH_APPEND_REQ);
    type_hdr.get()->num_entries = htonl(1);
    type_hdr.get()->shard_id = 0;
    type_hdr.get()->cid = htonl(thread_id);
    spdlog::debug("Type header: {}, size of: {} and type hdr: {}", type_hdr.get()->type, size_of_hdr, size_of_type_hdr);
    hdr.get()->payload_size = htonl(payload_size);
    hdr.get()->num_entries = htonl(1);
    hdr.get()->thread_id = htonl(thread_id);
    hdr.get()->recv_port = htons(client_recv_port);
    hdr.get()->g_idx = 0;
    hdr.get()->batch_size = 0;
    hdr.get()->timestamp = 0;
    hdr.get()->ring_view = htonl(1);
    hdr.get()->status = htonl(1);
    hdr.get()->cntrl_pkt_it = htonl(1);
    uint64_t allocated_packet_size = size_of_type_hdr + size_of_hdr + payload_size + 1;
    uint64_t num_entries = 1;

    // Only need to read from the map once to get the queue
    while (!end_thread) {
     	// Create packet buffer which will be sent  
     	std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        double start_time = collect_stats ? stat->getStartLat() : 0;
	//hdr.get()->num_entries = htonl(num_entries);
	hdr.get()->nonce = htonl(nonce);
	memcpy(packet.get(), reinterpret_cast<const char*>(type_hdr.get()), size_of_type_hdr);
	memcpy(packet.get() + size_of_type_hdr, reinterpret_cast<const char*>(hdr.get()), size_of_hdr);
	memcpy(packet.get() + size_of_type_hdr + size_of_hdr, payload.c_str(), payload.length() + 1);

	//spdlog::debug("Size of packet: {} and size of app info: {} and size of hdr: {}", allocated_packet_size, payload.length(), size_of_hdr);
	bool res = false;
	if (use_switch) {
	    spdlog::debug("Sending to the SWITCH at IP {} and port {} at port {} and idx {} and nonce {}", switch_ip, switch_receive_port, client_recv_port, cli_idx, nonce);
	    res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ, switch_ip, switch_receive_port);
	} else {
	    for (uint64_t i = 0; i < stor_ips.size(); i++) {
	        //spdlog::debug("Sending to the STORAGE SERVER at IP {} and port {} at port {} and idx {}", stor_ip, stor_receive_port, client_recv_port, cli_idx);
	        res = net->send_client_udp_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ, stor_ips[i], stor_receive_port);
	    }
	}
        if (!res) { // NEED TO CHECK BATCH SIZE TODO TODO 
	   num_entries += 1;
	   continue;
        } else {
           num_entries = 1;
        }
        cntr += 1;

     	bool got_quorum = false;
        while (!got_quorum) {
	    if (end_thread) {
                break;
	    }
	
	    char* recv_ptr = net->recv_packet();
	    if (!recv_ptr) {
	        continue;
	    }
	    
	    struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
            struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
	    spdlog::debug("Registering the time and operation with type {} and nonce {}!", ntohs(type_hdr->type), ntohl(append_entry->nonce));
	    //unsigned char* type_raw = (unsigned char*)type_hdr;
	    //printf("Raw header bytes: %02x %02x %02x %02x %02x\n", type_raw[0], type_raw[1], type_raw[2], type_raw[3], type_raw[4]);
	    //unsigned char* raw = (unsigned char*)append_entry;
	    //printf("Raw header bytes: %02x %02x %02x %02x %02x\n", raw[0], raw[1], raw[2], raw[3], raw[4]);
	    if (ntohs(type_hdr->type) == ETH_APPEND_RESP && ntohl(append_entry->nonce) == nonce) {
		if (collect_stats && start_time > 0) {
		    highest_idx = ntohl(append_entry->g_idx);
	            stat->getDuration(start_time);
	            stat->addOp();
		    spdlog::debug("!!!!!!!!!!!!!!!GOT HERE IN THREAD {}", thread_id);
		}
	        got_quorum = true;

	    }
        }
	nonce += 1;
    }
    spdlog::debug("Made it out of the loop!");
    spdlog::critical("Highest index seen is: {}", highest_idx);
    spdlog::critical("SEND THREAD sent {} appends in {} seconds.", cntr, max_duration);
    spdlog::critical("For SEND thread {}, the lat is: {}, tput: {}, total ops: {}", thread_id, stat->getAvgLatency(), stat->getThroughput(max_duration), stat->getTotalOps());
    stat->exportResultsToJson();
    net->done();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string input_file = std::string(argv[1]);
    YAML::Node config = YAML::LoadFile(input_file);
    set_spdlog_level(get_log_level(config));
    std::vector<std::thread> client_threads;
    std::vector<std::thread> client_recv_threads;
    std::string json_name = get_json_name(config);
    bool batch_on = get_batch_on(config);
    uint64_t payload_size = get_payload_size(config);
    std::string switch_ip = get_switch_ip(config);
    std::vector<std::string> stor_ips = get_stor_ips(config);
   
    NUM_THREADS = (uint16_t)get_num_client_threads(config);

    uint64_t dur = get_experiment_duration(config) - get_warm_up(config) - get_cool_down(config);
    for (uint64_t i = 0; i < NUM_THREADS; i++) {
        uint64_t send_port = get_send_port(config) + i;
	uint16_t recv_port = get_recv_port(config) + NUM_THREADS + (uint16_t)i;	
	spdlog::critical("Creating client with send port: {} and receive port: {}", send_port, recv_port);
        uint64_t client_batch_size = get_num_failures(config) + 1;	
    	std::shared_ptr<Network> net = std::make_shared<Network>(std::to_string(send_port), 
                                  				 std::to_string(recv_port),
				  				 get_socket_type(config),
                                  				 get_log_level(config),
				  				 client_batch_size,
				  				 get_batch_on(config),
								 get_batch_timeout(config),
				  				 get_interface(config),
				  				 get_self_ip(config),
				  				 get_num_pkt_types(config),
				  				 false);
        client_threads.emplace_back(std::thread(&custom_client, 
				                net, 
						i, 
						json_name, 
						get_batch_size(config), 
						batch_on, 
						payload_size, 
						switch_ip, 
						stor_ips,
						get_stor_receive_port(config), 
						get_switch_receive_port(config),
						recv_port,
						get_use_switch(config),
						get_use_stor(config),
						get_self_ip(config),
					        get_cli_idx(config),
						dur));


        pthread_t native_handle = client_threads[i].native_handle();

    	// Create a CPU set and add the desired core
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
    	CPU_SET((i+1) % (std::thread::hardware_concurrency() - 1), &cpuset); // Pin to core 'i'

        // Set thread affinity
        int result = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpuset);
        if (result != 0) {
            std::cerr << "Error setting thread affinity for thread " << client_threads[i].get_id() << ": " << result << std::endl;
        }  
    }

    spdlog::critical("Warmup! {}", get_warm_up(config));
    std::chrono::seconds warmup(get_warm_up(config));
    
    collect_stats = true;
    spdlog::critical("Working! {}", dur);
    std::chrono::seconds sleep_duration(dur);
    std::this_thread::sleep_for(sleep_duration); 
    collect_stats = false;

    spdlog::critical("Cooldown! {}", get_cool_down(config));
    std::chrono::seconds cooldown(get_cool_down(config));
    std::this_thread::sleep_for(cooldown);
    spdlog::critical("Done with cooldown!", get_cool_down(config));

    end_thread = true;
    spdlog::debug("Thread done!");
    //cv.notify_all();
    for (uint64_t i = 0; i < client_threads.size(); i++) {
        client_threads[i].join();
        //client_recv_threads[i].join();
    }
    spdlog::critical("Done joining the clients!");
    //recv_thread.join();
    return 0;
}

