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

#include "spdlog/spdlog.h"
#include "utils.h"
#include "measure.h"
#include "network.h"
#include "ring_headers.h"

const double MAX_TIMEOUT = .5;
std::string sequence_pkt_type = "sequencer";
std::string storage_pkt_type = "storage";
bool end_thread = false;
const uint64_t MAX_POLL_TIME = 100;

std::mutex req_q_mutex;
std::condition_variable req_cv;

std::mutex resp_q_mutex;
std::condition_variable resp_cv;

tbb::concurrent_queue<char*> req_q;
tbb::concurrent_queue<char*> resp_q;

void receiver(std::shared_ptr<Network> net, std::string switch_ip, std::string switch_receive_port) {
    spdlog::critical("network recv thread starting with tid = {}", gettid());
    (void) switch_ip;
    (void) switch_receive_port;
    while (!end_thread) {
        char* recv_ptr = net->recv_packet();

	if (!recv_ptr) {
	    req_q.push(NULL);
            {
	        std::unique_lock<std::mutex> lock(req_q_mutex);
            }
	    req_cv.notify_all();
	    continue;
	}
        
	struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
   	if (type_hdr->type != ETH_APPEND_REQ && type_hdr->type != ETH_APPEND_RESP) { // only supports append requests right now
	    continue;
	}
        struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
	uint64_t num_entries = ntohs(type_hdr->num_entries);
	spdlog::debug("number of entries: {}", num_entries);
	uint64_t pkt_size = (num_entries*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ring->payload_size + 1));
        char* pkt = (char*)std::malloc(pkt_size);
        memcpy(pkt, recv_ptr, pkt_size); 
	if (type_hdr->type == ETH_APPEND_REQ) {
	    req_q.push(pkt);
	    {
	        std::unique_lock<std::mutex> lock(req_q_mutex);
            }
	    req_cv.notify_all();
	} else if (type_hdr->type == ETH_APPEND_RESP) {
 	    resp_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(resp_q_mutex);
            }
	    resp_cv.notify_all();
	}
    }
}

void udp_sequencer_request(std::shared_ptr<Network> net, 
		      std::string json_name, 
		      uint64_t thread_id, 
		      uint64_t batch_size, 
		      bool batch_on,
		      bool use_store,
		      std::vector<std::string> cli_ips,
		      std::vector<std::string> stor_ips,
		      std::string stor_receive_port,
		      uint64_t max_duration) {
    (void) json_name;
    (void) max_duration;
    (void) thread_id;
    (void) batch_size;
    (void) cli_ips;
    spdlog::critical("Network Sequencer Thread starting with TID = {}", gettid());
    //std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id, self_ip);
    spdlog::info("Simple Net Sequencer, about to start with {}!", !end_thread);
    spdlog::info("Simple Net Sequencer, stor_ip {}!", stor_ips[0]);
    spdlog::info("Batch on: {}!", batch_on);
    uint64_t pkt_req_cntr = 0;
    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();


    // Request header
    while (!end_thread) {
        bool got_quorum = false;

        while (!got_quorum) {
    	    // Stop receiving/sending messages since the experiment is over
            if (end_thread) {
                break;
            }

    	    // Wait to receive the packet 
    	    char* recv_ptr;
       	    {
               std::unique_lock<std::mutex> lock(req_q_mutex);
    	       req_cv.wait(lock, [] {return end_thread || !req_q.empty();});
    	       if (!req_q.try_pop(recv_ptr) || !recv_ptr) {
    	           if (use_store) {
    	               for (uint64_t i = 0; i < stor_ips.size(); i++) {
                           net->send_udp_packet(NULL, 0, stor_ips[i], stor_receive_port);
    	               }  
    	           }
                   continue;
               }
            }
    
    	    // Continue waiting for more packets if 1) recv_ptr is NULL and 2) max timeout hasn't been reached

    	    // Isolate the ethernet header from the receive ptr
             	    	
    	    // Get the correct header (ring append entry) from the received packet
            struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
            uint64_t pkt_size = size_of_type_hdr + size_of_hdr + append_entry->payload_size + 1;
        	
            // If the batch isn't full and the timeout not expired exceed
    	    // Create the packet to send to the storage server with the running packet size and the additional header
            std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);

    	    // Copy batch header into the reply packet, and the batch content 
            memcpy(reply_packet.get(), recv_ptr, pkt_size);
    	
    	    // Send the network packet
            bool res = false;
    	    if (use_store) {
    	        //spdlog::debug("Sending multiple packets!");
    	        for (uint64_t i = 0; i < stor_ips.size(); i++) {
	            res = net->send_udp_packet(std::move(reply_packet), pkt_size, stor_ips[i], stor_receive_port);
    	        }
    	    } else {
    	        //spdlog::debug("Only sending a single packet!");
    	        ((struct ring_type*)reply_packet.get())->type = ETH_APPEND_RESP;
    
    		// 2. Prepare a buffer for the string
    		// INET_ADDRSTRLEN is a standard constant (usually 16)
    		char buffer[INET_ADDRSTRLEN];
    
    		// 3. Convert the 4 bytes into a dotted-quad string
    		if (inet_ntop(AF_INET, &append_entry->client_ip, buffer, INET_ADDRSTRLEN) == nullptr) {
		     spdlog::critical("UH OH UNABLE TO GET DOTTED_QUAD STRING");
		     memset(buffer, 0, INET_ADDRSTRLEN);
    		}
		std::string client_ip(buffer);
                net->send_client_udp_packet(std::move(reply_packet), pkt_size, client_ip, std::to_string(append_entry->recv_port));
    	        res = true;
    	    }
    	    pkt_req_cntr += 1;
            if (!res) {
           	   continue;
    	    }
            got_quorum = true;
        }
    }
    spdlog::critical("Done here! Req cntr: {}", pkt_req_cntr); 
    // Get some statistics,

    //spdlog::debug("Stats results: Lat: {}, Tput: {}, Total Ops: {}", stat->getAvgLatency(), stat->getThroughput(max_duration), stat->getTotalOps());
}

void udp_sequencer_response(std::shared_ptr<Network> net, 
		      std::string json_name, 
		      uint64_t thread_id, 
		      uint64_t batch_size, 
		      bool batch_on,
		      bool use_store,
		      std::vector<std::string> cli_ips,
		      std::vector<std::string> stor_ips,
		      std::string stor_receive_port,
		      uint64_t max_duration) {
    (void) json_name;
    (void) max_duration;
    (void) thread_id;
    (void) batch_size;
    (void) use_store;
    (void) stor_receive_port;
    (void) cli_ips;
    spdlog::critical("Network Sequencer Thread starting with TID = {}", gettid());
    //std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id, self_ip);
    spdlog::info("Simple Net Sequencer, about to start with {}!", !end_thread);
    spdlog::info("Simple Net Sequencer, stor_ip {}!", stor_ips[0]);
    spdlog::info("Batch on: {}!", batch_on);
    uint64_t pkt_resp_cntr = 0;


    if (true) {
        // Request header
	while (!end_thread) {
            bool got_quorum = false;

	    // The packet size of the append request while waiting for the batch to fill up

            // Calculate the batch size for this type of packet
            size_t size_of_hdr = get_ring_append_size();
            size_t size_of_type_hdr = get_ring_type_size();

            //double lat_start_time = stat->getStartLat();
            while (!got_quorum) {
		 // Stop receiving/sending messages since the experiment is over
                 if (end_thread) {
                     break;
                 }
		
		// Wait to receive the packet 
		char* recv_ptr;
	    	{
	            std::unique_lock<std::mutex> lock(resp_q_mutex);
		    resp_cv.wait(lock, [] {return end_thread || !resp_q.empty();});
		    if (!resp_q.try_pop(recv_ptr) || !recv_ptr) {
	                continue;
	            }
	    	}

		// Continue waiting for more packets if 1) recv_ptr is NULL and 2) max timeout hasn't been reached

		// Isolate the ethernet header from the receive ptr
                    // Get the append entry header from the storage reply
		struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
		struct ring_type* append_type = (struct ring_append_entry*)(recv_ptr);
		//spdlog::debug("Number of entries received: {}", append_entry->num_entries);
		uint64_t num_entries = ntohs(append_type->num_entries);
		uint64_t recv_offset = 0;
		for (uint64_t i = 0; i < num_entries; i++) {
		    // Create reply buffer packet
 		    uint64_t reply_pkt_size = size_of_type_hdr + size_of_hdr;
     	            std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);

		    //spdlog::debug("WE ARE ON ITERATION: {} with offset {}", i, recv_offset);
		    struct ring_append_entry* batch_append_entry = (struct ring_append_entry*)(recv_ptr + recv_offset + size_of_type_hdr);
	            // 2. Prepare a buffer for the string
    		    // INET_ADDRSTRLEN is a standard constant (usually 16)
    		    char buffer[INET_ADDRSTRLEN];
    
    		    // 3. Convert the 4 bytes into a dotted-quad string
    		    if (inet_ntop(AF_INET, &batch_append_entry->client_ip, buffer, INET_ADDRSTRLEN) == nullptr) {
		         spdlog::critical("UH OH UNABLE TO GET DOTTED_QUAD STRING");
		         memset(buffer, 0, INET_ADDRSTRLEN);
    		    }
		    std::string client_ip(buffer);

		    struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
		    type_hdr->type = ETH_APPEND_RESP;

		    // Copy contents into the packet
                    memcpy(reply_packet.get(), recv_ptr + recv_offset, reply_pkt_size);
		    net->send_client_udp_packet(std::move(reply_packet), reply_pkt_size, client_ip, std::to_string(batch_append_entry->recv_port));

		    recv_offset += reply_pkt_size;
		    //spdlog::debug("Send packet response with size {}!", reply_pkt_size);
		}
		pkt_resp_cntr += num_entries;
                got_quorum = true;
            }

            // Create packet buffer which will be sent  
            if (end_thread) {
                break;
            }
        }
    } else {
        // Request header
    }
    spdlog::critical("Done here! Resp cntr: {}", pkt_resp_cntr); 
    // Get some statistics,

    //spdlog::debug("Stats results: Lat: {}, Tput: {}, Total Ops: {}", stat->getAvgLatency(), stat->getThroughput(max_duration), stat->getTotalOps());
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }

    spdlog::critical("Network Main Thread starting with TID = {}", gettid());
    std::string input_file = std::string(argv[1]);
    YAML::Node config = YAML::LoadFile(input_file);
    std::vector<int> eth_types = {ETH_APPEND_REQ};
    std::shared_ptr<Network> net = std::make_shared<Network>(std::to_string(get_send_port(config)), 
                                   get_switch_receive_port(config),
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
    spdlog::info("Simple Network server");
    std::string json_name = "dummy";
    uint64_t max_duration = get_experiment_duration(config);
    std::thread request_thread(udp_sequencer_request, net, json_name, 0, get_batch_size(config), get_batch_on(config), get_use_stor(config), get_cli_ip(config), get_stor_ips(config), get_stor_receive_port(config), max_duration);
    std::thread response_thread(udp_sequencer_response, net, json_name, 0, get_batch_size(config), get_batch_on(config), get_use_stor(config), get_cli_ip(config), get_stor_ips(config), get_stor_receive_port(config), max_duration);
    
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

    pthread_t request_handle = request_thread.native_handle();

    // Create a CPU set and add the desired core
    cpu_set_t req_cpuset;
    CPU_ZERO(&req_cpuset);
    CPU_SET(0, &req_cpuset); // Pin to core 'i'

    // Set thread affinity
    int req_result = pthread_setaffinity_np(request_handle, sizeof(cpu_set_t), &req_cpuset);
    if (req_result != 0) {
        std::cerr << "Error setting thread affinity for thread " << request_thread.get_id() << ": " << req_result << std::endl;
    } 
    
    pthread_t response_handle = response_thread.native_handle();

    // Create a CPU set and add the desired core
    cpu_set_t resp_cpuset;
    CPU_ZERO(&resp_cpuset);
    CPU_SET(1, &resp_cpuset); // Pin to core 'i'

    // Set thread affinity
    int result = pthread_setaffinity_np(response_handle, sizeof(cpu_set_t), &resp_cpuset);
    if (result != 0) {
        std::cerr << "Error setting thread affinity for thread " << response_thread.get_id() << ": " << result << std::endl;
    } 


    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
    req_cv.notify_all();
    request_thread.join();
    resp_cv.notify_all();
    response_thread.join();
    recv_thread.join();
    net->done();
    return 0;
}

