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

void custom_udp_sequencer(std::unique_ptr<Network> net, 
		      std::string json_name, 
		      uint64_t thread_id, 
		      uint64_t batch_size, 
		      bool batch_on,
		      bool use_store,
		      std::vector<std::string> cli_ips,
		      std::string stor_ip,
		      std::string stor_receive_port,
		      uint64_t max_duration) {
    (void) json_name;
    (void) max_duration;
    (void) thread_id;
    (void) batch_size;
    spdlog::critical("Network Sequencer Thread starting with TID = {}", gettid());
    //std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id, self_ip);
    spdlog::info("Simple Net Sequencer, about to start with {}!", !end_thread);
    spdlog::info("Simple Net Sequencer, stor_ip {}!", stor_ip);
    spdlog::info("Batch on: {}!", batch_on);
	
    if (true) {
        // Request header
        uint64_t num_entries = 1;

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
                char* recv_ptr = net->recv_packet();
		
		// Continue waiting for more packets if 1) recv_ptr is NULL and 2) max timeout hasn't been reached
                if (!recv_ptr) {
	            //spdlog::critical("TIMEOUT SEND THE BATCH");
		    if (use_store) {
                        net->send_udp_packet(NULL, 0, 0, ETH_APPEND_REQ, stor_ip, stor_receive_port);
		    }
		    //stat->getDuration(lat_start_time);
		    //stat->addOp();
                    continue;
                }

		// Isolate the ethernet header from the receive ptr
                struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
	        //spdlog::debug("I got the type: {}", type_hdr->type);

		// If the packet is of type ETH_APPEND_REQ
                if (type_hdr->type == ETH_APPEND_REQ) {
             	    	
		    // Get the correct header (ring append entry) from the received packet
            	    struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
                    uint64_t pkt_size = size_of_type_hdr + size_of_hdr + append_entry->payload_size + 1;
            	    
            	    // If the batch isn't full and the timeout not expired exceed
		    // Create the packet to send to the storage server with the running packet size and the additional header
            	    spdlog::debug("Final request packet number of entries: {}, Payload sz: {}, Nonce: {}, Port: {}, Pkt size: {}", append_entry->num_entries, append_entry->payload_size, append_entry->nonce, append_entry->recv_port, pkt_size);
                    std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);

		    // Copy batch header into the reply packet, and the batch content 
                    memcpy(reply_packet.get(), recv_ptr, pkt_size);
		    
		    // Send the network packet
		    //spdlog::debug("Send size: {}", pkt_size);
                    bool res = false;
		    if (use_store) {
			//spdlog::debug("Sending multiple packets!");
            	        res = net->send_udp_packet(std::move(reply_packet), pkt_size, 0, ETH_APPEND_REQ, stor_ip, stor_receive_port);
		    } else {
			//spdlog::debug("Only sending a single packet!");
			((struct ring_type*)reply_packet.get())->type = ETH_APPEND_RESP;
            	        net->send_client_udp_packet(std::move(reply_packet), pkt_size, 0, ETH_APPEND_RESP, cli_ips[append_entry->cli_idx], std::to_string(append_entry->recv_port));
			res = true;
		    }

	       	   if (!res) {
	       	       num_entries += 1;
	       	       continue;
	       	   } else {
	       	      num_entries = 1;
	       	   }

		    //stat->getDuration(lat_start_time);
		    //stat->addOp();
                    got_quorum = true;

            	    // Reset the timeout durations
                } else if (type_hdr->type == ETH_APPEND_RESP) { // send to client TODO 
                    // Get the append entry header from the storage reply
		    struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
		    //spdlog::debug("Number of entries received: {}", append_entry->num_entries);
		    uint64_t num_entries = append_entry->num_entries;
		    uint64_t recv_offset = 0;
		    for (uint64_t i = 0; i < num_entries; i++) {
			// Create reply buffer packet
 		        uint64_t reply_pkt_size = size_of_type_hdr + size_of_hdr;
     	                std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);

		        //spdlog::debug("WE ARE ON ITERATION: {} with offset {}", i, recv_offset);
		        struct ring_append_entry* batch_append_entry = (struct ring_append_entry*)(recv_ptr + recv_offset + size_of_type_hdr);
	    
		        type_hdr->type = ETH_APPEND_RESP;
		        spdlog::debug("Sending append response with payload size: {}, and out of {} number of entries and total {} entries, this is entry #{}. This is going to cli ID {} at IP {} and port {}", batch_append_entry->payload_size, batch_append_entry->num_entries, num_entries, i, batch_append_entry->cli_idx, cli_ips[batch_append_entry->cli_idx], batch_append_entry->recv_port); // TODO next steps: Batch isn't being created properly

			// Copy contents into the packet
                        memcpy(reply_packet.get(), recv_ptr + recv_offset, reply_pkt_size);
		        net->send_client_udp_packet(std::move(reply_packet), reply_pkt_size, 0, ETH_APPEND_RESP, cli_ips[batch_append_entry->cli_idx], std::to_string(batch_append_entry->recv_port));

		        recv_offset += reply_pkt_size;
		        //spdlog::debug("Send packet response with size {}!", reply_pkt_size);
		    }

		    /*std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);

		    // Copy batch header into the reply packet, and the batch content 
                    memcpy(reply_packet.get(), recv_ptr, pkt_size);
		    // Send the network packet
		    //spdlog::debug("Send size: {} to IP {} at port {}", pkt_size, cli_ip, std::to_string(append_entry->recv_port));
            	    for (std::string cli_ip : cli_ips) {
		        net->send_udp_packet(std::move(reply_packet), pkt_size, 0, ETH_APPEND_RESP, cli_ip, std::to_string(append_entry->recv_port));
		    }*/

                    got_quorum = true;
                }
            }

            // Create packet buffer which will be sent  
            if (end_thread) {
                break;
            }
        }
    } else {
        // Request header
    }
    spdlog::debug("Done here!"); 
    // Get some statistics
    net->done();

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
    std::unique_ptr<Network> net = std::make_unique<Network>(std::to_string(get_send_port(config)), 
                                   get_switch_receive_port(config),
				   get_socket_type(config),
                                   get_log_level(config),
				   get_batch_size(config),
				   get_batch_on(config),
				   get_interface(config),
				   get_self_ip(config),
				   get_num_pkt_types(config),
				   false); 
    set_spdlog_level(get_log_level(config));
    spdlog::info("Simple Network server");
    std::string json_name = "dummy";
    uint64_t max_duration = get_experiment_duration(config);
    std::thread server_thread(custom_udp_sequencer, std::move(net), json_name, 0, get_batch_size(config), get_batch_on(config), get_use_stor(config), get_cli_ip(config), get_stor_ip(config), get_stor_receive_port(config), max_duration);

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



    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;

    server_thread.join();
    return 0;
}

