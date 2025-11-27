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
		      uint64_t payload_size,
		      uint64_t max_duration) {
    (void) batch_size;
    (void) batch_on;
    (void) json_name;
    (void) thread_id;
    (void) max_duration;
    spdlog::critical("Network Sequencer Thread starting with TID = {}", gettid());
    //std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id);
    spdlog::info("Simple Net Sequencer, about to start with {}!", !end_thread);
    spdlog::info("Simple Net Sequencer, stor_ip {}!", stor_ip);
    spdlog::info("Batch on: {}!", batch_on);
	
    uint64_t nonce = 1;
    if (true) {
        // Request header
        std::unique_ptr<struct ring_append_entry> req_hdr = create_ring_append_entry(nonce, thread_id);
        req_hdr.get()->num_entries = 0;
        while (!end_thread) {
            bool got_quorum = false;

	    // The packet size of the append request while waiting for the batch to fill up

            // Calculate the batch size for this type of packet
            size_t size_of_hdr = get_ring_append_size();
            size_t size_of_type_hdr = get_ring_type_size();
            uint64_t pkt_size = size_of_type_hdr + size_of_hdr + payload_size + 1;

            auto start_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
            double start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(start_duration_since_epoch).count();

            //double lat_start_time = stat->getStartLat();
            while (!got_quorum) {
		 // Stop receiving/sending messages since the experiment is over
                 if (end_thread) {
                     break;
                 }

		// Wait to receive the packet 
                char* recv_ptr = net->recv_packet(); // TODO add epoll
		// Get the current elapsed duration
                auto end_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
                double end_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(end_duration_since_epoch).count();
                double dur = end_time_s - start_time_s;

		// Continue waiting for more packets if 1) recv_ptr is NULL and 2) max timeout hasn't been reached
                if (!recv_ptr && (dur < MAX_TIMEOUT)) {
                    continue;
                } else if (!recv_ptr) {
		    //spdlog::debug("Sequencer timed out!");
		    continue;
		}

		// Isolate the ethernet header from the receive ptr
                struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
		//spdlog::debug("I got the type: {}", type_hdr->type);

		// If the packet is of type ETH_APPEND_REQ
                if (type_hdr->type == ETH_APPEND_REQ) { // TODO
             	    	
		    // Get the correct header (ring append entry) from the received packet
            	    struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));

            	    // Copy new packet into the batch and update the running packet size for the append request batch

            	    // If the batch isn't full and the timeout not expired exceed
		    // Create the packet to send to the storage server with the running packet size and the additional header
            	    //spdlog::debug("Final request packet size: {}", append_req_running_pkt_size);
                    std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);

		    // Copy batch header into the reply packet, and the batch content 
                    memcpy(reply_packet.get(), recv_ptr, pkt_size);
		    // Send the network packet
		    //spdlog::debug("Send size: {}", pkt_size);
		    if (use_store) {
            	        net->send_udp_packet(std::move(reply_packet), pkt_size, 0, ETH_APPEND_REQ, stor_ip, stor_receive_port);
		    } else {
			((struct ring_type*)reply_packet.get())->type = ETH_APPEND_RESP;
			for (std::string cli_ip : cli_ips) {
            	            net->send_udp_packet(std::move(reply_packet), pkt_size, 0, ETH_APPEND_RESP, cli_ip, std::to_string(append_entry->recv_port));
			}
		    }

		    /*stat->getDuration(lat_start_time);
		    stat->addOp();*/
                    got_quorum = true;

            	    // Reset the timeout durations
       		    start_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
                    start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(start_duration_since_epoch).count();
                } else if (type_hdr->type == ETH_APPEND_RESP) { // send to client TODO 
		    //spdlog::debug("IN THE ETH RESPONDER!");
                    // Get the append entry header from the storage reply
		    struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
		    std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);

		    // Copy batch header into the reply packet, and the batch content 
                    memcpy(reply_packet.get(), recv_ptr, pkt_size);
		    // Send the network packet
		    //spdlog::debug("Send size: {} to IP {} at port {}", pkt_size, cli_ip, std::to_string(append_entry->recv_port));
            	    for (std::string cli_ip : cli_ips) {
		        net->send_udp_packet(std::move(reply_packet), pkt_size, 0, ETH_APPEND_RESP, cli_ip, std::to_string(append_entry->recv_port));
		    }

                    got_quorum = true;
                    start_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
                    start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(start_duration_since_epoch).count();
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

    spdlog::debug("Stats results:");
    /*stat->getAvgLatency();
    stat->getThroughput(max_duration);
    stat->getTotalOps();*/
    
}

void custom_sequencer(std::unique_ptr<Network> net, 
		      std::string json_name, 
		      uint64_t thread_id, 
		      uint64_t batch_size, 
		      bool batch_on,
		      std::array<uint8_t,6> cli_mac,
		      std::string cli_ip,
		      std::array<uint8_t,6> stor_mac,
		      std::string stor_ip,
		      uint64_t payload_size,
		      uint64_t max_duration) {
    (void) max_duration;
    (void) json_name;
    spdlog::critical("Network Sequencer Thread starting with TID = {}", gettid());
    //std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id);
    spdlog::info("Simple Net Sequencer, about to start with {}!", !end_thread);
    spdlog::info("Simple Net Sequencer, cli_ip {}!", cli_ip);
    spdlog::info("Simple Net Sequencer, stor_ip {}!", stor_ip);
    spdlog::info("Batch on: {}!", batch_on);


    in_addr_t stor_in_addr = inet_addr(stor_ip.c_str());
    in_addr_t cli_in_addr = inet_addr(cli_ip.c_str());
    uint64_t nonce = 1;

    if (true) {
        // Request header
        std::unique_ptr<struct ring_append_entry> req_hdr = create_ring_append_entry(nonce, thread_id);
        req_hdr.get()->num_entries = 0;
        while (!end_thread) {
            bool got_quorum = false;

	    // The packet size of the append request while waiting for the batch to fill up
            size_t append_req_running_pkt_size = 0;

            // Calculate the batch size for this type of packet
            size_t size_of_hdr = get_ring_append_size();
            uint64_t actual_batch_size = (size_of_hdr + payload_size + 1)*batch_size;
	    uint64_t batch_offset = 0;
	    uint64_t padding = 0; //size_of_hdr + payload_size;

            auto start_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
            double start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(start_duration_since_epoch).count();

	    // Buffer that stores each received packet while waiting for batch to fill up
            std::unique_ptr<char[]> batch_packet = std::make_unique<char[]>(actual_batch_size + padding);

            while (!got_quorum) {
		 // Stop receiving/sending messages since the experiment is over
                 if (end_thread) {
                     break;
                 }

		// Wait to receive the packet 
                char* recv_ptr = net->recv_packet(); // TODO add epoll

		// Get the current elapsed duration
                auto end_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
                double end_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(end_duration_since_epoch).count();
                double dur = end_time_s - start_time_s;

		// Continue waiting for more packets if 1) recv_ptr is NULL and 2) max timeout hasn't been reached
                if (!recv_ptr && (dur < MAX_TIMEOUT)) {
                    continue;
                }

		// Isolate the ethernet header from the receive ptr
                struct ethhdr* eth = (struct ethhdr*)recv_ptr;

		// If the packet is of type ETH_APPEND_REQ
                if (ntohs(eth->h_proto) == ETH_APPEND_REQ) {
            	
		    // Get the correct header (ring append entry) from the received packet
            	    struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr));
		    uint64_t intermediate_sz = append_entry->payload_size + 1 + size_of_hdr;

		    append_req_running_pkt_size += intermediate_sz;

            	    // Copy new packet into the batch and update the running packet size for the append request batch
		    spdlog::debug("BATCH PACKET INFORMATION: offset {}, Intermediate size: {}, Size of header: {}", batch_offset, intermediate_sz, size_of_hdr);
		    memcpy(batch_packet.get() + batch_offset, (char*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr)), intermediate_sz);

	            batch_offset += intermediate_sz;
            	    req_hdr.get()->num_entries += 1;
         	    
		    spdlog::debug("Request Append entry: {}, {}, tid: {}, num_entries: {}, Batch size: {}, Running pkt size: {}", append_entry->nonce, append_entry->payload_size, append_entry->thread_id, req_hdr.get()->num_entries, actual_batch_size, append_req_running_pkt_size);

            	    // If the batch isn't full and the timeout not expired exceed
            	    if(append_req_running_pkt_size < actual_batch_size && dur < MAX_TIMEOUT) {
			// Continue waiting to receive another packet
            	        continue;
            	    } else {
			// Create the packet to send to the storage server with the running packet size and the additional header
            	        spdlog::debug("Final request packet size: {}", append_req_running_pkt_size);
                        std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(size_of_hdr + append_req_running_pkt_size);

			// Copy batch header into the reply packet, and the batch content 
                    	memcpy(reply_packet.get(), reinterpret_cast<const char*>(req_hdr.get()), size_of_hdr);
                    	memcpy(reply_packet.get() + size_of_hdr, batch_packet.get(), append_req_running_pkt_size);
			//uint64_t sample_pkt = size_of_hdr + append_req_running_pkt_size;
            	        spdlog::debug("Final request num append entries: {}, Append payload size: {}, Batch size {}, Pkt: {}", req_hdr.get()->num_entries, append_entry->payload_size, batch_size, append_req_running_pkt_size, append_req_running_pkt_size);

			// Send the network packet
			spdlog::debug("Send size: {}", size_of_hdr + append_req_running_pkt_size);
            	        net->send_packet(std::move(reply_packet), size_of_hdr + append_req_running_pkt_size, 0, ETH_APPEND_REQ, stor_mac, stor_in_addr);

        		req_hdr.get()->num_entries = 0;
            	    }	
                    got_quorum = true;
            	    // Reset the timeout durations
       		    start_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
                    start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(start_duration_since_epoch).count();
                } else if (ntohs(eth->h_proto) == ETH_APPEND_RESP) { // send to client TODO 
                    // Get the append entry header from the storage reply
		    struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr));

		    //uint64_t next_pkt_size = resp_running_pkt_size + append_entry->payload_size + size_of_hdr;
            	    /*if (next_pkt_size < actual_batch_size  && 
			dur < MAX_TIMEOUT) {

			// Copy the header + packet into the batch_packet buffer
            	        memcpy(batch_packet.get(), (char*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr)), append_entry->payload_size + size_of_hdr);
            	        resp_running_pkt_size += append_entry->payload_size;
            	        resp_running_pkt_size += size_of_hdr;
            	        resp_hdr.get()->num_entries += 1;
         	                spdlog::debug("Response Append entry: {}, {}, tid: {}, num_entries: {}", append_entry->nonce, append_entry->payload_size, append_entry->thread_id, resp_hdr.get()->num_entries);
            	        continue;
            	    }*/
		    uint64_t size_of_reply_pkt = size_of_hdr + (1 + append_entry->payload_size + size_of_hdr) * append_entry->num_entries;
                    std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(size_of_reply_pkt);
            	    memcpy(reply_packet.get(), (char*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr)), size_of_reply_pkt);

            	    //spdlog::debug("ETH_APPEND_RESP: {}, Append nonce: {}, Append payload size: {}, Append Thread ID: {}", ETH_APPEND_RESP, append_entry->nonce, append_entry->payload_size, append_entry->thread_id);

            	    //spdlog::debug("Final respond num append entries: {}, Append payload size: {}, Batch size {}, Num entries: {}", append_entry->num_entries, append_entry->payload_size, size_of_hdr, append_entry->num_entries);
         	    
		    net->send_packet(std::move(reply_packet), size_of_reply_pkt, 0, ETH_APPEND_RESP, cli_mac, cli_in_addr);

                    got_quorum = true;
                    start_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
                    start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(start_duration_since_epoch).count();
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

    spdlog::debug("Stats results:");
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
    std::thread server_thread(custom_udp_sequencer, std::move(net), json_name, 0, get_batch_size(config), get_batch_on(config), get_use_stor(config), get_cli_ip(config), get_stor_ip(config), get_stor_receive_port(config), get_payload_size(config), max_duration);

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

