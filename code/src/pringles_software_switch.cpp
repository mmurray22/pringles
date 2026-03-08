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
#include "ring_headers.h"
#include "pringles_software_switch.h"

LogSoftwareSwitch::LogSoftwareSwitch(std::string input_file, uint64_t switch_id) {
    spdlog::critical("Pringles Software Switch is starting!");
    YAML::Node config = YAML::LoadFile(input_file);

    // Create network
    this->net = std::make_shared<Network>(std::to_string(get_send_port(config)), 
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

    // Initialize storage server identity variables
    this->switch_id = switch_id;
    this->use_store = get_use_stor(config);
    // TODO use_client?
    this->view_num = 1;
    set_spdlog_level(get_log_level(config));
    this->max_idx = 0;
    this->stor_ips = get_stor_ips(config);

    // Routing
    this->switch_mac = get_switch_mac(config);
    this->switch_ip = get_switch_ip(config);
    this->switch_recv_port = get_switch_receive_port(config);
    this->stor_receive_port = get_stor_receive_port(config);

    // Thread
    recv_thread = std::thread(&LogSoftwareSwitch::receiver, this);
    append_req_thread = std::thread(&LogSoftwareSwitch::append_request, this);
    append_resp_thread = std::thread(&LogSoftwareSwitch::append_response, this);
    read_req_thread = std::thread(&LogSoftwareSwitch::read_request, this);
    read_resp_thread = std::thread(&LogSoftwareSwitch::read_response, this);
    tail_req_thread = std::thread(&LogSoftwareSwitch::tail_request, this);
    sub_thread = std::thread(&LogSoftwareSwitch::subscribe_request, this);
}

LogSoftwareSwitch::~LogSoftwareSwitch() {
    append_req_cv.notify_all();
    append_resp_cv.notify_all();
    read_req_cv.notify_all();
    read_resp_cv.notify_all();
    tail_req_cv.notify_all();
    sub_cv.notify_all();

    recv_thread.join();
    append_req_thread.join();
    append_resp_thread.join();
    read_req_thread.join();
    read_resp_thread.join();
    tail_req_thread.join();
    sub_thread.join();

    net->done();
}

void LogSoftwareSwitch::receiver() {
    spdlog::critical("network recv thread starting with tid = {}", gettid());
    while (!end_thread) {
        char* recv_ptr = net->recv_packet();
	if (!recv_ptr) {
	    continue;
	}
	struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
	if (ntohs(type_hdr->type) == ETH_APPEND_RESP) {
            struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
	    uint64_t pkt_size = ntohs(type_hdr->num_entries)*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ntohl(ring->payload_size) + 1);
            char* pkt = (char*)std::malloc(pkt_size);
            memcpy(pkt, recv_ptr, pkt_size); 
	    append_resp_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(append_resp_q_mutex);
            }
	    append_resp_cv.notify_all();
	} else if (ntohs(type_hdr->type) == ETH_APPEND_REQ) {
	    spdlog::debug("Appending request being processed!");
            struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ring_type));
	    spdlog::debug("Appending request with payload size: {} and nonce {} and number of entries {}", ntohl(ring->payload_size), ntohl(ring->nonce), ntohs(type_hdr->num_entries));
            char* pkt = (char*)std::malloc(ntohs(type_hdr->num_entries)*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ntohl(ring->payload_size) + 1));
            memcpy(pkt, recv_ptr, ntohs(type_hdr->num_entries)*(sizeof(struct ring_type) + sizeof(struct ring_append_entry) + ntohl(ring->payload_size) + 1)); 
	    append_req_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(append_req_q_mutex);
            }
	    append_req_cv.notify_all();
	} else if (ntohs(type_hdr->type) == ETH_READ_RESP) {
	    spdlog::debug("Received read response!");
            struct ring_read_entry* ring = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
            char* pkt = (char*)std::malloc(type_hdr->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_read_entry) + ntohl(ring->payload_size)+ 1));
            memcpy(pkt, recv_ptr, type_hdr->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_read_entry) + ntohl(ring->payload_size)+ 1)); 
	    read_resp_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(read_resp_q_mutex);
            }
	    read_resp_cv.notify_all();
	} else if (ntohs(type_hdr->type) == ETH_READ_REQ) {
            struct ring_read_entry* ring = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
            char* pkt = (char*)std::malloc(type_hdr->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_read_entry) + ring->payload_size + 1));
            memcpy(pkt, recv_ptr, type_hdr->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_read_entry) + ring->payload_size + 1)); 
	    read_req_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(read_req_q_mutex);
            }
	    read_req_cv.notify_all();
	} else if (ntohs(type_hdr->type) == ETH_SUBSCRIBE_ENTRY) {
            char* pkt = (char*)std::malloc((sizeof(struct ring_type) + sizeof(struct ring_subscribe_entry)));
            memcpy(pkt, recv_ptr, (sizeof(struct ring_type) + sizeof(struct ring_subscribe_entry))); 
	    sub_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(sub_q_mutex);
            }
	    sub_cv.notify_all();
	} else if (ntohs(type_hdr->type) == ETH_TAIL_REQ) {
            char* pkt = (char*)std::malloc((sizeof(struct ring_type) + sizeof(struct ring_tail_req)));
            memcpy(pkt, recv_ptr, (sizeof(struct ring_type) + sizeof(struct ring_tail_req))); 
	    tail_req_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(tail_req_q_mutex);
            }
	    tail_req_cv.notify_all();
	} else {
	    spdlog::debug("TYPE UNKNOWN: {}!!!", type_hdr->type);
	    continue;
	}
    }
}

void LogSoftwareSwitch::append_request() {
    spdlog::critical("Switch: APPEND REQUEST Thread starting with TID = {}", gettid());
    spdlog::info("Simple Net Sequencer, stor_ip {}!", stor_ips[0]);
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
               std::unique_lock<std::mutex> lock(append_req_q_mutex);
    	       append_req_cv.wait(lock, [this] {return end_thread || !append_req_q.empty();});
    	       if (!append_req_q.try_pop(recv_ptr) || !recv_ptr) {
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
	    max_idx += 1;
	    append_entry->g_idx = htonl(max_idx);
	    spdlog::debug("Global sequence number is now: {}", ntohl(append_entry->g_idx));
            uint64_t pkt_size = size_of_type_hdr + size_of_hdr + ntohl(append_entry->payload_size) + 1;
        	
            // If the batch isn't full and the timeout not expired exceed
    	    // Create the packet to send to the storage server with the running packet size and the additional header
                	    // Send the network packet
            bool res = false;
    	    if (use_store) {
    	        //spdlog::debug("Sending multiple packets!");
    	        for (uint64_t i = 0; i < stor_ips.size(); i++) {
		    spdlog::debug("Final request packet payload sz: {}, Nonce: {}, Port: {}, Pkt size: {}",  ntohl(append_entry->payload_size), ntohl(append_entry->nonce), ntohs(append_entry->recv_port), pkt_size);
            	    std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);
    	            // Copy batch header into the reply packet, and the batch content 
            	    memcpy(reply_packet.get(), recv_ptr, pkt_size);
	            res = net->send_udp_packet(std::move(reply_packet), pkt_size, stor_ips[i], stor_receive_port);
    	        }
    	    } else {
                struct ring_type* append_type = (struct ring_type*)(recv_ptr + sizeof(struct ring_type));
		spdlog::debug("Final request packet number of entries: {}, Payload sz: {}, Nonce: {}, Port: {}, Pkt size: {}", ntohs(append_type->num_entries), ntohl(append_entry->payload_size), ntohl(append_entry->nonce), ntohs(append_entry->recv_port), pkt_size);
                std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);
    	        // Copy batch header into the reply packet, and the batch content 
                memcpy(reply_packet.get(), recv_ptr, pkt_size);
    	
    	        //spdlog::debug("Only sending a single packet!");
    	        ((struct ring_type*)reply_packet.get())->type = htons(ETH_APPEND_RESP);
    
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
    //spdlog::debug("Stats results: Lat: {}, Tput: {}, Total Ops: {}", stat->getAvgLatency(), stat->getThroughput(max_duration), stat->getTotalOps());
}

void LogSoftwareSwitch::append_response() {
    spdlog::critical("Network Sequencer Thread starting with TID = {}", gettid());
    spdlog::info("Simple Net Sequencer, about to start with {}!", !end_thread);
    spdlog::info("Simple Net Sequencer, stor_ip {}!", stor_ips[0]);
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
	            std::unique_lock<std::mutex> lock(append_resp_q_mutex);
		    append_resp_cv.wait(lock, [this] {return end_thread || !append_resp_q.empty();});
		    if (!append_resp_q.try_pop(recv_ptr) || !recv_ptr) {
	                continue;
	            }
	    	}

		// Continue waiting for more packets if 1) recv_ptr is NULL and 2) max timeout hasn't been reached

		// Isolate the ethernet header from the receive ptr
                    // Get the append entry header from the storage reply
		struct ring_type* append_type = (struct ring_type*)(recv_ptr);
		uint64_t num_entries = ntohs(append_type->num_entries);
		uint64_t recv_offset = 0;
		for (uint64_t i = 0; i < num_entries; i++) {
		    // Create reply buffer packet
 		    struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
		    type_hdr->type = ntohs(ETH_APPEND_RESP);

		    struct ring_append_entry* batch_append_entry = (struct ring_append_entry*)(recv_ptr + recv_offset + size_of_type_hdr);
		    
		    uint64_t reply_pkt_size = size_of_type_hdr + size_of_hdr + ntohl(batch_append_entry->payload_size) + 1;
     	            std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);


	            // 2. Prepare a buffer for the string
    		    // INET_ADDRSTRLEN is a standard constant (usually 16)
    		    char buffer[INET_ADDRSTRLEN];
    
    		    // 3. Convert the 4 bytes into a dotted-quad string
		    spdlog::debug("Client ip: {}", batch_append_entry->client_ip);
    		    if (inet_ntop(AF_INET, &batch_append_entry->client_ip, buffer, INET_ADDRSTRLEN) == nullptr) {
		         spdlog::critical("UH OH UNABLE TO GET DOTTED_QUAD STRING");
		         memset(buffer, 0, INET_ADDRSTRLEN);
    		    }
		    std::string client_ip(buffer);

		    spdlog::debug("Sending append response with payload size: {}, and total {} entries, this is entry #{}. This is going to IP {} and port {}", ntohl(batch_append_entry->payload_size), num_entries, i, client_ip, ntohs(batch_append_entry->recv_port));

		    // Copy contents into the packet
                    memcpy(reply_packet.get(), recv_ptr + recv_offset, reply_pkt_size);
		    net->send_client_udp_packet(std::move(reply_packet), reply_pkt_size, client_ip, std::to_string(ntohs(batch_append_entry->recv_port)));


		    size_t current_size = subscribe_stor.size();
                    for (size_t i = 0; i < current_size; i++) {
     	                std::unique_ptr<char[]> send_sub_packet = std::make_unique<char[]>(reply_pkt_size);
                        memcpy(send_sub_packet.get(), recv_ptr + recv_offset, reply_pkt_size);
			((struct ring_type*)(send_sub_packet.get()))->type = ntohs(ETH_SUBSCRIBE_ENTRY);
                        const std::vector<std::string>& entry = subscribe_stor[i];
		        net->send_client_udp_packet(std::move(send_sub_packet), reply_pkt_size, entry[0], entry[1]);
                    }
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

void LogSoftwareSwitch::read_request() {
    spdlog::critical("Network Sequencer Thread starting with TID = {}", gettid());
    spdlog::info("Simple Net Sequencer, about to start with {}!", !end_thread);
    spdlog::info("Simple Net Sequencer, stor_ip {}!", stor_ips[0]);
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
               std::unique_lock<std::mutex> lock(read_req_q_mutex);
    	       read_req_cv.wait(lock, [this] {return end_thread || !read_req_q.empty();});
    	       if (!read_req_q.try_pop(recv_ptr) || !recv_ptr) {
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
            spdlog::debug("Final request packet payload sz: {}, Nonce: {}, Port: {}, Pkt size: {}", ntohl(append_entry->payload_size), ntohl(append_entry->nonce), ntohs(append_entry->recv_port), pkt_size);
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

void LogSoftwareSwitch::read_response() {
    spdlog::critical("Network Sequencer Thread starting with TID = {}", gettid());
    spdlog::info("Simple Net Sequencer, about to start with {}!", !end_thread);
    spdlog::info("Simple Net Sequencer, stor_ip {}!", stor_ips[0]);
    uint64_t pkt_resp_cntr = 0;


    if (true) {
        // Request header
	while (!end_thread) {
            bool got_quorum = false;

	    // The packet size of the append request while waiting for the batch to fill up

            // Calculate the batch size for this type of packet
            size_t size_of_hdr = get_ring_read_size();
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
	            std::unique_lock<std::mutex> lock(read_resp_q_mutex);
		    read_resp_cv.wait(lock, [this] {return end_thread || !read_resp_q.empty();});
		    if (!read_resp_q.try_pop(recv_ptr) || !recv_ptr) {
	                continue;
	            }
	    	}
		spdlog::debug("Responding to the read!");

		// Continue waiting for more packets if 1) recv_ptr is NULL and 2) max timeout hasn't been reached

		// Isolate the ethernet header from the receive ptr
                    // Get the append entry header from the storage reply
		struct ring_type* read_type = (struct ring_type*)(recv_ptr);
		uint64_t num_entries = ntohs(read_type->num_entries);
		uint64_t recv_offset = 0;
		spdlog::debug("Going to send responses now qith num entries {}!", num_entries);
		for (uint64_t i = 0; i < num_entries; i++) {
		    // Create reply buffer packet
		    struct ring_read_entry* batch_read_entry = (struct ring_read_entry*)(recv_ptr + recv_offset + size_of_type_hdr);
		    uint64_t reply_pkt_size = size_of_type_hdr + size_of_hdr + ntohl(batch_read_entry->payload_size) + 1;
     	            std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);


	            // 2. Prepare a buffer for the string
    		    // INET_ADDRSTRLEN is a standard constant (usually 16)
    		    char buffer[INET_ADDRSTRLEN];
    
    		    // 3. Convert the 4 bytes into a dotted-quad string
    		    if (inet_ntop(AF_INET, &batch_read_entry->client_ip, buffer, INET_ADDRSTRLEN) == nullptr) {
		         spdlog::critical("UH OH UNABLE TO GET DOTTED_QUAD STRING");
		         memset(buffer, 0, INET_ADDRSTRLEN);
    		    }
		    std::string client_ip(buffer);

		    struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
		    type_hdr->type = ntohs(ETH_READ_RESP);
		    spdlog::debug("Sending read response with payload size: {}, with total {} entries, this is entry #{}. This is going to IP {} and port {}",  ntohl(batch_read_entry->payload_size), num_entries, i, client_ip, ntohs(batch_read_entry->recv_port));

		    // Copy contents into the packet
                    memcpy(reply_packet.get(), recv_ptr + recv_offset, reply_pkt_size);
		    net->send_client_udp_packet(std::move(reply_packet), reply_pkt_size, client_ip, std::to_string(ntohs(batch_read_entry->recv_port)));

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

void LogSoftwareSwitch::tail_request() {
    uint64_t pkt_req_cntr = 0;
    size_t size_of_hdr = get_ring_tail_size();
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
               std::unique_lock<std::mutex> lock(tail_req_q_mutex);
    	       tail_req_cv.wait(lock, [this] {return end_thread || !tail_req_q.empty();});
    	       if (!tail_req_q.try_pop(recv_ptr) || !recv_ptr) {
                   continue;
               }
            }
    
    	    // Continue waiting for more packets if 1) recv_ptr is NULL and 2) max timeout hasn't been reached

    	    // Isolate the ethernet header from the receive ptr
             	    	
    	    // Get the correct header (ring append entry) from the received packet

            struct ring_tail_req* tail_entry = (struct ring_tail_req*)(recv_ptr + sizeof(struct ring_type));
	    tail_entry->tail_seq_no = htonl(max_idx);
    	    ((struct ring_type*)recv_ptr)->type = htons(ETH_TAIL_RESP);
            uint64_t pkt_size = size_of_type_hdr + size_of_hdr;
	    spdlog::debug("Answering a get tail request with sequence number: {}!", ntohl(tail_entry->tail_seq_no));
            // If the batch isn't full and the timeout not expired exceed
    	    // Create the packet to send to the storage server with the running packet size and the additional header
            std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);

    	    // Copy batch header into the reply packet, and the batch content 
            memcpy(reply_packet.get(), recv_ptr, pkt_size);

            char buffer[INET_ADDRSTRLEN];
    	    if (inet_ntop(AF_INET, &tail_entry->client_ip, buffer, INET_ADDRSTRLEN) == nullptr) {
	         spdlog::critical("UH OH UNABLE TO GET DOTTED_QUAD STRING");
	         memset(buffer, 0, INET_ADDRSTRLEN);
    	    }
	    std::string client_ip(buffer);

            net->send_client_udp_packet(std::move(reply_packet), pkt_size, client_ip, std::to_string(ntohs(tail_entry->recv_port)));
        }
    }
    spdlog::critical("Done here! Req cntr: {}", pkt_req_cntr); 
    // Get some statistics,

    //spdlog::debug("Stats results: Lat: {}, Tput: {}, Total Ops: {}", stat->getAvgLatency(), stat->getThroughput(max_duration), stat->getTotalOps());
}

void LogSoftwareSwitch::subscribe_request() {
    uint64_t pkt_cntr = 0;

    // Request header
    while (!end_thread) {
        // Wait to receive the packet 
        char* recv_ptr;
        {
           std::unique_lock<std::mutex> lock(sub_q_mutex);
           sub_cv.wait(lock, [this] {return end_thread || !sub_q.empty();});

           if (!sub_q.try_pop(recv_ptr) || !recv_ptr) {
               continue;
           }
        }
    
        struct ring_subscribe_entry* sub_entry = (struct ring_subscribe_entry*)(recv_ptr + sizeof(struct ring_type));
	
	char buffer[INET_ADDRSTRLEN];
    	if (inet_ntop(AF_INET, &sub_entry->client_ip, buffer, INET_ADDRSTRLEN) == nullptr) {
	     spdlog::critical("UH OH UNABLE TO GET DOTTED_QUAD STRING");
	     memset(buffer, 0, INET_ADDRSTRLEN);
    	}
	std::string client_ip(buffer);
	store_sub(client_ip, std::to_string(ntohs(sub_entry->recv_port)));
        pkt_cntr += 1;
    }
    spdlog::critical("Done here! Req cntr: {}", pkt_cntr); 
}

bool LogSoftwareSwitch::store_sub(std::string client_ip, std::string recv_port) {
    std::vector<std::string> entry = {client_ip, recv_port};
    subscribe_stor.push_back(entry);
    return true;
}

bool LogSoftwareSwitch::recover_switch() {
    return false; // TODO
}

void LogSoftwareSwitch::change_view(uint64_t new_view_num) {
    view_num = new_view_num;
}

void LogSoftwareSwitch::wait_to_finish() {
    while (true) {
        std::chrono::seconds sleep_duration(15);
    }
}
