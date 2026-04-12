#include <chrono>
#include <thread>
#include <iostream>
#include <numeric>
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

    this->use_shards = get_use_shards(config);

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
				   get_multicast_addr(config),
				   false,
				   false); 

    this->max_duration = get_experiment_duration(config);
    // Initialize storage server identity variables
    this->switch_id = switch_id;
    this->use_store = get_use_stor(config);
    
    // Sharding information
    std::vector<std::vector<std::string>> yaml_shards = get_all_shards(config);
    for (uint64_t i = 0; i < yaml_shards.size(); i++) {
	tbb::concurrent_vector<std::string> shard;
        for (uint64_t j = 0; j < yaml_shards[i].size(); j++) {
	    shard.push_back(yaml_shards[i][j]);
	}
	this->all_shards.push_back(shard);
    } 
    /*std::vector<std::string> yaml_vec = get_all_shards(config);
    for (std::string entry : yaml_vec) {
        spdlog::critical("Shard multicast: {}", entry);
        this->all_shards.push_back(entry);
    }*/

    // Streaming information
    this->use_streams = get_use_streams(config);

    // Acks
    this->ack_threshold = get_ack_threshold(config);

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
    spdlog::critical("UNIVERSAL RECEIVER thread starting with tid = {}", gettid());
    //pin_current_thread_linux(0);
    while (!end_thread) {
        char* recv_ptr = net->recv_packet();
	if (!recv_ptr) {
	    continue;
	}
	spdlog::debug("Received something!!!");
	struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
	if (ntohs(type_hdr->type) == ETH_APPEND_RESP) {
	    spdlog::debug("Appending response being processed!");
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
	    spdlog::debug("Received read request!");
            struct ring_read_entry* ring = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
            char* pkt = (char*)std::malloc(type_hdr->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_read_entry) + ring->payload_size + 1));
            memcpy(pkt, recv_ptr, type_hdr->num_entries*(sizeof(struct ring_type) + sizeof(struct ring_read_entry) + ring->payload_size + 1)); 
	    read_req_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(read_req_q_mutex);
            }
	    read_req_cv.notify_all();
	} else if (ntohs(type_hdr->type) == ETH_SUBSCRIBE_ENTRY) {
	    spdlog::debug("Received subscribe entry!");
            char* pkt = (char*)std::malloc((sizeof(struct ring_type) + sizeof(struct ring_subscribe_entry)));
            memcpy(pkt, recv_ptr, (sizeof(struct ring_type) + sizeof(struct ring_subscribe_entry))); 
	    sub_q.push(pkt);
            {
	        std::unique_lock<std::mutex> lock(sub_q_mutex);
            }
	    sub_cv.notify_all();
	} else if (ntohs(type_hdr->type) == ETH_TAIL_REQ) {
	    spdlog::debug("Received tail request!");
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
    spdlog::critical("APPEND REQUEST Thread starting with TID = {}", gettid());
    pin_current_thread_linux(1);
    uint64_t pkt_req_cntr = 0;
    size_t size_of_hdr = get_ring_append_size();
    size_t size_of_type_hdr = get_ring_type_size();
    std::vector<double> lats;


    // Request header
    while (!end_thread) {
        bool got_quorum = false;
	auto duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
	double start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(duration_since_epoch).count();

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
                           net->send_udp_packet(NULL, 0, stor_ips[i], stor_receive_port, false);
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
	        /*if (use_streams) { // TODO do I need this?
		    spdlog::debug("Stream processing here now!");
	            uint32_t stream_id = ntohl(append_entry->stream_id);
	            tbb::concurrent_hash_map<uint64_t, tbb::concurrent_unordered_set<uint64_t>>::const_accessor acc;
		    
		    // If the stream isn't being tracked yet, add it to the store
		    if (!concurrent_stream_tracker.find(acc, stream_id)) {
		        spdlog::debug("Stream is being added to the tracker: {}", stream_id);
		        tbb::concurrent_hash_map<uint64_t, tbb::concurrent_unordered_set<uint64_t>>::accessor put_acc;
		        concurrent_stream_tracker.insert(put_acc, stream_id);
			put_acc.release();
		    }
	            	
	            tbb::concurrent_hash_map<uint64_t, tbb::concurrent_unordered_set<uint64_t>>::accessor update_set_acc;
  		    if (concurrent_stream_tracker.find(update_set_acc, stream_id)) { // Should find stream_id
			spdlog::debug("Concurrent stream tracker being updated");
			auto result = update_set_acc->second.insert(ntohl(append_entry->g_idx));
		        if (!result.second) {
		            spdlog::critical("Unable to add seq no {} in stream ID {} to the set!", ntohl(append_entry->g_idx), stream_id);
		        }	
		    }
		    update_set_acc.release();
	        }*/

		if (use_shards) {
		    std::string multicast_addr;
		    tbb::concurrent_vector<std::string> shard;
		    uint64_t key_id = 0;
		    if (use_streams) {
			key_id = ntohl(append_entry->stream_id);
			spdlog::debug("Stream, yes shard Key ID for which shard to send to: {}", key_id);
		        tbb::concurrent_hash_map<uint64_t, uint64_t>::const_accessor acc;
		        if (stream_id_to_shard_id.find(acc, key_id)) {
		            //multicast_addr = all_shards[acc->second];
		            shard = all_shards[acc->second];
		        } else {
                            //multicast_addr = all_shards[next_available_shard];
                            shard = all_shards[next_available_shard];
		            tbb::concurrent_hash_map<uint64_t, uint64_t>::accessor put_acc;
		            bool insert_succ = stream_id_to_shard_id.insert(put_acc, key_id);
			    if (insert_succ) {
			        put_acc->second = next_available_shard;
			    }
			    put_acc.release();
		            next_available_shard = (next_available_shard + 1) % all_shards.size();
		        }
			spdlog::debug("Stream multicast addr: {}", multicast_addr);
		    } else { // If streams aren't used, then sequence number will be used
		        key_id = ntohl(append_entry->g_idx) % all_shards.size(); // TODO bit shift?
			spdlog::debug("No stream, yes shard Key ID for which shard to send to: {}", key_id);
			tbb::concurrent_hash_map<uint64_t, uint64_t>::const_accessor acc;
		        if (seq_idx_to_shard_id.find(acc, key_id)) {
		            //multicast_addr = all_shards[acc->second];
		            shard = all_shards[acc->second];
		        } else {
                            //multicast_addr = all_shards[next_available_shard];
                            shard = all_shards[next_available_shard];
		            tbb::concurrent_hash_map<uint64_t, uint64_t>::accessor put_acc;
		            bool insert_succ = seq_idx_to_shard_id.insert(put_acc, key_id);
			    if (insert_succ) {
			        put_acc->second = next_available_shard;
			    }
			    put_acc.release();
		            next_available_shard = (next_available_shard + 1) % all_shards.size();
		        }
			spdlog::debug("No stream multicast addr: {}", multicast_addr);
		    }
		    std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);
            	    memcpy(reply_packet.get(), recv_ptr, pkt_size);
		    for (std::string ip : shard) {
			 spdlog::debug("Sending to IP: {}", ip);
			 std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);
            	    	 memcpy(reply_packet.get(), recv_ptr, pkt_size);
		         net->send_udp_packet(std::move(reply_packet), pkt_size, ip, stor_receive_port, false);
		    }
		} else {
    	             for (uint64_t i = 0; i < stor_ips.size(); i++) {
		         spdlog::debug("Req payload sz: {}, Nonce: {}, Port: {}, Pkt size: {}",  ntohl(append_entry->payload_size), ntohl(append_entry->nonce), ntohs(append_entry->recv_port), pkt_size);
            	         std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);
    	                 // Copy batch header into the reply packet, and the batch content 
            	         memcpy(reply_packet.get(), recv_ptr, pkt_size);
	                 res = net->send_udp_packet(std::move(reply_packet), pkt_size, stor_ips[i], stor_receive_port, false);
    	             }
		}
    	    } else {
                struct ring_type* append_type = (struct ring_type*)(recv_ptr + sizeof(struct ring_type));
		spdlog::debug("Final # entry: {}, Payload sz: {}, Nonce: {}, Port: {}, Pkt size: {}", ntohs(append_type->num_entries), ntohl(append_entry->payload_size), ntohl(append_entry->nonce), ntohs(append_entry->recv_port), pkt_size);
                std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);
    	        // Copy batch header into the reply packet, and the batch content 
                memcpy(reply_packet.get(), recv_ptr, pkt_size);
    	
    	        ((struct ring_type*)reply_packet.get())->type = htons(ETH_APPEND_RESP);
    
		std::string client_ip = get_quad_ip(append_entry->client_ip);

                net->send_client_udp_packet(std::move(reply_packet), pkt_size, client_ip, std::to_string(append_entry->recv_port));
    	        res = true;
    	    }
    	    pkt_req_cntr += 1;
            if (!res) {
           	   continue;
    	    }
            got_quorum = true;
	    
	    duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
	    double end_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(duration_since_epoch).count();
	    double dur = end_time_s - start_time_s;
	    lats.push_back(dur);
        }
    }
    double final_avg_latency = std::accumulate(lats.begin(), lats.end(), 0.0) / lats.size();
    final_avg_latency *= 1000;

    spdlog::critical("Done here! Req cntr: {} and Latencies: {}", pkt_req_cntr, final_avg_latency); 
    //spdlog::debug("Stats results: Lat: {}, Tput: {}, Total Ops: {}", stat->getAvgLatency(), stat->getThroughput(max_duration), stat->getTotalOps());
}

void LogSoftwareSwitch::append_response() {
    spdlog::critical("APPEND RESPONSE Network Sequencer Thread starting with TID = {}", gettid());
    pin_current_thread_linux(2);
    uint64_t pkt_resp_cntr = 0;

    while (!end_thread) {

	// The packet size of the append request while waiting for the batch to fill up

        // Calculate the batch size for this type of packet
        size_t size_of_hdr = get_ring_append_size();
        size_t size_of_type_hdr = get_ring_type_size();

        //double lat_start_time = stat->getStartLat();
	    
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
	for (uint64_t i = 0; i < num_entries; i++) { // TODO: If there's multiple entries, currently there is assumption that there are multiple type headers
	    // Create reply buffer packet
 	    struct ring_type* type_hdr = (struct ring_type*)recv_ptr;
	    struct ring_append_entry* batch_append_entry = (struct ring_append_entry*)(recv_ptr + recv_offset + size_of_type_hdr);
            uint64_t seq_no = ntohl(batch_append_entry->g_idx);

	    // Check the acknowledgements first
	    bool got_quorum = false;
            tbb::concurrent_hash_map<uint64_t, uint64_t>::accessor acc;
            if (ack_map.find(acc, seq_no)) {
                acc->second += 1;
	        if (acc->second >= ack_threshold) {
	            got_quorum = true;
	        }
		spdlog::debug("The current number of acks is {} for seq no {}", acc->second, seq_no);
            } else {
                bool succ = ack_map.insert(acc, seq_no);
                if (succ) {
                    acc->second = 1;
		    if (acc->second >= ack_threshold) {
	                got_quorum = true;
	            }
                }
		spdlog::debug("The current number of acks is {} for seq no {}", acc->second, seq_no);
            }
            acc.release();
	    if (!got_quorum) {
	        continue;
	    }
 
    	    // INET_ADDRSTRLEN is a standard constant (usually 16)
	    std::string client_ip = get_quad_ip(batch_append_entry->client_ip);
	    if (client_ip.length() == 0) {
		continue;
	    }

	    spdlog::debug("Sending append response with payload size: {}, and total {} entries, this is entry #{}. This is going to IP {} and port {}", ntohl(batch_append_entry->payload_size), num_entries, i, client_ip, ntohs(batch_append_entry->recv_port));
	    uint32_t stream_id = ntohl(batch_append_entry->stream_id);

	    // Copy contents into the packet
	    type_hdr->type = ntohs(ETH_APPEND_RESP);
	    uint64_t reply_pkt_size = size_of_type_hdr + size_of_hdr + ntohl(batch_append_entry->payload_size) + 1;
     	    std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);
            memcpy(reply_packet.get(), recv_ptr + recv_offset, reply_pkt_size);
	    net->send_client_udp_packet(std::move(reply_packet), reply_pkt_size, client_ip, std::to_string(ntohs(batch_append_entry->recv_port)));


	    // Send new entry to all subscribers
	    if (use_streams) {
		tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<std::vector<std::string>>>::const_accessor acc;
		if (stream_subscribe_stor.find(acc, stream_id)) {
		    send_subscriber_pkts(acc->second, acc->second.size(), reply_pkt_size + recv_offset, recv_ptr);
	        }  // TODO semantics of acc release?
	    } else {
		send_subscriber_pkts(subscribe_stor, subscribe_stor.size(), reply_pkt_size + recv_offset, recv_ptr);
	    }
            recv_offset += reply_pkt_size;
	    //spdlog::debug("Send packet response with size {}!", reply_pkt_size);
	}
        
	pkt_resp_cntr += num_entries;
    }
    spdlog::critical("Done here! Resp cntr: {}", pkt_resp_cntr); 
    // Get some statistics,

    //spdlog::debug("Stats results: Lat: {}, Tput: {}, Total Ops: {}", stat->getAvgLatency(), stat->getThroughput(max_duration), stat->getTotalOps());
}

void LogSoftwareSwitch::send_subscriber_pkts(tbb::concurrent_vector<std::vector<std::string>> sub_it, size_t num_subscribers, uint64_t reply_pkt_size, char* recv_ptr) {
    for (size_t i = 0; i < num_subscribers; i++) {
        std::unique_ptr<char[]> send_sub_packet = std::make_unique<char[]>(reply_pkt_size);
        memcpy(send_sub_packet.get(), recv_ptr, reply_pkt_size);
        ((struct ring_type*)(send_sub_packet.get()))->type = ntohs(ETH_SUBSCRIBE_ENTRY);
        const std::vector<std::string>& entry = sub_it[i];
        net->send_client_udp_packet(std::move(send_sub_packet), reply_pkt_size, entry[0], entry[1]);
    }
}

void LogSoftwareSwitch::read_request() { // TODO: Should check use_store ahead of time
    spdlog::critical("READ REQUEST Network Sequencer Thread starting with TID = {}", gettid());
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
    	           if (use_store) { // TODO what is this for?
    	               for (uint64_t i = 0; i < stor_ips.size(); i++) {
                           net->send_udp_packet(NULL, 0, stor_ips[i], stor_receive_port, false);
    	               }  
    	           }
                   continue;
               }
            }
   
            struct ring_read_entry* read_entry = (struct ring_read_entry*)(recv_ptr + sizeof(struct ring_type));
            uint64_t pkt_size = size_of_type_hdr + size_of_hdr + read_entry->payload_size + 1;

	    // If the batch isn't full and the timeout not expired exceed
    	    // Create the packet to send to the storage server with the running packet size and the additional header
            spdlog::debug("Final request packet payload sz: {}, Nonce: {}, Port: {}, Pkt size: {}", ntohl(read_entry->payload_size), ntohl(read_entry->nonce), ntohs(read_entry->recv_port), pkt_size);
            
	    if (ntohl(read_entry->g_idx) > max_idx) {
	        spdlog::critical("Index is too high! Read rejected.");
		std::string client_ip = get_quad_ip(read_entry->client_ip);
		std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);
    	        // Copy batch header into the reply packet, and the batch content 
                memcpy(reply_packet.get(), recv_ptr, pkt_size);
	        net->send_udp_packet(std::move(reply_packet), pkt_size, client_ip, std::to_string(read_entry->recv_port), false); // TODO: Error indicator to header?
		continue;
	    }

            tbb::concurrent_hash_map<uint64_t, uint64_t>::accessor acc;
	    if (!ack_map.find(acc, ntohl(read_entry->g_idx)) || (acc->second < ack_threshold)) {
	        spdlog::critical("Index is too high! Read rejected.");
		std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);
    	        // Copy batch header into the reply packet, and the batch content 
                memcpy(reply_packet.get(), recv_ptr, pkt_size);
	        net->send_udp_packet(std::move(reply_packet), pkt_size, switch_ip, switch_recv_port, false); // Recirculate
		continue;
	    }
        	
            bool res = false;
    	    if (use_store) {
		if (use_shards) {
                    std::string multicast_addr = "";
		    tbb::concurrent_vector<std::string> shard;
		    uint64_t key_id = 0;
		    if (use_streams) {
		        key_id = ntohl(read_entry->stream_id);
		        tbb::concurrent_hash_map<uint64_t, uint64_t>::const_accessor acc;
		        if (stream_id_to_shard_id.find(acc, key_id)) {
		            //multicast_addr = all_shards[acc->second];
		            shard = all_shards[acc->second];
		        }
		    } else {
		        key_id = ntohl(read_entry->g_idx) % all_shards.size();  
		        tbb::concurrent_hash_map<uint64_t, uint64_t>::const_accessor acc;
		        if (seq_idx_to_shard_id.find(acc, key_id)) {
		            //multicast_addr = all_shards[acc->second];
		            shard = all_shards[acc->second];
		        }
		    }
		    spdlog::debug("The multicast addr to send to is: {}", multicast_addr);
		    if (shard.size() == 0) {
		        spdlog::critical("Sequence number or Stream is not stored yet. Read rejected.");
		        std::string client_ip = get_quad_ip(read_entry->client_ip);
	   	 	std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);
    	   	 	// Copy batch header into the reply packet, and the batch content 
           	 	memcpy(reply_packet.get(), recv_ptr, pkt_size);
	                net->send_udp_packet(std::move(reply_packet), pkt_size, client_ip, std::to_string(read_entry->recv_port), false); // TODO: Error indicator to header?
		        continue;
		    }
		    for (std::string ip : shard) {
			std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);
    	   	 	// Copy batch header into the reply packet, and the batch content 
           	 	memcpy(reply_packet.get(), recv_ptr, pkt_size);
		        res = net->send_udp_packet(std::move(reply_packet), pkt_size, ip, stor_receive_port, false);
		    }
	        }  else {
		    // Send every read request to every storage server
    	            for (uint64_t i = 0; i < stor_ips.size(); i++) {
			std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(pkt_size);
    	   	 	// Copy batch header into the reply packet, and the batch content 
           	 	memcpy(reply_packet.get(), recv_ptr, pkt_size);
	                res = net->send_udp_packet(std::move(reply_packet), pkt_size, stor_ips[i], stor_receive_port, false);
    	            }
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
    spdlog::critical("READ RESPONSE Network Sequencer Thread starting with TID = {}", gettid());
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

		    uint32_t cli_ip_int = batch_read_entry->client_ip;
		    std::string client_ip = get_quad_ip(cli_ip_int);

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
	    
	    std::string client_ip = get_quad_ip(tail_entry->client_ip);
	    if (client_ip.length() == 0) {
		continue;
	    }
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
	uint32_t stream_id = ntohl(sub_entry->stream_id);	
	std::string client_ip = get_quad_ip(sub_entry->client_ip);
	if (client_ip.length() == 0) {
	    continue;
	}
	store_sub(client_ip, std::to_string(ntohs(sub_entry->recv_port)), stream_id);
        pkt_cntr += 1;
    }
    spdlog::critical("=================================== Done here! Req cntr: {} ====================================", pkt_cntr); 
}

bool LogSoftwareSwitch::store_sub(std::string client_ip, std::string recv_port, uint32_t stream_id) {
    bool succ = true;
    std::vector<std::string> entry = {client_ip, recv_port};
    if (use_streams) {
	tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<std::vector<std::string>>>::accessor acc;
	if (stream_subscribe_stor.find(acc, stream_id)) {
	    acc->second.push_back(entry);
	} else {
    	    succ = stream_subscribe_stor.insert(acc, stream_id);
    	    if (succ) {
                acc->second.push_back(entry);
    	    }
    	    acc.release();
	}
    } else {
        subscribe_stor.push_back(entry);
    }
    return succ;
}

bool LogSoftwareSwitch::recover_switch() {
    return false; // TODO
}

void LogSoftwareSwitch::change_view(uint64_t new_view_num) {
    view_num = new_view_num;
}

void LogSoftwareSwitch::wait_to_finish() {
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
}

std::string LogSoftwareSwitch::get_quad_ip(uint32_t ip_addr) {
    // INET_ADDRSTRLEN is a standard constant (usually 16)
    char buffer[INET_ADDRSTRLEN];
    
    // Convert the 4 bytes into a dotted-quad string
    if (inet_ntop(AF_INET, &ip_addr, buffer, INET_ADDRSTRLEN) == nullptr) {
         spdlog::critical("UH OH UNABLE TO GET DOTTED_QUAD STRING");
         return "";
    }	
    std::string client_ip(buffer);
    return client_ip;
}
