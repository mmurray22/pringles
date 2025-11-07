#include "pringles_storage.h"
#include "spdlog/spdlog.h"
#include "ringclient.pb.h"
#include "ring_headers.h"
#include "yaml-cpp/yaml.h"
#include "utils.h"

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

LogStorage::LogStorage(std::string input_file, uint64_t storage_id) {
   YAML::Node config = YAML::LoadFile(input_file);
   num_pkt_types = get_num_pkt_types(config);
   std::vector<std::array<uint8_t, 6>> mac_addrs = get_dst_mac_addrs(config);
   if (mac_addrs.size() < 1) {
       spdlog::critical("Unable to parse mac address!");
       throw;
   }

   // Create network
   net = std::make_unique<Network>(get_threads(config), 
                                   get_send_port(config), 
                                   get_recv_port(config),
				   get_socket_type(config),
                                   get_log_level(config),
				   get_batch_size(config),
				   get_batch_on(config),
				   get_interface(config),
				   get_self_ip(config),
				   get_packet_types(config),
				   get_pkt_eth_types(),
				   mac_addrs,
				   1, false); // TODO Need to do something else here??? Storage server could be faster
    set_spdlog_level(get_log_level(config));
    spdlog::info("Pringles Client: Only Append being tested");
    this->shard_id = get_shard_id(config);
    this->shard_switch_id = get_shard_switch_id(config);
    this->view_num = 1;
    this->max_duration = get_experiment_duration(config);
 
    this->stor = StorageType(get_storage_type(config));
    this->ssid = storage_id;
   
    // TODO ERROR NOT THREAD SAFE FOR MULTIPLE THREADS 
    for (uint64_t i = 0; i < 1; i++) { // TODO TODO TODO THIS CANNOT BE A CONSTANT
        recv_threads.emplace_back(std::thread(&LogStorage::pringles_recv_queue, this));	

	pthread_t native_handle = recv_threads.back().native_handle();

        // Create a CPU set and add the desired core
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset); // Pin to core 'i'

        // Set thread affinity
        int result = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpuset);
        if (result != 0) {
            std::cerr << "Error setting thread affinity for thread " << recv_threads.back().get_id() << ": " << result << std::endl;
        } else {
            std::cout << "Thread " << recv_threads.back().get_id() << " pinned to core " << i << std::endl;
        } 
    }

    //this->recv_thread = std::thread(&LogStorage::pringles_recv_queue, this);
}

LogStorage::~LogStorage() {
    for (uint64_t i = 0; i < 1; i++) { // TODO TODO TODO THIS CANNOT BE A CONSTANT
        recv_threads[i].join();	
    }

    //this->recv_thread.join();
    net->done();
}

bool LogStorage::recover_storage_server() { // TODO
    return false;
}

bool LogStorage::store(uint64_t idx, std::string entry) {
    switch (stor)
    {
	case StorageType::MEM_KV: 
	{
	    std::unique_lock<std::mutex> lock(kv_store_lock);
	    kv_store.insert(std::pair<uint64_t, std::string>(idx, entry));
	    return true;
	};	    
	default:
	{
	    spdlog::critical("Storage type is not supported!");
	};
    }
    return false;
}

std::string LogStorage::get(uint64_t idx) {
    std::string default_str = "";
    switch (stor)
    {
	case StorageType::MEM_KV: 
	{
	    return kv_store[idx]; // TODO error checking
	};	    
	default:
	{
	    spdlog::critical("Storage type is not supported!");
	};
    }
    return default_str;
}

void LogStorage::change_view(uint64_t new_view_num) {
    view_num = new_view_num;
}

void LogStorage::pringles_recv_queue() {
    // initialize - for each packet type, receive queue
    spdlog::critical("Recv Storage Thread starting with TID = {}", gettid());
    uint64_t cnt = 0;
    uint64_t total_cnt = 0;
    size_t size_of_hdr = get_size_of_hdr(PacketType::append);
    while (true) {
	if (end_thread) {
	    break;
	}
        char* recv_ptr = net->recv_packet();
	//char* recv_ptr = net->read_from_recv_queue(); // will receive the full packet, including Eth header
	if (!recv_ptr) {
	    continue;
	}
        struct ethhdr* eth = (struct ethhdr*)recv_ptr;
	//spdlog::debug("The ethernet type is {}", ntohs(eth->h_proto));
	if (ntohs(eth->h_proto) == ETH_APPEND_REQ) {
	    total_cnt += 1;

            spdlog::debug("Ethernet protocol with size {}", size_of_hdr);
            struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr));
	    if (append_entry->g_idx == 0 && append_entry->num_entries == 0) {
	        spdlog::debug("INVALID PACKET WITH g_idx 0 or no entries - dropping");
    	        continue;		
	    } 
	    spdlog::debug("Payload size: {} and index {} and cid {} and nonce {} and num_entries {}", append_entry->payload_size, append_entry->g_idx, append_entry->cid, append_entry->nonce, append_entry->num_entries);
	    size_t offset = sizeof(struct ethhdr) + sizeof(struct iphdr) + size_of_hdr;
	    size_t reply_pkt_size = size_of_hdr + append_entry->payload_size * append_entry->num_entries;
	    size_t reply_pkt_offset = size_of_hdr;
     	    std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);
            memcpy(reply_packet.get(), reinterpret_cast<const char*>(append_entry), size_of_hdr);
	    for (size_t i = 0; i < append_entry->num_entries; i++) {
		char* inner_pkt = (char*)(recv_ptr + offset);
	        ringclient::Payload payload;
	    	payload.ParseFromArray(inner_pkt, append_entry->payload_size);
		uint64_t idx = append_entry->g_idx + i;
	        store(idx, payload.mutable_append()->entry());

		ringclient::Payload reply;
		std::string output;
     		reply.set_packet_type(static_cast<int>(PacketType::append));
     		reply.set_nonce(payload.nonce());
     		reply.SerializeToString(&output);
		memcpy(reply_packet.get() + reply_pkt_offset, output.data(), output.length());
		spdlog::debug("Sending packet back with nonce {} and reply nonce {} and index of this entry is {} and the entry length is {}", payload.nonce(), reply.nonce(), idx, payload.mutable_append()->entry().length());
		offset += append_entry->payload_size;
		reply_pkt_offset += append_entry->payload_size;
		cnt += 1;
	    }

	    //spdlog::debug("For nonce {}, we got idx {}, which got {} matching acks and {} acks overall", append_entry->nonce, append_entry->g_idx, min_matching_acks, append_ack_map[append_entry->nonce].first);
            net->send_packet(std::move(reply_packet), reply_pkt_size, static_cast<int>(PacketType::append), get_pkt_eth_types()[PacketType::append]);
	    //net->add_to_send_queue(std::move(reply_packet), static_cast<uint64_t>(PacketType::append), reply_pkt_size);
	}
    }
    spdlog::critical("RECEIVED {} append packets and REPLIED to {} append packets on storage server {}", total_cnt, cnt, gettid());
}

void LogStorage::wait_to_finish() {
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
}

/* Header functions */
std::vector<int> LogStorage::get_pkt_eth_types() {
    std::vector<int> eth_types = {};
    for(uint64_t i = 0; i < num_pkt_types; i++) {
        eth_types.push_back(get_eth_type(i));
    }
    return eth_types;
}

size_t LogStorage::get_size_of_hdr(uint64_t pkt_type) {
    if (PacketType(pkt_type) == PacketType::append) {
        return sizeof(struct ring_append_entry);
    }
    return 0;
}

int LogStorage::get_eth_type(uint64_t pkt_type) {
    if (pkt_type == PacketType::append) {
        return ETH_APPEND_REQ;
    }
    return -1; // no ethernet type found
}
