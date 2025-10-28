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
				   mac_addrs);
    set_spdlog_level(get_log_level(config));
    spdlog::info("Pringles Client: Only Append being tested");
    this->shard_id = get_shard_id(config);
    this->shard_switch_id = get_shard_switch_id(config);
    this->view_num = 1;
    this->max_duration = get_experiment_duration(config);
 
    this->stor = StorageType(get_storage_type(config));
    this->ssid = storage_id;

    this->recv_thread = std::thread(&LogStorage::pringles_recv_queue, this);
}

LogStorage::~LogStorage() {
    this->recv_thread.join();
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
    while (true) {
	if (end_thread) {
	    break;
	}
	std::unique_ptr<char[]> recv_ptr = net->read_from_recv_queue(); // will receive the full packet, including Eth header
	if (!recv_ptr) {
	    continue;
	}
        struct ethhdr* eth = (struct ethhdr*)recv_ptr.get();
	//spdlog::debug("The ethernet type is {}", ntohs(eth->h_proto));
	if (ntohs(eth->h_proto) == ETH_APPEND_REQ) {
            size_t hdr_size = get_size_of_hdr(PacketType::append);
	    if (hdr_size == 0) {
                spdlog::debug("Improper header type!! Packet being discarded");
            	continue;
            }
            spdlog::debug("Ethernet protocol with size {}", hdr_size);
	    //struct iphdr * ip = (struct iphdr *)(sample_pkt.get() + sizeof(struct ethhdr));
            struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr.get() + sizeof(struct ethhdr) + sizeof(struct iphdr));
            char* inner_pkt = (char*)(recv_ptr.get() + sizeof(struct ethhdr) + sizeof(struct iphdr) + get_size_of_hdr(PacketType::append));
	    std::string str(inner_pkt);
	    spdlog::debug("Payload size: {} and index {} and cid {} and nonce {}", append_entry->payload_size, append_entry->g_idx, append_entry->cid, append_entry->nonce);
	    if (append_entry->g_idx > 0) {
	        ringclient::Payload payload;
	        payload.ParseFromString(str);
	        store(append_entry->g_idx, payload.mutable_append()->entry());
		uint64_t size_of_hdr = get_size_of_hdr(PacketType::append);
     		std::unique_ptr<char[]> packet = std::make_unique<char[]>(size_of_hdr);
     		// Construct packet - only need the header this time
     		memcpy(packet.get(), reinterpret_cast<const char*>(append_entry), size_of_hdr);
	        net->add_to_send_queue(std::move(packet), static_cast<uint64_t>(PacketType::append), size_of_hdr);
	    } else {
	        spdlog::debug("Index is invalid! Not reply sent.");
	    }
	} else {
	    //spdlog::debug("No parsing support for this packet at this time!");
	}
    }
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
