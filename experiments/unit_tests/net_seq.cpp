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

const uint64_t MAX_WAIT_TIME = 100;
std::string sequence_pkt_type = "sequencer";
std::string storage_pkt_type = "storage";
bool end_thread = false;

struct AppendInfo {
    uint64_t nonce;
    std::string entry;
};

struct ReturnInfo {
    uint64_t nonce;
};


void custom_sequencer(std::unique_ptr<Network> net, 
		      std::string json_name, 
		      uint64_t thread_id, 
		      uint64_t batch_size, 
		      bool batch_on,
		      std::array<uint8_t,6> cli_mac,
		      std::string cli_ip,
		      std::array<uint8_t,6> stor_mac,
		      std::string stor_ip) {
    spdlog::critical("Network Storage Thread starting with TID = {}", gettid());
    (void) json_name;
    (void) thread_id;
    (void) batch_size;
    (void) batch_on;
    //std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id);
    spdlog::info("Simple Net Sequencer, about to start with {}!", !end_thread);
    spdlog::info("Simple Net Sequencer, cli_ip {}!", cli_ip);
    spdlog::info("Simple Net Sequencer, stor_ip {}!", stor_ip);

    size_t size_of_hdr = get_ring_append_size();
    while (!end_thread) {
     	bool got_quorum = false;
        while (!got_quorum) {
	     if (end_thread) {
	         break;
	     }

            char* recv_ptr = net->recv_packet();
	    if (!recv_ptr) {
	        continue;
	    }

	    struct ethhdr* eth = (struct ethhdr*)recv_ptr;
   	    if (ntohs(eth->h_proto) == ETH_APPEND_REQ || ntohs(eth->h_proto) == ETH_APPEND_RESP) { // send to storage server
	        got_quorum = true;
		struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr));
	    	size_t reply_pkt_size = size_of_hdr + append_entry->payload_size + 1;
	    	size_t reply_pkt_offset = size_of_hdr;
     	        spdlog::debug("Append entry: {}, {}", append_entry->nonce, append_entry->payload_size);

		std::unique_ptr<char[]> reply_packet = std::make_unique<char[]>(reply_pkt_size);
            	memcpy(reply_packet.get(), reinterpret_cast<const char*>(append_entry), size_of_hdr);
     		reply_packet[reply_pkt_size - 1] = '\0';

		/*struct AppendInfo* append_info = reinterpret_cast<struct AppendInfo*>(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr) + sizeof(struct ring_append_entry));
		spdlog::debug("Nonce: {}, Entry: {}", append_info->nonce, append_info->entry);*/

		memcpy(reply_packet.get() + reply_pkt_offset, reinterpret_cast<const char*>(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr) + sizeof(struct ring_append_entry)), append_entry->payload_size);
		spdlog::debug("ETH_APPEND_REQ: {}, Append nonce: {}, Append payload size: {}", ETH_APPEND_REQ, append_entry->nonce, append_entry->payload_size);
     		if (ntohs(eth->h_proto) == ETH_APPEND_REQ) {
		    net->send_packet(std::move(reply_packet), reply_pkt_size, 0, ETH_APPEND_REQ, stor_mac, stor_ip);
		} else if (ntohs(eth->h_proto) == ETH_APPEND_RESP) {
     		    net->send_packet(std::move(reply_packet), reply_pkt_size, 0, ETH_APPEND_RESP, cli_mac, cli_ip);
		}

	    }

	}

	// Create packet buffer which will be sent  
	if (end_thread) {
	    break;
	}
    }
    net->done();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }

    spdlog::critical("Network Main Thread starting with TID = {}", gettid());
    std::string input_file = std::string(argv[1]);
    YAML::Node config = YAML::LoadFile(input_file);
    std::vector<int> eth_types = {ETH_APPEND_REQ};
    std::vector<std::array<uint8_t, 6>> mac_addrs = get_dst_mac_addrs(config);
    if (mac_addrs.size() < 1) {
        spdlog::critical("Unable to parse mac address!");
        throw;
    }

    std::unique_ptr<Network> net = std::make_unique<Network>(get_threads(config), 
                                   get_send_port(config), 
                                   get_recv_port(config),
				   get_socket_type(config),
                                   get_log_level(config),
				   get_batch_size(config),
				   get_batch_on(config),
				   get_interface(config),
				   get_self_ip(config),
				   get_packet_types(config),
				   eth_types,
				   mac_addrs,
				   1, false); 
    set_spdlog_level(get_log_level(config));
    spdlog::info("Simple Network server");
    std::string json_name = "dummy";
    std::thread server_thread(custom_sequencer, std::move(net), json_name, 0, get_batch_size(config), get_batch_on(config), get_cli_mac(config), get_cli_ip(config), get_stor_mac(config), get_stor_ip(config));

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

    uint64_t max_duration = get_experiment_duration(config) + 5;

    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;

    server_thread.join();
    return 0;
}

