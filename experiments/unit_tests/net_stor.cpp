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

#define ETH_APPEND_REQ 0x0860

const uint64_t MAX_WAIT_TIME = 100;
std::string sequence_pkt_type = "sequencer";
std::string storage_pkt_type = "storage";
bool end_thread = false;

void custom_server(std::unique_ptr<Network> net, std::string json_name, uint64_t thread_id, uint64_t batch_size, bool batch_on) {
    spdlog::critical("Network Storage Thread starting with TID = {}", gettid());
    (void) json_name;
    (void) thread_id;
    (void) batch_size;
    (void) batch_on;
    //std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id);
    spdlog::info("Simple Net Server, about to start with {}!", !end_thread);
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
	    //spdlog::debug("Made it here!");    
	    struct ethhdr* eth = (struct ethhdr*)recv_ptr;
   	    if (ntohs(eth->h_proto) == ETH_APPEND_REQ) {
	        got_quorum = true;
		uint64_t allocated_packet_size = 150;
     		std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
     		memset(packet.get(), 'x', allocated_packet_size);
     		packet[allocated_packet_size - 1] = '\0';
     		net->send_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ);
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
    std::thread server_thread(custom_server, std::move(net), json_name, 0, get_batch_size(config), get_batch_on(config));

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

