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

#include "spdlog/spdlog.h"
#include "utils.h"
#include "measure.h"

#define ETH_APPEND_REQ 0x0860

const uint64_t MAX_WAIT_TIME = 100;
std::string sequence_pkt_type = "sequencer";
std::string storage_pkt_type = "storage";
bool end_thread = false;

void custom_client(std::string input_file, uint64_t thread_id) {
    YAML::Node config = YAML::LoadFile(input_file);
    std::string json_name = get_json_name(config);
    uint64_t batch_size = get_batch_size(config);
    bool batch_on = get_batch_on(config);
    uint64_t max_duration = get_experiment_duration(config);
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
				   1, false); // TODO Need to do something else here??? Storage server could be faster
    set_spdlog_level(get_log_level(config));
    spdlog::info("Simple Network: Sending/Receiving to remote host");

    spdlog::critical("Network Client Thread starting with TID = {}", gettid());
    std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id);
    spdlog::info("Simple Net Client!");
    uint32_t nonce = 1;

    uint64_t allocated_packet_size = 4000;
    /*char* allocated_packet = std::malloc(allocated_packet_size);
    memset(allocated_packet, 'x', allocated_packet_size);*/
    while (!end_thread) {
	//uint32_t nonce = generate_nonce();

     	// Create packet buffer which will be sent  

     	std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);

     	//packet[allocated_packet_size - 1] = '\0';
	
        double start_time = stat->getStartLat();
	net->send_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ);

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
   	    if (ntohs(eth->h_proto) == ETH_APPEND_REQ) {
		stat->getDuration(start_time);
		stat->addOp();
	        got_quorum = true;
	    }
        }
	nonce += 1;
    }
    spdlog::debug("Made it out of the loop!");
    stat->getAvgLatency();
    stat->getThroughput(max_duration);
    stat->getTotalOps();
    stat->exportResultsToJson();

    net->done();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string input_file = std::string(argv[1]);
    YAML::Node config = YAML::LoadFile(input_file);
    std::vector<std::thread> client_threads;
    for (uint64_t i = 0; i < get_num_client_threads(config); i++) {
        client_threads.emplace_back(std::thread(&custom_client, input_file, i));

        pthread_t native_handle = client_threads[i].native_handle();

    	// Create a CPU set and add the desired core
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
    	CPU_SET(i, &cpuset); // Pin to core 'i'

        // Set thread affinity
        int result = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpuset);
        if (result != 0) {
            std::cerr << "Error setting thread affinity for thread " << client_threads[i].get_id() << ": " << result << std::endl;
        }  
    }
   
    spdlog::debug("Going to wait to sleep {}", get_experiment_duration(config));
    std::chrono::seconds sleep_duration(get_experiment_duration(config));
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
    spdlog::debug("Thread done!");
    for (uint64_t i = 0; i < client_threads.size(); i++) {
        client_threads[i].join();
    }
    return 0;
}

