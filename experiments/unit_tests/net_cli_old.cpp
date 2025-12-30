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
#include "ring_headers.h"

const uint64_t MAX_WAIT_TIME = 100;
std::string sequence_pkt_type = "sequencer";
std::string storage_pkt_type = "storage";
bool end_thread = false;

void custom_client(std::unique_ptr<Network> net, 
		   uint64_t thread_id,
		   std::string json_name,
		   uint64_t batch_size,
		   bool batch_on,
		   uint64_t payload_size,
		   std::array<uint8_t, 6> switch_mac,
		   std::string switch_ip) {
    uint64_t nonce = thread_id;
    uint64_t scale = 2;

    std::vector<int> eth_types = {ETH_APPEND_REQ};


    spdlog::info("Simple Network: Sending/Receiving to remote host");

    spdlog::critical("Network Client Thread starting with TID = {}", gettid());
    std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id);
    spdlog::info("Simple Net Client!");

    std::string payload(payload_size, 'x');
    size_t size_of_hdr = get_ring_append_size();
    std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(nonce, thread_id);
    hdr.get()->payload_size = payload_size;
    hdr.get()->num_entries = 1;
    uint64_t allocated_packet_size = size_of_hdr + payload_size + 1;

    auto start_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
    double start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(start_duration_since_epoch).count();
    while (!end_thread) {
     	// Create packet buffer which will be sent  
     	std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);

        double start_time = stat->getStartLat();
	memcpy(packet.get(), reinterpret_cast<const char*>(hdr.get()), size_of_hdr);
	memcpy(packet.get() + size_of_hdr, payload.c_str(), payload.length());
     	packet[allocated_packet_size - 1] = '\0';
	//memcpy(packet.get() + size_of_hdr, reinterpret_cast<const char*>(&appInfo), sizeof(AppendInfo));
	
	//spdlog::debug("Size of packet: {} and size of app info: {} and size of hdr: {}", allocated_packet_size, sizeof(AppendInfo), size_of_hdr);
	net->send_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ, switch_mac, switch_ip);

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
   	    if (ntohs(eth->h_proto) == ETH_APPEND_RESP) {
		// TODO: Check nonce
		stat->getDuration(start_time);
		stat->addOp();
	        got_quorum = true;
	    }
        }
	nonce *= scale;
	scale += 1;
    }
    auto end_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
    double end_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(end_duration_since_epoch).count();
    double dur = end_time_s - start_time_s;
    spdlog::debug("Made it out of the loop!");
    stat->getAvgLatency();
    stat->getThroughput((uint64_t)dur);
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
    set_spdlog_level(get_log_level(config));
    std::vector<std::thread> client_threads;
    std::string json_name = get_json_name(config);
    uint64_t batch_size = get_batch_size(config);
    bool batch_on = get_batch_on(config);
    uint64_t payload_size = get_payload_size(config);
    std::array<uint8_t, 6> switch_mac = get_switch_mac(config);
    std::string switch_ip = get_switch_ip(config);
    for (uint64_t i = 0; i < get_num_client_threads(config); i++) {
    	std::unique_ptr<Network> net = std::make_unique<Network>(get_send_port(config), 
                                  				 get_recv_port(config),
				  				 get_socket_type(config),
                                  				 get_log_level(config),
				  				 get_batch_size(config),
				  				 get_batch_on(config),
				  				 get_interface(config),
				  				 get_self_ip(config),
				  				 get_num_pkt_types(config),
				  				 false);
    
        client_threads.emplace_back(std::thread(&custom_client, std::move(net), i+1, json_name, batch_size, batch_on, payload_size, switch_mac, switch_ip));

        pthread_t native_handle = client_threads[i].native_handle();

    	// Create a CPU set and add the desired core
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
    	CPU_SET(i % std::thread::hardware_concurrency(), &cpuset); // Pin to core 'i'

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

