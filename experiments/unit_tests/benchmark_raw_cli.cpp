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
#include <shared_mutex>
#include <utility>
#include <condition_variable>
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
#include <queue>
#include <unordered_map>

#include "spdlog/spdlog.h"
#include "utils.h"
#include "measure.h"
#include "ring_headers.h"

const uint64_t MAX_PACKET_SIZE = 8192; // TODO put in the yaml
const uint64_t MAX_WAIT_TIME = 100;
std::string sequence_pkt_type = "sequencer";
std::string storage_pkt_type = "storage";
bool end_thread = false;

std::mutex recv_q_mutex;
std::unordered_map<uint64_t, std::queue<char*>> recv_q;
std::condition_variable cv;

void receiver(int recv_socket,
              char* recv_ptr,
	      uint64_t num_threads) {
    while (!end_thread) {
        int numbytes = 0;
        struct sockaddr_storage src_addr;
        socklen_t addr_len = sizeof src_addr;
        if ((numbytes = recvfrom(recv_socket, recv_ptr, MAX_PACKET_SIZE, 0, (struct sockaddr *)&src_addr, &addr_len)) < 0) {
            continue;
        }

        if (!recv_ptr) {
            continue;
        }
        struct ethhdr* eth = (struct ethhdr*)recv_ptr;
        if (ntohs(eth->h_proto) != ETH_APPEND_RESP) {
            continue;
        }
        struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr));
	char* pkt = (char*)std::malloc(sizeof(struct ethhdr) + sizeof(struct iphdr) + sizeof(struct ring_append_entry) + ring->payload_size);
        memcpy(pkt, recv_ptr, sizeof(struct ethhdr) + sizeof(struct iphdr) + sizeof(struct ring_append_entry) + ring->payload_size); 
        {
	    std::unique_lock<std::mutex> lock(recv_q_mutex);
            recv_q[ring->thread_id % num_threads].push(pkt);
        }
	cv.notify_all();
    }
}

void raw_bench(char* send_packet,
               uint64_t packet_size,
	       struct sockaddr_ll sin,
	       int send_socket,
	       std::string json_name,
	       uint64_t batch_size,
	       bool batch_on,
	       uint64_t thread_id,
	       uint64_t num_threads) {
    spdlog::critical("Network Client Thread starting with TID = {}", gettid());
    std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id);

    auto start_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
    double start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(start_duration_since_epoch).count();
    while (!end_thread) {
        double start_time = stat->getStartLat();
        ssize_t num_bytes = 0;
        if ((num_bytes = sendto(send_socket, send_packet, packet_size, 0, (struct sockaddr*)(&sin), sizeof(sin))) < 0 || 
                ((uint64_t)num_bytes != packet_size)) {
            spdlog::warn("Error {} occurred: {}", std::to_string(errno), strerror(errno));
            spdlog::debug("Num bytes sent: {} vs. expected: {}", num_bytes, packet_size);
        }
     	bool got_quorum = false;
        while (!got_quorum) {
	    if (end_thread) {
                break;
	    }
	    char* recv_ptr = NULL;

	    {
	        std::unique_lock<std::mutex> lock(recv_q_mutex);
		cv.wait(lock, [thread_id, num_threads] { return !recv_q[thread_id % num_threads].empty() || end_thread; });
		if (end_thread) {
		    break;
		}
	        recv_ptr = recv_q[thread_id % num_threads].front();
	        recv_q[thread_id % num_threads].pop();
	    }

		stat->getDuration(start_time);
		stat->addOp();
	        got_quorum = true;
		free(recv_ptr);
        }
    }
    auto end_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
    double end_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(end_duration_since_epoch).count();
    double dur = end_time_s - start_time_s;
    spdlog::debug("Made it out of the loop!");

    stat->getAvgLatency();
    stat->getThroughput((uint64_t)dur);
    stat->getTotalOps();
    //stat->exportResultsToJson();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string input_file = std::string(argv[1]);
    YAML::Node config = YAML::LoadFile(input_file);
    set_spdlog_level(get_log_level(config));
    
    //std::vector<std::thread> client_threads;
    
    uint64_t batch_size = get_batch_size(config);
    bool batch_on = get_batch_on(config);
    uint64_t payload_size = get_payload_size(config);
    std::array<uint8_t, 6> dst_mac = get_stor_mac(config);
    std::string dst_ip = get_stor_ip(config);
    std::string send_interface = get_interface(config);
    std::string self_ip = get_self_ip(config);
    std::vector<char*> send_bufs;
    std::vector<char*> recv_bufs;   
    std::vector<std::thread> benchmark_vec;

     // Receive
    int recv_socket;
    if ((recv_socket = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL))) == -1) {
        spdlog::critical("Unable to create raw socket! Error {} occurred: {}", std::to_string(errno), strerror(errno));
        return -1;
    }
    struct timeval timeout;
    timeout.tv_sec = 1;  // 5 seconds timeout
    timeout.tv_usec = 0;
    if (setsockopt(recv_socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        spdlog::critical("Cannot set socket options, Error {} occurred: {}", std::to_string(errno), strerror(errno));
        return -1;
    }
    if (setsockopt(recv_socket, SOL_SOCKET, SO_BINDTODEVICE, send_interface.c_str(), strlen(send_interface.c_str())) < 0) {
        perror("Error binding socket to device. Interface name wrong or permissions failed.");
        close(recv_socket);
        return -1;
    }
    int ignore_outgoing = 1;
    if (setsockopt(recv_socket, SOL_PACKET, PACKET_IGNORE_OUTGOING, &ignore_outgoing, sizeof(ignore_outgoing)) < 0) {
        perror("Error binding socket to device. Interface name wrong or permissions failed.");
        close(recv_socket);
        return -1;
    }

    uint64_t num_threads = 8;
    for (uint64_t i = 0; i < num_threads; i++) {
	std::queue<char*> q;
        recv_q.emplace(i, q);
    }
    char* norm_buf = (char*)std::malloc(MAX_PACKET_SIZE);
    std::thread recv_thread(receiver, recv_socket, norm_buf, num_threads);
    pthread_t native_handle = recv_thread.native_handle();

    // Create a CPU set and add the desired core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(15, &cpuset); // Pin to core 'i'

    // Set thread affinity
    int result = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpuset);
    if (result != 0) {
        std::cerr << "Error setting thread affinity for thread " << recv_thread.get_id() << ": " << result << std::endl;
    } 
    for (uint64_t i = 0; i < num_threads; i++) {
        // Send 
        int send_socket = -1;
        if ((send_socket = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL))) == -1) {
            spdlog::critical("Unable to create raw socket! Error {} occurred: {}", std::to_string(errno), strerror(errno));
            throw std::runtime_error("Can't create sending socket");
        }
        spdlog::debug("The socket fd is {}", send_socket);
        
   
        // create eth hdr
        int eth_type_def = ETH_APPEND_REQ;
        std::unique_ptr<struct ethhdr> eth = std::make_unique<struct ethhdr>();
        for (int i = 0; i < 6; i++) { // 48 bit mac address - local broadcast
            eth.get()->h_dest[i] = dst_mac[i];
        }
        std::unique_ptr<struct ifreq> ifr = std::make_unique<struct ifreq>();
        memset(ifr.get(), 0, sizeof(struct ifreq));
            
        snprintf (ifr.get()->ifr_name, sizeof (ifr.get()->ifr_name), "%s", (const char*)send_interface.c_str());
        if (ioctl(send_socket, SIOCGIFHWADDR, ifr.get()) < 0) {
            spdlog::critical("Unable to get our MAC address! Errno {} with error {}", std::to_string(errno), strerror(errno));
            return -1;
        }
        memcpy(eth.get()->h_source, ifr.get()->ifr_hwaddr.sa_data, 6 * sizeof (uint8_t));
        eth.get()->h_proto = htons(eth_type_def); // Tells receiver how to parse packet

        // create ip hdr
        std::unique_ptr<struct iphdr> ip = std::make_unique<struct iphdr>();
        ip.get()->ihl      = 5; //version length
        ip.get()->version  = 4; // version; should we allow for ipv6?
        ip.get()->tos      = 0; // type of service - set to normal, could change in future
        ip.get()->tot_len  = htons(sizeof(struct iphdr)); // TODO check total length of packet header
        ip.get()->id       = htons(54321); // default ID number for ip packet
        ip.get()->ttl      = 64; // default hops; circle back in case of change
        ip.get()->protocol = IPPROTO_RAW; // Raw IP
        ip.get()->saddr = inet_addr(self_ip.c_str()); // source address
        ip.get()->daddr = inet_addr(dst_ip.c_str()); // destination address
        ip.get()->check = 0; //checksum(pkt, sizeof(struct iphdr)); // checksum ONLY for the IPv4 header^

        // Sockaddr structure
        struct sockaddr_ll sin;
        sin.sll_ifindex = if_nametoindex((const char*)send_interface.c_str());//ifr.get()->ifr_ifindex;
        sin.sll_halen = ETH_ALEN;
        for (int j = 0; j < 6; j++) { // 48 bit mac address - local broadcast
            sin.sll_addr[j] = dst_mac[j];
        }

        size_t size_of_hdr = get_ring_append_size();
        std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(i, 1);
        hdr.get()->payload_size = payload_size;
        hdr.get()->num_entries = 1;
        hdr.get()->thread_id = i;

        std::string payload(payload_size, 'x');
        
        // If the packet type is a descriptive string to indicate header type
        /* Running a raw socket based protocol */
        size_t packet_size = sizeof(struct ethhdr) + sizeof(struct iphdr) + size_of_hdr + payload.length() + 1;
        //spdlog::debug("Eth hdr: {}, IP hdr: {}, Size hdr + payload: {}", sizeof(struct ethhdr), sizeof(struct iphdr), pkt_len);

        char* packet = (char*)std::malloc(packet_size);
        
        /*Create ethernet header - dest addr will currently indicate multicast TODO unicast*/
        memcpy(packet, eth.get(), sizeof(struct ethhdr));
        memcpy(packet + sizeof(struct ethhdr), ip.get(), sizeof(struct iphdr));
        memcpy(packet + sizeof(struct ethhdr) + sizeof(struct iphdr), reinterpret_cast<const char*>(hdr.get()), size_of_hdr);
        memcpy(packet + sizeof(struct ethhdr) + sizeof(struct iphdr) + size_of_hdr, payload.c_str(), payload.length());
        packet[packet_size - 1] = '\0';
        send_bufs.push_back(packet);

    	std::string json_name = "testing" + std::to_string(i);
    	benchmark_vec.push_back(std::thread(raw_bench, packet, packet_size, sin, send_socket, json_name, batch_size, batch_on, i, num_threads));

    	pthread_t native_handle = benchmark_vec[i].native_handle();

    	// Create a CPU set and add the desired core
    	cpu_set_t cpuset;
    	CPU_ZERO(&cpuset);
    	CPU_SET(i % std::thread::hardware_concurrency(), &cpuset); // Pin to core 'i'

    	// Set thread affinity
    	int result = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpuset);
    	if (result != 0) {
    	    std::cerr << "Error setting thread affinity for thread " << benchmark_vec[i].get_id() << ": " << result << std::endl;
    	}  
    } 
    spdlog::debug("Going to wait to sleep {}", get_experiment_duration(config));
    std::chrono::seconds sleep_duration(get_experiment_duration(config));
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
    recv_thread.join();
    free(norm_buf);
    cv.notify_all();
    for (uint64_t i = 0; i < benchmark_vec.size(); i++) {
        benchmark_vec[i].join();
	free(send_bufs[i]);
    }

    spdlog::debug("Thread done!");
    return 0;
}
