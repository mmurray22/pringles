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
#include <queue>
#include <unordered_map>
#include <condition_variable>

#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_hash_map.h>
#include "spdlog/spdlog.h"
#include "utils.h"
#include "measure.h"
#include "ring_headers.h"

const uint64_t MAX_WAIT_TIME = 100;
uint64_t NUM_THREADS = 1;
std::string sequence_pkt_type = "sequencer";
std::string storage_pkt_type = "storage";
bool end_thread = false;

std::mutex recv_q_mutex;
tbb::concurrent_hash_map<uint64_t, tbb::concurrent_queue<char*>> recv_q;

//std::unordered_map<uint64_t, std::queue<char*>> recv_q;
std::condition_variable cv;

// TODO need to batch by source - fine for a single physical client machines
void receiver(int recv_socket,
              char* recv_ptr) {

    spdlog::critical("Network Recv Thread starting with TID = {}", gettid());
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

	// Unpack batch
        size_t size_of_hdr = get_ring_append_size();
	struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr));
	uint64_t recv_offset = sizeof(struct ethhdr) + sizeof(iphdr) + size_of_hdr;
	//spdlog::debug("Received NEW packet with num entries = {}", append_entry->num_entries);
	for (uint64_t i = 0; i < append_entry->num_entries; i++) {
	    struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + recv_offset);
	    //spdlog::debug("Received packet with num entries = {}, The payload size: {}, thread id: {}", ring->num_entries, ring->payload_size, ring->thread_id);
	    char* pkt = (char*)std::malloc(sizeof(struct ring_append_entry) + ring->payload_size);
            memcpy(pkt, (char*)(recv_ptr + recv_offset), sizeof(struct ring_append_entry) + ring->payload_size); 
        
    	    tbb::concurrent_hash_map<uint64_t, tbb::concurrent_queue<char*>>::accessor acc;
 	    if (!recv_q.find(acc, ring->thread_id % NUM_THREADS)) {
	    	spdlog::debug("UNKNOWN THREAD ID {}, skipping", ring->thread_id);
	    }
            tbb::concurrent_queue<char*>& push_q = acc->second;
	    acc.release();
	    //spdlog::debug("THREAD ID {}", ring->thread_id);


	    push_q.push(pkt);
	    recv_offset += (size_of_hdr + ring->payload_size);
	}

	//struct ring_append_entry* ring = (struct ring_append_entry*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr));
	{
	    std::unique_lock<std::mutex> lock(recv_q_mutex);
        }
	//spdlog::debug("NOTFYING THE CONDITION VARIABLE!");
	cv.notify_all();
    }
}

void custom_client(std::unique_ptr<Network> net, 
		   uint64_t thread_id,
		   std::string json_name,
		   uint64_t batch_size,
		   bool batch_on,
		   uint64_t payload_size,
		   std::array<uint8_t, 6> switch_mac,
		   std::string switch_ip,
		   std::array<uint8_t, 6> stor_mac,
		   std::string stor_ip,
		   std::string stor_receive_port,
		   std::string switch_receive_port
		   bool use_switch,
		   bool use_stor) {
    (void) switch_mac;
    (void) stor_mac;
    uint64_t nonce = thread_id;
    uint64_t scale = 2;
    uint64_t highest_idx = 0;

    std::vector<int> eth_types = {ETH_APPEND_REQ};
    
    std::unique_ptr<struct ring_type> ring_type_hdr = create_ring_type(ETH_APPEND_REQ);
    spdlog::info("Simple Network: Sending/Receiving to remote host");

    spdlog::critical("Network Client Thread starting with TID = {}, internal thread id {}", gettid(), thread_id);
    std::unique_ptr<Stats> stat = std::make_unique<Stats>(batch_size, batch_on, json_name, thread_id);

    std::string payload(payload_size, 'x');
    size_t size_of_hdr = get_ring_append_size();
    std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(nonce, thread_id);
    hdr.get()->payload_size = payload_size;
    hdr.get()->num_entries = 1;
    hdr.get()->thread_id = thread_id;
    uint64_t allocated_packet_size = size_of_hdr + payload_size + 1;

    auto start_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
    double start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(start_duration_since_epoch).count();
    
    // Only need to read from the map once to get the queue
    /*tbb::concurrent_hash_map<uint64_t, tbb::concurrent_queue<char*>>::accessor acc;
    if (!recv_q.find(acc, thread_id % NUM_THREADS)) {
        spdlog::debug("QUEUE MISSING FOR THREAD ID {}", thread_id);
	return;
    }
    tbb::concurrent_queue<char*>& check = acc->second;
    acc.release();*/

    while (!end_thread) {
     	// Create packet buffer which will be sent  
     	std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        double start_time = stat->getStartLat();
	hdr.get()->nonce = nonce;
	memcpy(packet.get(), reinterpret_cast<const char*>(hdr.get()), size_of_hdr);
	memcpy(packet.get() + size_of_hdr, payload.c_str(), payload.length() + 1);

	//spdlog::debug("Size of packet: {} and size of app info: {} and size of hdr: {}", allocated_packet_size, payload.length(), size_of_hdr);
	if (use_switch) {
	    net->send_udp_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ, switch_ip, switch_receive_port);
	} else {
	    //for (uint64_t i = 0; i < stor_ips.size(); i++) {
	    net->send_udp_packet(std::move(packet), allocated_packet_size, 0, ETH_APPEND_REQ, stor_ip, stor_receive_port);
	    //}
	}

     	bool got_quorum = false;
        while (!got_quorum) {
	    if (end_thread) {
                break;
	    }
	
	    /*
	    tbb::concurrent_hash_map<uint64_t, tbb::concurrent_queue<char*>>::accessor acc;
            if (!recv_q.find(acc, thread_id % NUM_THREADS)) {
                spdlog::debug("QUEUE MISSING FOR THREAD ID {}", thread_id);
                return;
            }
            tbb::concurrent_queue<char*>& check = acc->second;
            acc.release();
    
	    char* recv_ptr = NULL;
            {
	        std::unique_lock<std::mutex> lock(recv_q_mutex);
		cv.wait(lock, [thread_id, &check] { 
				return !check.empty() || end_thread; });

		//spdlog::debug("After the condition variable! {}", check.empty());
		if (check.try_pop(recv_ptr)) {
		    //spdlog::debug("Successfully dequeued entry!! Where the receive pointer is: {}", recv_ptr != NULL); 
		}
            }
	    */

	    char* recv_ptr = net->recv_packet();
	    if (!recv_ptr) {
	        continue;
	    }
	    
	    // TODO: Check nonce
	    //spdlog::debug("Registering the time and operation!");
	    stat->getDuration(start_time);
	    stat->addOp();
	    got_quorum = true;
        }
	nonce *= scale;
	scale += 1;
    }
    auto end_duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
    double end_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(end_duration_since_epoch).count();
    double dur = end_time_s - start_time_s;
    spdlog::debug("Made it out of the loop!");
    spdlog::critical("Highest index seen is: {}", highest_idx);
    spdlog::critical("For thread {}, the lat is: {}, tput: {}, total ops: {}", thread_id, stat->getAvgLatency(), stat->getThroughput((uint64_t)dur), stat->getTotalOps());
    //stat->getAvgLatency();
    //stat->getThroughput((uint64_t)dur);
    //stat->getTotalOps();
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
    std::array<uint8_t, 6> stor_mac = get_stor_mac(config);
    std::string stor_ip = get_stor_ip(config);
   
    NUM_THREADS = get_num_client_threads(config);

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
    if (setsockopt(recv_socket, SOL_SOCKET, SO_BINDTODEVICE, get_interface(config).c_str(), strlen(get_interface(config).c_str())) < 0) {
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


    for (uint64_t i = 0; i < NUM_THREADS; i++) {
        tbb::concurrent_hash_map<uint64_t, tbb::concurrent_queue<char*>>::accessor acc;
        if (recv_q.insert(acc, i)) {
	    spdlog::debug("New queue created!");
	}
	acc.release();
    }
    char* norm_buf = (char*)std::malloc(MAX_PACKET_SIZE);
    std::thread recv_thread(receiver, recv_socket, norm_buf);
    pthread_t recv_native_handle = recv_thread.native_handle();

    // Create a CPU set and add the desired core
    cpu_set_t recv_cpuset;
    CPU_ZERO(&recv_cpuset);
    CPU_SET(std::thread::hardware_concurrency() - 1, &recv_cpuset); // Pin to core 'i'
    int recv_result = pthread_setaffinity_np(recv_native_handle, sizeof(cpu_set_t), &recv_cpuset);
    if (recv_result != 0) {
        std::cerr << "Error setting thread affinity for thread " << recv_thread.get_id() << ": " << recv_result << std::endl;
    } 
    for (uint64_t i = 0; i < NUM_THREADS; i++) {
        uint64_t send_port = get_send_port(config) + i;
	uint64_t recv_port = get_recv_port(config) + + NUM_THREADS + i;	
 
    	std::unique_ptr<Network> net = std::make_unique<Network>(std::to_string(send_port), 
                                  				 std::to_string(recv_port),
				  				 get_socket_type(config),
                                  				 get_log_level(config),
				  				 get_batch_size(config),
				  				 get_batch_on(config),
				  				 get_interface(config),
				  				 get_self_ip(config),
				  				 get_num_pkt_types(config),
				  				 false);
        client_threads.emplace_back(std::thread(&custom_client, 
				                std::move(net), 
						i, 
						json_name, 
						batch_size, 
						batch_on, 
						payload_size, 
						switch_mac, 
						switch_ip, 
						stor_mac,
						stor_ip,
						get_stor_receive_port(config), 
						get_switch_receive_port(config),
						get_use_switch(config),
						get_use_stor(config)));

        pthread_t native_handle = client_threads[i].native_handle();

    	// Create a CPU set and add the desired core
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
    	CPU_SET((i+1) % (std::thread::hardware_concurrency() - 1), &cpuset); // Pin to core 'i'

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
    cv.notify_all();
    for (uint64_t i = 0; i < client_threads.size(); i++) {
        client_threads[i].join();
    }
    spdlog::debug("Done joining the clients!");
    recv_thread.join();
    free(norm_buf);
    return 0;
}

