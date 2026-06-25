#include "pringles_storage.h"
#include "spdlog/spdlog.h"
#include "ringclient.pb.h"
#include "ring_headers.h"
#include "yaml-cpp/yaml.h"
#include "utils.h"

#include <errno.h>
#include <numeric>
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
    spdlog::critical("Pringles Storage is starting!");
    YAML::Node config = YAML::LoadFile(input_file);
    
    // Shards
    this->shard_id = get_shard_id(config);
    this->use_shards = get_use_shards(config);

    // Streams
    this->use_streams = get_use_streams(config);

    this->num_append_stor_threads = 5; //get_append_stor_threads(config);
    for (uint64_t i = 0; i < num_append_stor_threads; i++) {
	std::unique_ptr<Network> append_net = std::make_unique<Network>( 
				   get_socket_type(config),
                                   get_log_level(config),
				   get_batch_size(config), 
				   get_batch_on(config),
				   get_batch_timeout(config),
				   get_interface(config),
				   get_self_ip(config));
        append_stor_threads.emplace_back(std::thread(&LogStorage::append_server, this, std::stoi(get_stor_receive_port(config)), std::move(append_net)));
    }

    // Initialize storage server identity variables

    this->shard_switch_id = get_shard_switch_id(config);
    this->view_num = 1;
    this->max_duration = get_experiment_duration(config);
    this->ssid = storage_id;
    this->use_switch = get_use_switch(config);
    set_spdlog_level(get_log_level(config));

    // Routing
    this->use_switch = get_use_switch(config);
    this->switch_mac = get_switch_mac(config);
    this->switch_ip = get_switch_ip(config);
    this->switch_recv_port = get_switch_receive_port(config);

    this->append_cntr = 0;
    spdlog::critical("Done with the constructor!");
}

LogStorage::~LogStorage() {
    for (uint64_t i = 0; i < append_stor_threads.size(); i++) {
        append_stor_threads[i].join();
    }
}

bool LogStorage::store(uint64_t idx, std::string entry) {
    tbb::concurrent_hash_map<uint64_t, std::string>::accessor accessor;
    bool insert_succ = concurrent_stor.insert(accessor, idx);
    if (insert_succ) {
        accessor->second = entry;
    }
    accessor.release();
    return insert_succ;
}

std::string LogStorage::get(uint64_t idx) {
    std::string entry = "";
    tbb::concurrent_hash_map<uint64_t, std::string>::accessor accessor;
    // 2. Attempt to find the key
    if (concurrent_stor.find(accessor, idx)) {
	entry = accessor->second;
    } else {
	spdlog::warn("Key {} not found!!!", idx);
    }
    return entry;
}

void LogStorage::change_view(uint64_t new_view_num) {
    view_num = new_view_num;
}

void LogStorage::wait_to_finish() {
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
}

void LogStorage::append_server(int append_port, std::unique_ptr<Network> append_net) {
    spdlog::critical("APPEND SERVER thread starting with tid = {}", gettid());
    int batch_socket = append_net->setup_batch_socket(append_port);
    if (batch_socket < 0) {
        append_net->stop_batch_threads();
	return;
    }

    int rnd_send_socket = append_net->create_random_port_socket();
    if (rnd_send_socket < 0) {
        append_net->stop_batch_threads();
	return;
    }

    uint64_t append_cntr = 0;
    spdlog::debug("Created batch socket {}!", batch_socket);
    while (!end_thread) {
	int num_received = append_net->recv_many_packets(batch_socket);
	if (num_received < 0) {
	    continue;
	}
	spdlog::debug("Received {} packets!", num_received);
        for (int i = 0; i < num_received; i++) {
	    char* buf = append_net->get_buf(i);
	    struct mmsghdr msg = append_net->get_msg(i);

	    if (buf != NULL) {
	        //struct ring_append_entry* append_entry = (struct ring_append_entry*)(buf + sizeof(struct ring_type));
	        struct ring_type* type_hdr = (struct ring_type*)(buf);
         	// Update the type of the type header
         	type_hdr->type = htons(ETH_APPEND_RESP);
     	        // Get append payload
 	        /*uint64_t sequence_no = ntohl(append_entry->g_idx);
                char* entry = (char*)(buf + sizeof(struct ring_type) + sizeof(struct ring_append_entry));
           
     	        // Actually store the entry
                std::string string_to_store(entry); 
                store(sequence_no, string_to_store);
     	        max_append_idx = sequence_no;
                spdlog::debug("The updated index is: {}, Entry: {}, Recv port: {}", max_append_idx, string_to_store, ntohs(append_entry->recv_port));
*/
		if (msg.msg_len > 0) {
		    struct iovec* iovecs = append_net->get_iovecs();
                    iovecs[i].iov_len = msg.msg_len;
                }
		//memcpy(msg.msg_hdr.msg_name, &server_addr, sizeof(server_addr));
     	        append_cntr += 1;
	    }
	}

	struct mmsghdr* msgs = append_net->get_msgs();
	struct iovec* iovecs = append_net->get_iovecs();
	spdlog::debug("Sending the batch of processed messages out!");
        sendmmsg(rnd_send_socket, msgs, num_received, 0);
        for (int i = 0; i < num_received; i++) {
            msgs[i].msg_hdr.msg_namelen = sizeof(struct sockaddr_in);
            iovecs[i].iov_len = MAX_PACKET_SIZE;
        }
    }
    append_net->stop_batch_threads();
    close(batch_socket);
    spdlog::critical("Storage server counter: {}", append_cntr);
}

std::string LogStorage::get_quad_ip(uint32_t ip_addr) { // TODO DEDUPLICAT
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
