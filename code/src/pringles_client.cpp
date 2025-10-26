#include <thread>
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

#include "ring_headers.h"
#include "ringclient.pb.h"
#include "pringles_client.h"
#include "utils.h"
#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"
#define RECEIVE_PORT 3149

// TODO make multithreaded???
LogClient::LogClient(std::string input_file, uint64_t cli_id) {

   // Get packet types for sending/receiving

   YAML::Node config = YAML::LoadFile(input_file);
   num_pkt_types = get_num_pkt_types(config);
  
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
				   get_pkt_eth_types());
    cid = cli_id;
    set_spdlog_level(get_log_level(config));
    spdlog::info("Pringles Client: Only Append being tested");
    
    // Start receiving thread 
    this->pkt_q = {};
    for (size_t i = 0; i < num_pkt_types; i++) {
	std::queue<char*> q = {};
	this->pkt_q.insert(std::pair<PacketType, std::queue<char*>>(PacketType(i), q));
    }
    this->recv_thread = std::thread(&LogClient::pringles_recv_queue, this);
    this->duration_thread = std::thread(&LogClient::wait_for_finish, this);

    // Protocol types
    this->seq = SequencerType(get_sequencer_type(config));
    //this->stor = StorageType(get_storage_type(config));

    // Updating the log 
    cached_log_entries = {};
    this->max_duration = get_experiment_duration(config);
}

LogClient::~LogClient() {
    recv_thread.join();
    //TODO subscribe_thread.join();
    duration_thread.join();
    spdlog::debug("Joined the client threads!");
    net->done();
    stat.getAvgLatency();
    stat.getThroughput(max_duration);
    stat.getTotalOps();
}

/* Custom function */
uint64_t LogClient::append(std::string entry) {
     uint32_t nonce = generate_nonce();
     spdlog::debug("Nonce: {}", nonce);
     std::unique_ptr<ringclient::Payload> p = std::make_unique<ringclient::Payload>();
     std::string output;
     std::unique_ptr<char[]> packet;
     size_t packet_size = 0;
    
     // General packet information
     p->set_packet_type(static_cast<int>(PacketType::append));
     p->set_nonce(nonce);

     spdlog::debug("Creating append packet!");
     
     // Create packet payload
     ringclient::AppendEntry* app = p->mutable_append();
     switch (seq)
     {
	case SequencerType::DUMMY: 
	{
	    app->set_idx(1);
	    break;
	};	    
	default:
	{
            app->set_idx(-1); // To be filled in by the switch
	};
     }

     app->set_entry(entry);
     //p->set_allocated_append(app);
     p->SerializeToString(&output);
     packet_size += output.length();
     
     // Create packet header
     size_t size_of_hdr = get_size_of_hdr(PacketType::append);
     packet_size += size_of_hdr;
     std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(generate_nonce(), 1);
     packet = std::make_unique<char[]>(packet_size+1);
     
     // Construct packet
     memcpy(packet.get(), reinterpret_cast<const char*>(hdr.get()), size_of_hdr);
     memcpy(packet.get() + size_of_hdr, &output, output.length());
     packet[packet_size] = '\0';
     //std::unique_ptr<char[]> pkt = create_pkt(PacketType::append, nonce, entry);
     spdlog::debug("Adding append packet to send queue of size {} with payload {} and header size {}", packet_size, output.length(), size_of_hdr);
     net->add_to_send_queue(std::move(packet), static_cast<int>(PacketType::append), packet_size);
     stat.startLatTimer(nonce);
     /*uint64_t id = wait_for_append(PacketType::append, nonce);  
     if (id <= 0) {
         spdlog::critical("Packet wasn't processed - id negative!");
	 return 0;
     }
     return id;*/
     return 0;
}

std::string LogClient::read(uint64_t idx) {
	/*std::string default_str = "";
	uint32_t nonce = generate_nonce();
        std::unique_ptr<char[]> pkt = create_pkt(PacketType::append, nonce, entry);
        stat.startLatTimer(nonce);
        net->add_to_send_queue(std::move(pkt), static_cast<int>(PacketType::append));
        id = wait_for_append(PacketType::append, nonce);  
        if (id <= 0) {
            spdlog::critical("Packet wasn't processed - id negative!");
	    return 0;
        }
        return id;*/
	(void) idx;
	return NULL;
}

// Get latest committed entry
uint64_t LogClient::getTail() {
	// TODO later
	return 0;
}

// Subscribe to get all log updates after supplied index
// TODO: Add vector that the subscribed entries will go to?
void LogClient::subscribe(uint64_t idx) {
    this->subscribe_thread = std::thread(&LogClient::wait_for_subscribe, this, idx, static_cast<int>(PacketType::subscribe));
}

void LogClient::wait_for_subscribe(uint64_t idx, uint64_t pkt_type) {
     while (true) {
	if (end_thread) {
            spdlog::debug("!!!!!!!!!!!!!!TIME to stop");
            break;
        }
	
        char* subscribe_pkt = pkt_q[PacketType(pkt_type)].front();
	if (subscribe_pkt == NULL) { // TODO: condition variable nothing in the receive queue, so we sleep
	    /*spdlog::debug("Nothing receive! Going to sleep...");
            std::this_thread::sleep_for(std::chrono::milliseconds(MAX_WAIT_TIME));*/
            continue;
        }

	struct ring_subscribe_entry* subscribe_entry = (struct ring_subscribe_entry*)subscribe_pkt;
	uint64_t recv_idx = subscribe_entry->g_idx;
	if (recv_idx < idx) {
	    continue;
	}
 	pkt_q[PacketType(pkt_type)].pop();

	// Parse out packet contents (if needed)
	char* ser_payload = (char*)(subscribe_pkt + get_size_of_hdr(pkt_type));
	std::string str(ser_payload);
	ringclient::Payload payload;
	payload.ParseFromString(str);
	cached_log_entries.insert(std::pair<uint64_t, std::string>(recv_idx, payload.mutable_append()->entry()));
     }
}


// Garbage collect all log entries up to some index
bool LogClient::trim(uint64_t idx) {
	(void) idx;
	// TODO later
	return false;
}

/*** Helper functions ***/
int64_t LogClient::wait_for_append(PacketType pkt_type, uint32_t nonce) {
    const auto start = std::chrono::steady_clock::now();
    while (true) {
        const std::chrono::duration<double, std::milli> elapsed = std::chrono::steady_clock::now() - start;
        /*if (elapsed >= MAX_WAIT_TIME || end_thread) {
            spdlog::debug("!!!!!!!!!!!!!!TIMED OUT, packet with nonce {} never received", nonce);
            break;
        }*/
	
        char* recv_pkt_with_hdr = pkt_q[pkt_type].front();
	if (recv_pkt_with_hdr == NULL) { // TODO: condition variable nothing in the receive queue, so we sleep
	    /*spdlog::debug("Nothing receive! Going to sleep...");
            std::this_thread::sleep_for(std::chrono::milliseconds(MAX_WAIT_TIME));*/
            continue;
        }
	
	// Parse out header
        struct ring_append_entry* append_entry = (struct ring_append_entry*)recv_pkt_with_hdr;
	if (append_entry->nonce != nonce) {
	    pkt_q[pkt_type].push(recv_pkt_with_hdr);
	    continue;
	}
	pkt_q[pkt_type].pop();
	stat.endLatTimer(nonce);
	stat.addOp();
	return append_entry->g_idx;

	// Parse out packet contents (if needed)
    }
    return -1;
}

/*std::string wait_for_read(PacketType pkt_type, int32_t nonce) {
    while (true) {
        if (wait_time >= MAX_WAIT_TIME) {
            spdlog::debug("!!!!!!!!!!!!!!No more packets to receive.");
            break;
        }
	
        char* recv_pkt_with_hdr = pkt_q[pkt_type].pop();
	if (recv_pkt_with_hdr == NULL) { // nothing in the receive queue, so we sleep
            continue;
        }
	
	// Parse out header
        struct ring_read_entry* append_entry = (struct ring_append_entry*)recv_pkt_with_hdr;
	if (append_entry->nonce != nonce) {
	    pkt_q[pkt_type].push(recv_pkt_with_hdr);
	    continue;
	}

	// Parse out packet contents (if needed)
	char* ser_payload = (char*)(recv_pkt_with_hdr.get() + hdr_size);
	std::string str(ser_payload);
	ringclient::Payload payload;
	payload.ParseFromString(str);

    }
}*/

// TODO: No support for batching
std::unique_ptr<char[]> LogClient::create_pkt(PacketType pkt_type, 
		                   uint32_t nonce,
				   std::optional<std::string> entry,
			   	   std::optional<int64_t> idx) {
    std::unique_ptr<ringclient::Payload> p = std::make_unique<ringclient::Payload>();
    std::string output;
    std::unique_ptr<char[]> packet;
    size_t packet_size = 0;
    
    // General packet information
    p->set_packet_type(static_cast<int>(pkt_type));
    p->set_nonce(nonce);

    if (pkt_type == PacketType::append) {
        spdlog::debug("Append packet!");
	
	// Create packet payload
	ringclient::AppendEntry* app = p->mutable_append();
	app->set_idx(-1); // To be filled in by the switch
	if (entry.has_value()) {
	    app->set_entry(entry.value());
	}
	//p->set_allocated_append(app);
    	p->SerializeToString(&output);
	packet_size += output.length();
	
	// Create packet header
	size_t size_of_hdr = get_size_of_hdr(pkt_type);
        packet_size += size_of_hdr;
	std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(generate_nonce(), 1);
    	packet = std::make_unique<char[]>(packet_size+1);
	
	// Construct packet
	memcpy(packet.get(), reinterpret_cast<const char*>(hdr.get()), size_of_hdr);
        memcpy(packet.get() + size_of_hdr, &output, output.length());
	packet[packet_size] = '\0';
    } else if (pkt_type == PacketType::readentry) {
        /*spdlog::critical("Read packet!");
	ringclient::ReadEntry read;
	if (!idx.has_value()) {
	    return NULL;
	}
   	read.set_idx(idx.value());
	read.set_allocated_entry(NULL);
	p.set_allocated_read(read);*/
	(void) idx;
	spdlog::critical("Read not supported yet!");
    } else {
        spdlog::critical("Invalid packet type! No packet created.");
    }
    
    return packet;
}

void LogClient::pringles_recv_queue() {
    // initialize - for each packet type, receive queue
    while (true) {
	if (end_thread) {
	    break;
	}
	std::unique_ptr<char[]> recv_ptr = net->read_from_recv_queue(); // will receive the full packet, including Eth header
	if (!recv_ptr) {
	    continue;
	}
	uint64_t size_of_pkt = std::strlen(recv_ptr.get());
        std::unique_ptr<char[]> sample_pkt = std::make_unique<char[]>(size_of_pkt);
        struct ethhdr* eth = (struct ethhdr*)recv_ptr.get();

	if (eth->h_proto == ETH_APPEND_REQ) {
            size_t hdr_size = get_size_of_hdr(PacketType::append);
	    if (hdr_size == 0) {
                spdlog::debug("Improper header type!! Packet being discarded");
            	continue;
            }
            spdlog::debug("Ethernet protocol with size {}", hdr_size);
            char* rcv_str = (char*)(sample_pkt.get() + sizeof(struct ethhdr) + sizeof(struct iphdr));
	    pkt_q[PacketType::append].push(rcv_str);
	}
    }
}

void LogClient::wait_for_finish() {
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    spdlog::debug("End thread: {}", end_thread);
    end_thread = true;
} 

bool LogClient::experiment_status() {
    return !end_thread;
}

/* Header functions */
std::vector<int> LogClient::get_pkt_eth_types() {
    std::vector<int> eth_types = {};
    for(uint64_t i = 0; i < num_pkt_types; i++) {
        eth_types.push_back(get_eth_type(i));
    }
    return eth_types;
}

size_t LogClient::get_size_of_hdr(uint64_t pkt_type) {
    if (PacketType(pkt_type) == PacketType::append) {
        return sizeof(struct ring_append_entry);
    }
    return 0;
}

int LogClient::get_eth_type(uint64_t pkt_type) {
    if (PacketType(pkt_type) == PacketType::append) {
	spdlog::debug("Eth type is ETH_APPEND_REQ.");
        return ETH_APPEND_REQ;
    }
    spdlog::critical("No ethernet type found!");
    return -1; // no ethernet type found
}
