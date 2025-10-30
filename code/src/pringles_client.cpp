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
LogClient::LogClient(std::string input_file, uint64_t cli_id, uint64_t thread_id) {

   // Get packet types for sending/receiving

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
    cid = cli_id;
    set_spdlog_level(get_log_level(config));
    spdlog::info("Pringles Client: Only Append being tested");
    
    // Start receiving thread 
    for (size_t i = 0; i < num_pkt_types; i++) {
        this->pkt_q.insert(std::pair<PacketType, std::queue<std::unique_ptr<char[]>>>(PacketType(i), std::queue<std::unique_ptr<char[]>>()));
    }
    this->recv_thread = std::thread(&LogClient::pringles_recv_queue, this);

    // Protocol types
    this->seq = SequencerType(get_sequencer_type(config));
    //this->stor = StorageType(get_storage_type(config));

    // Updating the log 
    cached_log_entries = {};
    this->max_duration = get_experiment_duration(config);
    this->duration_thread = std::thread(&LogClient::wait_for_finish, this);

    this->stat = std::make_unique<Stats>(get_batch_size(config), get_batch_on(config), get_json_name(config), thread_id);
}

LogClient::~LogClient() {
    duration_thread.join();
    spdlog::debug("Duration thread joined!"); 
    recv_thread.join();
    spdlog::debug("Received thread joined!");
    //TODO subscribe_thread.join();

    spdlog::debug("Joined the client threads!");
    net->done();
    stat->getAvgLatency();
    stat->getThroughput(max_duration);
    stat->getTotalOps();
    stat->exportResultsToJson();
}

/* Custom function */
uint32_t LogClient::append(std::string entry) {
     std::unique_ptr<ringclient::Payload> p = std::make_unique<ringclient::Payload>();
     std::string output;
     std::unique_ptr<char[]> packet;
     size_t packet_size = 0;
    
     // General packet information
     p->set_packet_type(static_cast<int>(PacketType::append));

     spdlog::debug("Creating append packet!");
     
     // Create packet payload
     ringclient::AppendEntry* app = p->mutable_append();
     
     app->set_entry(entry);
     //p->set_allocated_append(app);
     p->SerializeToString(&output);
     packet_size += output.length();
     
     // Create packet header
     size_t size_of_hdr = get_size_of_hdr(PacketType::append);
     packet_size += size_of_hdr;
     std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(generate_nonce() /*nonce*/, 1, output.length());
     next_nonce_lock.lock();
     next_nonce = hdr.get()->nonce; 
     next_nonce_lock.unlock();

     switch (seq)
     {
	case SequencerType::DUMMY: 
	{
	    hdr.get()->g_idx = 1;
	    break;
	}; 
	default:
	{
            hdr.get()->g_idx = 0; // To be filled in by the switch
	};
     }
     packet = std::make_unique<char[]>(packet_size+1);
     
     // Construct packet
     memcpy(packet.get(), reinterpret_cast<const char*>(hdr.get()), size_of_hdr);
     memcpy(packet.get() + size_of_hdr, &output, output.length());
     packet[packet_size] = '\0';
     spdlog::debug("Adding append packet to send queue of size {} with payload {} and header size {}", packet_size, output.length(), size_of_hdr);
     stat->startLatTimer(hdr.get()->nonce);
     net->add_to_send_queue(std::move(packet), static_cast<int>(PacketType::append), packet_size);
     uint32_t idx = 0; 
     /*{
     std::unique_lock<std::mutex> lock(next_idx_lock);
     message_cond.wait(lock, [this] {
		     return message_available;
     });
     idx = next_idx; 
     message_available = false;
     }*/
     //idx = wait_for_append(PacketType::append, hdr.get()->nonce);  
     return idx;
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
    // subscribe_thread_running  = true <-- TODO add this in!
}

void LogClient::wait_for_subscribe(uint64_t idx, uint64_t pkt_type) {
     while (true) {
	if (end_thread) {
            spdlog::debug("!!!!!!!!!!!!!!TIME to stop");
            break;
        }

	if (pkt_q[PacketType(pkt_type)].empty()) { // TODO: condition variable nothing in the receive queue, so we sleep
             continue;
        }
        struct ring_subscribe_entry* subscribe_pkt = (struct ring_subscribe_entry*)(pkt_q[PacketType(pkt_type)].front().get());
	uint64_t recv_idx = subscribe_pkt->g_idx;
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
uint32_t LogClient::wait_for_append(PacketType pkt_type, uint32_t nonce) {
    pkt_q_lock.lock();	
    while (true) {
        if (end_thread) {
            spdlog::debug("!!!!!!!!!!!!!!TIMED OUT, packet with nonce {} never received", nonce);
            break;
        }
        if (pkt_q[pkt_type].empty()) {
	   continue;
	}

        struct ring_append_entry* append_entry = (struct ring_append_entry*)(pkt_q[pkt_type].front().get());
	pkt_q[pkt_type].pop();
	
	spdlog::debug("Payload size: {} and index {} and cid {} and nonce {}", append_entry->payload_size, append_entry->g_idx, append_entry->cid, append_entry->nonce);
	// Parse out header
	uint32_t idx = 0;
	if (append_entry->nonce != nonce) {
	    spdlog::debug("Mismatching nonce!");
	} else {
	    idx = append_entry->g_idx;
	}
	stat->endLatTimer(nonce);
	stat->addOp();
	spdlog::critical("!!!!!!!!!!!SUCCESSFULLY GOT THE INDEX: {}", append_entry->g_idx);
	return idx; //append_entry->g_idx;

	// Parse out packet contents (if needed)
    }
    return 0;
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
	std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(generate_nonce(), 1, output.length());
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
    //uint32_t nonce = 0;
    spdlog::critical("Recv Pringles Thread starting with TID = {}", gettid());
    while (true) {
	if (end_thread) {
	    break;
	}
	/*next_nonce_lock.lock();
	nonce = next_nonce;
	next_nonce_lock.unlock();*/
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
		/*uint64_t size_of_hdr = get_size_of_hdr(PacketType::append);
		std::unique_ptr<char[]> packet = std::make_unique<char[]>(size_of_hdr);*/
    		struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr.get() + sizeof(struct ethhdr) + sizeof(struct iphdr));
		if(stat->endLatTimer(append_entry->nonce)) {
			stat->addOp();
		}

		/*next_idx_lock.lock();
		next_idx = append_entry->g_idx;
		message_available = true;
		next_idx_lock.unlock();
		message_cond.notify_all();*/
     		/*memcpy(packet.get(), reinterpret_cast<const char*>(append_entry), size_of_hdr);
		pkt_q_lock.lock();
	        pkt_q[PacketType::append].push(std::move(packet));
		pkt_q_lock.unlock();*/
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
