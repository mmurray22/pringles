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
    cid = cli_id; // get_cli_id(config);
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
    this->stat = std::make_unique<Stats>(get_batch_size(config), get_batch_on(config), get_json_name(config), thread_id);
    message_available = false;

    min_matching_acks = get_num_failures(config) + 1;
    append_nonce_idx_map = {};
    append_ack_map = {};

/*    this->num_work_threads = get_num_client_threads(config);
    this->payload_size = get_payload_size(config);
    this->batch_size = num_work_threads * payload_size;
    std::vector<std::thread> cli_threads;
    for (uint64_t i = 0; i < num_work_threads; i++) {
             cli_threads.emplace_back(std::thread(&LogClient::execute, i));	
    }
  */ 
    this->max_duration = get_experiment_duration(config);
    this->duration_thread = std::thread(&LogClient::wait_for_finish, this);
}

/*void LogClient::execute(uint64_t thread_id) {
    std::string payload(payload_size, 'X');
    uint64_t cnt = 0;
    while (pringles_client.experiment_status()) {	    
 	uint32_t idx = pringles_client.append(payload);
        spdlog::debug("The entry was given index: {}", idx);
	cnt += 1;
    }
    spdlog::critical("Total number of sent appends (NOT necessarily successful): {}", cnt);
}*/


LogClient::~LogClient() {
    duration_thread.join();
    spdlog::debug("Duration thread joined!"); 
    recv_thread.join();
    spdlog::debug("Received thread joined!");
    //TODO subscribe_thread.join();
    /*for (uint64_t i = 0; i < num_work_threads; i++) {
        cli_threads[i].join();
    }*/

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

     //spdlog::debug("Creating append packet!");
     
     // Create packet payload
     ringclient::AppendEntry* app = p->mutable_append();
     
     app->set_entry(entry);
     p->SerializeToString(&output);
     packet_size += output.length();
     
     // Create packet header
     size_t size_of_hdr = get_size_of_hdr(PacketType::append);
     packet_size += size_of_hdr;
     std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(generate_nonce(), 1, output.length());

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
     //spdlog::debug("Adding append packet to send queue of size {} with payload {} and header size {}", packet_size, output.length(), size_of_hdr);
     
     next_idx_lock.lock();
     std::pair<uint64_t, std::map<uint64_t, uint64_t>> idx_pair = {0, {}};
     append_nonce_idx_map.insert(std::pair<int64_t,uint64_t>(hdr.get()->nonce, 0));
     append_ack_map.insert(std::pair<int64_t, std::pair<uint64_t, std::map<uint64_t, uint64_t>>>(hdr.get()->nonce, idx_pair));
     next_idx_lock.unlock();
     
     stat->startLatTimer(hdr.get()->nonce);
     spdlog::debug("The nonce sent is : {}", hdr.get()->nonce);
     net->add_to_send_queue(std::move(packet), static_cast<int>(PacketType::append), packet_size);
     
     {
      std::unique_lock<std::mutex> lock(next_idx_lock);
      message_cond.wait(lock, [this] {
		     return message_available || end_thread;
      });
      message_available = false;
     }
     if (append_nonce_idx_map[hdr.get()->nonce] == 0) {
         spdlog::critical("!!!!!! INCORRECT INDEX for nonce {}, NOT BEING COUNTED NEEDS TO BE HANDLED", hdr.get()->nonce); // TODO
	 return 0;
     }
     if(stat->endLatTimer(hdr.get()->nonce)) {
	stat->addOp();
     }
     spdlog::debug("Successfully got index for nonce {}", hdr.get()->nonce);
     return append_nonce_idx_map[hdr.get()->nonce];  
}

std::string LogClient::read(uint64_t/*uint32_t*/ idx) {
	/*
	 std::unique_ptr<ringclient::Payload> p = std::make_unique<ringclient::Payload>();
         std::string output;
     	std::unique_ptr<char[]> packet;
     	size_t packet_size = 0;
    
     	// General packet information
     	p->set_packet_type(static_cast<int>(PacketType::read));

     	spdlog::debug("Creating read packet!");
     
     	// Create packet payload
     	ringclient::ReadEntry* rd = p->mutable_read();
     	p->SerializeToString(&output);
     	packet_size += output.length();
     
     	// Create packet header
     	size_t size_of_hdr = get_size_of_hdr(PacketType::read);
     	packet_size += size_of_hdr;
     	std::unique_ptr<struct ring_read_entry> hdr = create_ring_read_entry(generate_nonce(), 1, output.length(), idx);

     	packet = std::make_unique<char[]>(packet_size+1);
     
     	// Construct packet
     	memcpy(packet.get(), reinterpret_cast<const char*>(hdr.get()), size_of_hdr);
     	memcpy(packet.get() + size_of_hdr, &output, output.length());
     	packet[packet_size] = '\0';
     	spdlog::debug("Adding read packet to send queue of size {} with payload {} and header size {}", packet_size, output.length(), size_of_hdr);
     
	next_entry_lock.lock();
     	read_nonce_idx_map.insert(std::pair<uint64_t, uint64_t>(hdr.get()->nonce, 0));
	next_entry_lock.unlock();
     
     	stat->startLatTimer(hdr.get()->nonce);
     	spdlog::debug("The read nonce sent is : {}", hdr.get()->nonce);
     	net->add_to_send_queue(std::move(packet), static_cast<int>(PacketType::read), packet_size);
     
        while (!end_thread) {
        {
           std::unique_lock<std::mutex> lock(next_read_lock);
           message_cond.wait(lock, [this] {
		     return message_available;
           });
           message_available = false;
        }
        if (read_nonce_idx_map[hdr.get()->nonce].length() == 0) {
         continue;
        }
        return read_nonce_idx_map[hdr.get()->nonce];  
        }
	std::string default_str = "";
        return default_str;
	 */
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
    spdlog::critical("Client Recv Pringles Thread starting with TID = {}", gettid());
    uint64_t cnt = 0;
    uint64_t total_cnt = 0;
    while (true) {
	if (end_thread) {
	    message_cond.notify_all();
	    break;
	}
	std::unique_ptr<char[]> recv_ptr = net->read_from_recv_queue(); // will receive the full packet, including Eth header
	if (!recv_ptr) {
	    continue;
	}
	struct ethhdr* eth = (struct ethhdr*)recv_ptr.get();
	if (ntohs(eth->h_proto) == ETH_APPEND_REQ) {
		total_cnt += 1;
    		struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr.get() + sizeof(struct ethhdr) + sizeof(struct iphdr));
		//spdlog::debug("The nonce received is : {}", append_entry->nonce);
		if (append_nonce_idx_map.count(append_entry->nonce) == 0) {
		    spdlog::debug("SKIPPING THIS NONCE {}", append_entry->nonce);
		    continue;
		}
		{
		 std::unique_lock<std::mutex> lock(next_idx_lock);
	         if (append_ack_map.count(append_entry->nonce)) {
		     append_ack_map[append_entry->nonce].first += 1;
		     if (append_ack_map[append_entry->nonce].second.count(append_entry->g_idx)) {
			 append_ack_map[append_entry->nonce].second[append_entry->g_idx] += 1;
		     } else {
		         append_ack_map[append_entry->nonce].second.insert(std::pair<uint64_t, uint64_t>(append_entry->g_idx, 1));
		     }
		     
		 }
		 if (append_ack_map[append_entry->nonce].second[append_entry->g_idx] >= min_matching_acks) {
			append_nonce_idx_map[append_entry->nonce] = append_entry->g_idx;
			//spdlog::debug("For nonce {}, we got idx {}, which got {} matching acks and {} acks overall", append_entry->nonce, append_entry->g_idx, min_matching_acks, append_ack_map[append_entry->nonce].first);
			append_ack_map.erase(append_entry->nonce);
		  	message_available = true;
			cnt += 1;
     		 }  // TODO need to do a timeout in case this never happens
		}
		message_cond.notify_all();
	}
    }
    spdlog::critical("Client Recv Pringles Thread received {} acks and {} replies for appends on TID = {}", total_cnt, cnt, gettid());
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
