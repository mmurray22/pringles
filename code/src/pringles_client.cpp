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
#include <atomic>

#include "ring_headers.h"
#include "ringclient.pb.h"
#include "pringles_client.h"
#include "utils.h"
#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"
#define RECEIVE_PORT 3149

LogClient::LogClient(std::string input_file, uint64_t thread_id) {
   // Get packet types for sending/receiving
   YAML::Node config = YAML::LoadFile(input_file);
   num_pkt_types = get_num_pkt_types(config);
   std::vector<std::array<uint8_t, 6>> mac_addrs = get_dst_mac_addrs(config);
   if (mac_addrs.size() < 1) {
       spdlog::critical("Unable to parse mac address!");
       throw;
   }
  
   this->num_work_threads = get_num_client_threads(config);
   // Create network
   bool run_threads = false;
   net = std::make_unique<Network>(get_send_port(config), 
                                   get_recv_port(config),
				   get_socket_type(config),
                                   get_log_level(config),
				   get_batch_size(config),
				   get_batch_on(config),
				   get_interface(config),
				   get_self_ip(config),
				   get_num_pkt_types(config),
				   run_threads);
    cid = get_cli_id(config);
    set_spdlog_level(get_log_level(config));
    spdlog::info("Pringles Client: Only Append being tested");
    
    // Start receiving thread 
    for (size_t i = 0; i < num_pkt_types; i++) {
        this->pkt_q.insert(std::pair<PacketType, std::queue<std::unique_ptr<char[]>>>(PacketType(i), std::queue<std::unique_ptr<char[]>>()));
    }

    // Protocol types
    this->seq = SequencerType(get_sequencer_type(config));

    // Updating the log 
    cached_log_entries = {};
    message_available = false;

    min_matching_acks = get_num_failures(config) + 1;
    append_nonce_idx_map = {};
    append_ack_map = {};
    this->started_append = false; 
    this->num_ready_bytes = 0;
    this->dummy_idx = 1;

    this->payload_size = get_payload_size(config);
    this->batch_size = num_work_threads * payload_size;
    spdlog::debug("Batch size: {}", batch_size);
    this->stat = std::make_unique<Stats>(get_batch_size(config), get_batch_on(config), get_json_name(config), thread_id);
    this->max_duration = get_experiment_duration(config);
    this->warm_up = get_warm_up(config);
    this->cool_down = get_cool_down(config);
    this->global_thread_id = thread_id;
   
    this->switch_mac = get_switch_mac(config);
    this->switch_ip = get_switch_ip(config); 

    //this->duration_thread = std::thread(&LogClient::wait_to_finish, this);
    this->execution_thread = std::thread(&LogClient::execute, this, thread_id);
    pthread_t native_handle = this->execution_thread.native_handle();

    // Create a CPU set and add the desired core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(thread_id, &cpuset); // Pin to core 'i'

    // Set thread affinity
    int result = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpuset);
    if (result != 0) {
        std::cerr << "Error setting thread affinity for thread " << this->execution_thread.get_id() << ": " << result << std::endl;
    }

    collect_stats = false;
}

LogClient::~LogClient() {
    spdlog::debug("Append thread joined!"); 
    spdlog::debug("Received thread joined!");
    //TODO subscribe_thread.join();
    /*for (uint64_t i = 0; i < num_work_threads; i++) {
        recv_threads[i].join();	
    }*/
    //duration_thread.join(); 
    execution_thread.join();
    spdlog::debug("Joined the client threads!");
    net->done();
}

void LogClient::execute(uint64_t thread_id) {
    spdlog::debug("At the beginning of execution here!");	
    spdlog::critical("Execute thread starting with TID = {}", gettid());
    std::string payload(payload_size, 'X');
    uint64_t cnt = 0;
    while (experiment_status()) {	    
 	uint32_t idx = append(payload); // dummy(payload); //append(payload);
        spdlog::debug("The entry was given index: {}", idx);
	cnt += 1;
    }
    spdlog::critical("Total number of sent appends (NOT necessarily successful): {} from thread {}", cnt, thread_id);
}

uint32_t LogClient::dummy(std::string entry) { // TODO need to implement retry timeout
     (void) entry;
     spdlog::debug("At the beginning of the append!");
     uint32_t nonce = generate_nonce();
     if (collect_stats) {
         stat->startLatTimer(nonce); // TODO: Could do without the map?
     }

     // Create packet buffer which will be sent  
     uint64_t allocated_packet_size = 150;
     std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
     memset(packet.get(), 'x', allocated_packet_size);
     packet[allocated_packet_size - 1] = '\0';
     net->send_packet(std::move(packet), allocated_packet_size, static_cast<int>(PacketType::append), get_pkt_eth_types()[PacketType::append], switch_mac, switch_ip);
    
     bool got_quorum = false;
     /*
     std::chrono::nanoseconds MAX_BATCH_WAIT_NS(100); // TODO PUT IN YAML
     auto wait_start = std::chrono::steady_clock::now();
     auto wait_end = wait_start + MAX_BATCH_WAIT_NS;
     */
     while (!got_quorum/* && std::chrono::steady_clock::now() < wait_end*/) {
        char* recv_ptr = net->recv_packet();
	if (!recv_ptr) {
	    continue;
	}
	got_quorum = true;
	if (end_thread) {
	    break;
	}
     }

     if(collect_stats && stat->endLatTimer(nonce)) {
     	   stat->addOp();
           spdlog::debug("Successfully got index for nonce {}", nonce);
     }
     return 1;  
}


/* REAL Custom function */
uint32_t LogClient::append(std::string entry) { // TODO need to implement retry timeout

     spdlog::debug("At the beginning of the append!");
     
     uint32_t ret_idx = 0;
     uint32_t nonce = generate_nonce();
     if (collect_stats) {
         stat->startLatTimer(nonce); // TODO: Could do without the map?
     }
    
     // Create protobuf content  --> TODO: Remove
     std::string output;
     ringclient::Payload p;
     p.set_packet_type(static_cast<int>(PacketType::append));
     p.set_nonce(nonce);
     spdlog::debug("Nonce: {}", p.nonce());
     ringclient::AppendEntry* app = p.mutable_append();
     app->set_entry(entry);
     p.SerializeToString(&output);
     
     // Create packet header	
     size_t size_of_hdr = get_size_of_hdr(PacketType::append);
     std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(nonce, cid); 
     switch (seq)
     {
	case SequencerType::DUMMY: 
	{
	    hdr.get()->g_idx = dummy_idx;
	    break;
	}; 
	default:
	{
            hdr.get()->g_idx = 0; // To be filled in by the switch
	};
     }
     hdr.get()->payload_size = output.length();	
     hdr.get()->num_entries = 1;
    
     // Create packet buffer which will be sent  
     uint64_t allocated_packet_size = size_of_hdr + output.length() + 1;
     spdlog::debug("Append packet has header size {} and allocated size: {}", size_of_hdr, allocated_packet_size);
     std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
     
     memcpy(packet.get(), reinterpret_cast<const char*>(hdr.get()), size_of_hdr);
     memcpy(packet.get() + size_of_hdr, output.data(), output.length());
     packet[allocated_packet_size - 1] = '\0';
     	
     net->send_packet(std::move(packet), allocated_packet_size, static_cast<int>(PacketType::append), get_pkt_eth_types()[PacketType::append], switch_mac, switch_ip);
    
     spdlog::debug("Map size: {}", append_ack_map.size());

     bool got_quorum = false;
     /*
     std::chrono::nanoseconds MAX_BATCH_WAIT_NS(100); // TODO PUT IN YAML
     auto wait_start = std::chrono::steady_clock::now();
     auto wait_end = wait_start + MAX_BATCH_WAIT_NS;
     */
     while (!got_quorum/* && std::chrono::steady_clock::now() < wait_end*/) {
        char* recv_ptr = net->recv_packet();
	if (!recv_ptr) {
	    continue;
	}
	if (end_thread) {
	    break;
	}
	struct ethhdr* eth = (struct ethhdr*)recv_ptr;
	if (ntohs(eth->h_proto) != ETH_APPEND_REQ) { // TODO will need to handle receiving multiple different packet types
	    continue;
	}
    	struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr));
	spdlog::debug("The nonce received is : {}", append_entry->nonce);
	if (append_entry->nonce != nonce) {
	    continue;
	}
	if (append_ack_map.count(append_entry->g_idx) > 0) {
	    append_ack_map[append_entry->g_idx] += 1;
	} else {
	    append_ack_map.insert(std::pair<uint64_t, uint64_t>(append_entry->g_idx, 1));
	}
		     
	if (append_ack_map[append_entry->g_idx] >= min_matching_acks) {
	    	spdlog::debug("Payload size: {} and index {} and cid {} and nonce {} and num entries {}", append_entry->payload_size, append_entry->g_idx, append_entry->cid, append_entry->nonce, append_entry->num_entries);
		size_t offset = sizeof(struct ethhdr) + sizeof(struct iphdr) + get_size_of_hdr(PacketType::append);
		char* inner_pkt = (char*)(recv_ptr + offset);
	        ringclient::Payload payload;
	    	payload.ParseFromArray(inner_pkt, append_entry->payload_size);
		spdlog::debug("THE PAYLOAD NONCE IS: {}", payload.nonce());
		ret_idx = append_entry->g_idx;
		spdlog::debug("DONE WITH ACKS For nonce {}, we got idx {}, which got {} matching acks and ?? acks overall", append_entry->nonce, append_entry->g_idx, min_matching_acks);
		got_quorum = true;
	} else {
	    continue;
	}
	spdlog::debug("Got to before the map erasure!");
	append_ack_map.clear();
	spdlog::debug("After the map erasure");
     }

     if(collect_stats && ret_idx != 0 && stat->endLatTimer(nonce)) {
     	   stat->addOp();
           spdlog::debug("Successfully got index for nonce {}", nonce);
     }
     return ret_idx;  
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

/* Experiment Logistics */
void LogClient::wait_to_finish() {
    collect_stats = true;
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    spdlog::debug("Collecting statistics!");
    stat->getAvgLatency();
    stat->getThroughput(max_duration);
    stat->getTotalOps();
    stat->exportResultsToJson();
    collect_stats = false;
} 

void LogClient::wait_to_warmup() {
    std::chrono::seconds sleep_duration(warm_up);
    std::this_thread::sleep_for(sleep_duration);
}

void LogClient::wait_to_cooldown() {
    std::chrono::seconds sleep_duration(cool_down);
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
