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

LogClient::LogClient(std::string input_file) {

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
				   mac_addrs,
				   this->num_work_threads);
    cid = get_cli_id(config);
    set_spdlog_level(get_log_level(config));
    spdlog::info("Pringles Client: Only Append being tested");
    
    // Start receiving thread 
    for (size_t i = 0; i < num_pkt_types; i++) {
        this->pkt_q.insert(std::pair<PacketType, std::queue<std::unique_ptr<char[]>>>(PacketType(i), std::queue<std::unique_ptr<char[]>>()));
    }
    //this->recv_thread = std::thread(&LogClient::pringles_recv_queue, this);

    // Protocol types
    this->seq = SequencerType(get_sequencer_type(config));
    //this->stor = StorageType(get_storage_type(config));

    // Updating the log 
    cached_log_entries = {};

    message_available = false;

    min_matching_acks = get_num_failures(config) + 1;
    append_nonce_idx_map = {};
    append_ack_map = {};
    this->started_append = false; 
    //this->append_thread = std::thread(&LogClient::run_append, this); 
    this->num_ready_bytes = 0;
    this->dummy_idx = 1;

    this->payload_size = get_payload_size(config);
    this->batch_size = num_work_threads * payload_size;
    spdlog::debug("Batch size: {}", batch_size);
    this->stat = std::make_unique<Stats>(get_batch_size(config), get_batch_on(config), get_json_name(config), 0); // TODO thread_id?
    this->max_duration = get_experiment_duration(config);

    for (uint64_t i = 0; i < num_work_threads; i++) {
             cli_threads.emplace_back(std::thread(&LogClient::execute, this, i));	
    }
    spdlog::debug("But we must be getting here?");
    for (uint64_t i = 0; i < num_work_threads; i++) {
             recv_threads.emplace_back(std::thread(&LogClient::pringles_recv_queue, this));	
    }
    spdlog::debug("We're not finishing the constructor are we?");
}

LogClient::~LogClient() {
    batch_ready_cond.notify_all();
    //append_cond.notify_all();
    //append_thread.join();
    spdlog::debug("Append thread joined!"); 
    //recv_thread.join();
    spdlog::debug("Received thread joined!");
    //TODO subscribe_thread.join();
    for (uint64_t i = 0; i < num_work_threads; i++) {
        recv_threads[i].join();	
    }
    for (uint64_t i = 0; i < num_work_threads; i++) {
        cli_threads[i].join();
    }
    

    spdlog::debug("Joined the client threads!");
    net->done();
    stat->getAvgLatency();
    stat->getThroughput(max_duration);
    stat->getTotalOps();
    stat->exportResultsToJson();
}

void LogClient::execute(uint64_t thread_id) {
    spdlog::debug("At the beginning of execution here!");	
    spdlog::critical("Execute thread starting with TID = {}", gettid());
    std::string payload(payload_size, 'X');
    uint64_t cnt = 0;
    while (experiment_status()) {	    
 	uint32_t idx = append(payload);
        spdlog::debug("The entry was given index: {}", idx);
	cnt += 1;
    }
    spdlog::critical("Total number of sent appends (NOT necessarily successful): {} from thread {}", cnt, thread_id);
}

/* REAL Custom function */
/*uint32_t LogClient::append(std::string entry) { // TODO need to implement retry timeout

     spdlog::debug("At the beginning of the append!");
     std::string output;
     uint32_t nonce = generate_nonce();
     stat->startLatTimer(nonce);
     
     ringclient::Payload p;
     p.set_packet_type(static_cast<int>(PacketType::append));
     p.set_nonce(nonce);
     spdlog::debug("Nonce: {}", p.nonce());
     ringclient::AppendEntry* app = p.mutable_append();
     app->set_entry(entry);
     p.SerializeToString(&output);
     
     num_ready_bytes_lock.lock();
     //append_entries_lock.lock();
     append_entries.push_back(output);
     append_nonce_idx_map.insert(std::pair<int64_t,int64_t>(nonce, -1)); // Perhaps there was a race condition? TODO
     //append_entries_lock.unlock();


     spdlog::debug("Num ready bytes b4: {}", num_ready_bytes);
     num_ready_bytes += output.length();
     spdlog::debug("Num ready bytes after: {}", num_ready_bytes);
     num_ready_bytes_lock.unlock();
     //spdlog::debug("NOTIFYING THE CONDITION VARIABLE");
     batch_ready_cond.notify_one();
     spdlog::debug("New append with nonce : {}", nonce);
     
     {
     	 std::unique_lock<std::mutex> lock(next_idx_lock);
     	 append_cond.wait(lock, [this, nonce] {
     	   	     return (append_nonce_idx_map[nonce] >= 0) || end_thread;
     	 });
     	 message_available = false; // TODO irrelevant
     }
     if (append_nonce_idx_map[nonce] <= 0) {
     	    spdlog::critical("!!!!!! INCORRECT INDEX for nonce {}, NOT BEING COUNTED NEEDS TO BE HANDLED", nonce);
     	    //batch_ready_cond.notify_all();
     	    return 0;
     }
     if(stat->endLatTimer(nonce)) {
     	   stat->addOp();
     }
     spdlog::debug("Successfully got index for nonce {}", nonce);
     return append_nonce_idx_map[nonce];  
}*/

uint32_t LogClient::append(std::string entry) { // TODO need to implement retry timeout

     spdlog::debug("At the beginning of the append!");
     std::string output;
     uint32_t nonce = generate_nonce();
     stat->startLatTimer(nonce);
     
     ringclient::Payload p;
     p.set_packet_type(static_cast<int>(PacketType::append));
     p.set_nonce(nonce);
     spdlog::debug("Nonce: {}", p.nonce());
     ringclient::AppendEntry* app = p.mutable_append();
     app->set_entry(entry);
     p.SerializeToString(&output);
     
     spdlog::debug("New append with nonce : {}", nonce);
     // Create packet header	
     size_t size_of_hdr = get_size_of_hdr(PacketType::append);
     std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(generate_nonce(), cid); 
     switch (seq) // MOVE
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
     spdlog::debug("BEFORE WHILE LOOP IN APPEND THREAD");

     size_t packet_size = size_of_hdr;
     uint64_t allocated_packet_size = size_of_hdr + output.length() + 1;
     std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
     hdr.get()->payload_size = output.length();	
     hdr.get()->num_entries = 1; //append_entries.size();

     memcpy(packet.get(), reinterpret_cast<const char*>(hdr.get()), size_of_hdr);
     memcpy(packet.get() + packet_size, output.data(), output.length());
     packet_size += output.length();

     packet[packet_size] = '\0';
     packet_size += 1;

     spdlog::debug("Adding append packet to send queue of size {} and header size {} vs allocated size: {}", packet_size, size_of_hdr, allocated_packet_size);
     	
     append_nonce_lock_map.emplace(std::piecewise_construct, std::forward_as_tuple(hdr.get()->nonce), std::forward_as_tuple());
     append_cond_map.emplace(std::piecewise_construct, std::forward_as_tuple(hdr.get()->nonce), std::forward_as_tuple());
     {
		//std::unique_lock<std::mutex> lock(append_nonce_lock_map[hdr.get()->nonce]);		
		std::unique_lock<std::mutex> lock(next_idx_lock);		
		append_nonce_idx_map.insert(std::pair<int64_t,int64_t>(nonce, -1)); // Perhaps there was a race condition? TODO

     }
	TbbStringMap::accessor a;
	if (append_ack_map.insert(a, hdr.get()->nonce)) {
		//append_ack_map.insert(std::pair<int64_t, std::unordered_map<uint64_t, uint64_t>>(hdr.get()->nonce, {}));
		a->second = {};
	}
     	spdlog::debug("The nonce of the batch is : {} and the num of entries: {}", hdr.get()->nonce, hdr.get()->num_entries);
     	stat->startLatTimer(hdr.get()->nonce);
	net->add_to_send_queue(std::move(packet), static_cast<int>(PacketType::append), packet_size);
     
     {
     	 std::unique_lock<std::mutex> lock(append_nonce_lock_map[hdr.get()->nonce]);
     	 //std::unique_lock<std::mutex> lock(next_idx_lock);
     	 append_cond_map[hdr.get()->nonce].wait(lock, [this, nonce] {
     	   	     return (append_nonce_idx_map[nonce] >= 0) || end_thread;
     	 });
     	 //message_available = false; // TODO irrelevant
     }
     if (append_nonce_idx_map[nonce] <= 0) {
     	    spdlog::critical("!!!!!! INCORRECT INDEX for nonce {}, NOT BEING COUNTED NEEDS TO BE HANDLED", nonce);
     	    //batch_ready_cond.notify_all();
     	    return 0;
     }
     if(stat->endLatTimer(nonce)) {
     	   stat->addOp();
     }
     spdlog::debug("Successfully got index for nonce {}", nonce);
     return append_nonce_idx_map[nonce];  
}

void LogClient::run_append() {
     std::chrono::nanoseconds MAX_BATCH_WAIT_NS(100); // TODO PUT IN YAML
     auto wait_start = std::chrono::steady_clock::now();
     auto wait_end = wait_start + MAX_BATCH_WAIT_NS;
    
     spdlog::critical("Run Append Thread starting with TID = {}", gettid());
     // Create packet header	
     size_t size_of_hdr = get_size_of_hdr(PacketType::append);
     std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(generate_nonce(), cid); 
     switch (seq) // MOVE
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
     spdlog::debug("BEFORE WHILE LOOP IN APPEND THREAD");

     while (!end_thread) { 
        std::unique_ptr<char[]> packet; 
	size_t packet_size = size_of_hdr;
	uint64_t allocated_packet_size = 0;
	// TODO YOU NEED A COND VARIABLE
	{
            std::unique_lock<std::mutex> lock(num_ready_bytes_lock); // TODO: PER QUEUE LOCK
            batch_ready_cond.wait(lock, [&, this] {
                return (num_ready_bytes > batch_size) || end_thread;        
            });
	    spdlog::debug("MADE IT PAST THE CONDITIONAL");
	    if (end_thread) {
		    break;
	    }
            
	// NEW

	// CORRECT num_ready_bytes_lock.lock(); // CRITICAL SECTION START TODO
	/*if (num_ready_bytes == 0 num_ready_bytes < batch_size) { // CHANGE TO TIMEOUT TODO
            // CORRECT num_ready_bytes_lock.unlock();
	    continue;
	}	*/

        /*if (num_ready_bytes >= batch_size) { SOMETHING WRONG WITH THIS LOGIC AND num_ready_bytes == 0
	} else if (std::chrono::steady_clock::now() >= wait_end && num_ready_bytes > 0) { // TODO make this an atomic
	} else {

	    continue;
	}*/
	//num_ready_bytes_lock.lock(); // CRITICAL SECTION START TODO TEST
	allocated_packet_size = size_of_hdr + num_ready_bytes + 1;
	num_ready_bytes = 0;
	} // TEMP TODO
	packet = std::make_unique<char[]>(allocated_packet_size);
	hdr.get()->payload_size = append_entries[0].length();
	hdr.get()->num_entries = append_entries.size();
	

     	memcpy(packet.get(), reinterpret_cast<const char*>(hdr.get()), size_of_hdr);

	spdlog::debug("Past the condition variable! {}", append_entries.size());
	for (uint64_t i = 0; i < append_entries.size(); /*i++*/) {
	    std::string output = append_entries[i];
	    if ((packet_size + output.length()) > allocated_packet_size) {
	        break;
	    }
     	    memcpy(packet.get() + packet_size, output.data(), output.length());
     	    packet_size += output.length();
	    append_entries.erase(std::next(append_entries.begin(), i), std::next(append_entries.begin(), i+1));
	}
	spdlog::debug("Packet size: {}, Num_entries: {}", packet_size, append_entries.size());
	packet[packet_size] = '\0';
	packet_size += 1;

     	spdlog::debug("Adding append packet to send queue of size {} and header size {} vs allocated size: {}", packet_size, size_of_hdr, allocated_packet_size);
     	
     	//next_idx_lock.lock(); // TODO check if this is safe behaviour?
        //next_idx_lock.unlock();
	//append_entries.clear();
    // } TODO
	//std::pair<uint64_t, std::map<uint64_t, uint64_t>> idx_pair = {0, {}};
     	append_ack_map.insert(std::pair<int64_t, std::unordered_map<uint64_t, uint64_t>>(hdr.get()->nonce, {}));
     	spdlog::debug("The nonce of the batch is : {} and the num of entries: {}", hdr.get()->nonce, hdr.get()->num_entries);
     	stat->startLatTimer(hdr.get()->nonce);
	net->add_to_send_queue(std::move(packet), static_cast<int>(PacketType::append), packet_size);
	wait_start = std::chrono::steady_clock::now();
     	wait_end = wait_start + MAX_BATCH_WAIT_NS;
     }
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

void LogClient::pringles_recv_queue() {
    // initialize - for each packet type, receive queue
    spdlog::critical("Client Recv Pringles Thread starting with TID = {}", gettid());
    uint64_t cnt = 0;
    uint64_t total_cnt = 0;
    while (true) {
	if (end_thread) {
	    for (auto it = append_cond_map.begin(); it != append_cond_map.end(); ++it) {
	        (it->second).notify_all();
	    }
	    break;
	}
	char* recv_ptr = net->read_from_recv_queue(); // will receive the full packet, including Eth header
	if (!recv_ptr) {
	    continue;
	}
	struct ethhdr* eth = (struct ethhdr*)recv_ptr;
	if (ntohs(eth->h_proto) == ETH_APPEND_REQ) { // TODO Move this to run_append? CHANGE TO ETH APPEND RESP?
		total_cnt += 1;
    		struct ring_append_entry* append_entry = (struct ring_append_entry*)(recv_ptr + sizeof(struct ethhdr) + sizeof(struct iphdr));
		spdlog::debug("The nonce received is : {}", append_entry->nonce);
		{
		 std::unique_lock<std::mutex> lock(append_nonce_lock_map[append_entry->nonce]);		
		 spdlog::debug("Append entry NONCE is {}", append_entry->nonce);
		 spdlog::debug("Map size: {}", append_ack_map.size());
		 TbbStringMap::const_accessor a;
		 if (!append_ack_map.find(a, append_entry->nonce)) {
		    spdlog::debug("SKIPPING THIS NONCE {}", append_entry->nonce);
		    continue;
		 }

		 //std::unique_lock<std::mutex> lock(next_idx_lock);		
		 //std::unique_lock<std::mutex> lock(next_idx_lock);
		 //append_ack_map[append_entry->nonce].first += 1;
		 if (append_ack_map[append_entry->nonce].find(append_entry->g_idx)) {
		     append_ack_map[append_entry->nonce][append_entry->g_idx] += 1;
		 } else {
		     next_idx_lock.lock();
		     append_ack_map[append_entry->nonce].insert(std::pair<uint64_t, uint64_t>(append_entry->g_idx, 1));
		     next_idx_lock.unlock();
		 }
		     
		 if (append_ack_map[append_entry->nonce][append_entry->g_idx] >= min_matching_acks) {
			/*** FILL IN APPEND NONCE MAP ****/
	    		spdlog::debug("Payload size: {} and index {} and cid {} and nonce {} and num entries {}", append_entry->payload_size, append_entry->g_idx, append_entry->cid, append_entry->nonce, append_entry->num_entries);
			size_t offset = sizeof(struct ethhdr) + sizeof(struct iphdr) + get_size_of_hdr(PacketType::append);
			for (size_t i = 0; i < append_entry->num_entries; i++) {
			    char* inner_pkt = (char*)(recv_ptr + offset);
	          	    ringclient::Payload payload;
	    		    payload.ParseFromArray(inner_pkt, append_entry->payload_size);
			    spdlog::debug("THE PAYLOAD NONCE IS: {}", payload.nonce());
			    if (append_nonce_idx_map.count(payload.nonce()) > 0) {
				append_nonce_idx_map[payload.nonce()] = append_entry->g_idx + i;
				spdlog::debug("The value put in the map is: {}", append_nonce_idx_map[payload.nonce()]);
			    } else {
			        spdlog::critical("NO RECORD OF THIS ENTRY???");
				append_nonce_idx_map[payload.nonce()] = 0;
			    }
			    offset += append_entry->payload_size;
		  	    cnt += 1;
			}
			spdlog::debug("DONE WITH ACKS For nonce {}, we got idx {}, which got {} matching acks and ?? acks overall", append_entry->nonce, append_entry->g_idx, min_matching_acks);

		  	message_available = true;
     		 } else {  // TODO need to do a timeout in case this never happens
		     message_available = false;
		 }
		}
		next_idx_lock.lock();
		if (message_available) {
			append_ack_map.erase(append_entry->nonce);
		}
		next_idx_lock.unlock();
		append_cond_map[append_entry->nonce].notify_all();
	}
	free(recv_ptr);
	// Read
	// Tail
	// Subscribe
	// Trim
    }
    spdlog::critical("Client Recv Pringles Thread received {} acks and {} replies for appends on TID = {}", total_cnt, cnt, gettid());
}

/* Experiment Logistics */
void LogClient::wait_to_finish() {
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
