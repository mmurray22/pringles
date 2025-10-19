#include "pringles_client.h"
#include <thread>
#include "utils.h"
#include "api.h"
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#define RECEIVE_PORT 3149

LogClient::LogClient(std::string input_file, uint64_t cli_id) {

   // Get packet types for sending/receiving
   pkt_types = get_vec_of_packet_types();
   YAML::Node config = YAML::LoadFile(input_file);

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
				   pkt_types);
  
    client_id = get_client_id(cid);
    set_spdlog_level(get_log_level(config));
    spdlog::info("Pringles Client: Only Append being tested");
    
    // Start receiving thread 
    recv_thread(read_pringles_recv_queue);

    // Optional: Include trace
    //std::shared_ptr<Trace<std::string>> trace = std::make_shared<Trace<std::string>>(get_trace_file(config));
}

LogClient::~LogClient() {
    end_thread = true;
    recv_thread.join()
    subscribe_thread.join();
}

/* Custom function */
uint64_t LogClient::append(std::string entry) {
    int64_t nonce = *(generate_nonce().get());
    std::unique_ptr<char[]> pkt = create_pkt(PacketType::append, entry);
    //pending_append_entries(id(entry));
    net->add_to_send_queue(std::move(pkt), packet_types[PacketType::append]); // TODO: Need to create packet_types vector
    // wait until id(entry)
    int64_t id = wait_for_append(PacketType::append, nonce);
    if (id <= 0) {
        spdlog::critical("Packet wasn't processed - id negative!");
	return 0;
    }
    return id;
}

std::unique_ptr<std::string> LogClient::read(uint64_t idx) {
	// TODO
}

// Get latest committed entry
uint64_t LogClient::getTail() {
	// TODO later
}

// Subscribe to get all log updates after supplied index
// TODO: Add vector that the subscribed entries will go to?
void LogClient::subscribe(uint64_t idx) {
    subscribe_thread(&log_updates, idx);
}

void LogClient::log_updates(uint64_t idx) {
    // TODO 
}


// Garbage collect all log entries up to some index
bool LogClient::trim(uint64_t idx) {
	// TODO later
}

/*** Helper functions ***/
int64_t wait_for_append(PacketType pkt_type, int32_t nonce) {
    const auto start = std::chrono::steady_clock::now();
    while (true) {
        const std::chrono::duration<double, std::milli> elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed >= MAX_TIME || end_thread) {
            spdlog::debug("!!!!!!!!!!!!!!TIMED OUT, packet with nonce {} never received", nonce);
            break;
        }
	
        char* recv_pkt_with_hdr = pkt_q[pkt_type].pop();
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
	return append_entry->idx;

	// Parse out packet contents (if needed)
    }
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
std::unique_ptr<char[]> create_pkt(PacketType pkt_type, 
		                   int64_t nonce,
				   std::optional<std::string> entry = std::nullopt,
			   	   std::optional<int64_t> idx = 0) {
    ringclient::Payload p;
    std::unique_ptr<std::string> output;
    std::unique_ptr<char[]> packet;
    size_t packet_size = 0;
    
    // General packet information
    p.set_packet_type(packet_type_to_string(pkt_type));
    p.set_nonce(cid);

    if (pkt_type == PacketType::append) {
        spdlog::debug("Append packet!");
	ringclient::AppendEntry app;
	app.set_idx(-1); // To be filled in by the switch
	if (entry.has_value()) {
	    app.set_allocated_entry(entry.value());
	}
	p.set_allocated_append(app);
    	p.SerializeToString(output.get());
	
	packet_size += (*output.get()).size();
	size_t size_of_hdr = get_size_of_hdr(pkt_type);
        packet_size += size_of_hdr;
	std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(*(generate_nonce().get()), 0);

    	packet = std::make_unique<char[]>(packet_size);
	memcpy(packet.get(), hdr.get(), size_of_hdr);
        memcpy(packet.get() + size_of_hdr, output.get(), (*output.get()).size());

    } /*else if (pkt_type == PacketType::read) {
        spdlog::critical("Read packet!");
	ringclient::ReadEntry read;
	if (!idx.has_value()) {
	    return NULL;
	}
   	read.set_idx(idx.value());
	read.set_allocated_entry(NULL);
	p.set_allocated_read(read);
    }*/ else {
        spdlog::critical("Invalid packet type! No packet created.");
    }
    
    return packet;
}

void pringles_recv_queue() {
    // initialize - for each packet type, receive queue
    pkt_q = {};
    for (int i = 0; i < pkt_types.size(); i++) {
	pkt_q.insert(pkt_types[i], {});
    }
    while (true) {
	if (end_thread) {
	    break;
	}
	// TODO: Char[] vs string
	std::unique_ptr<char[]> recv_ptr = net->read_from_recv_queue(); // will receive the full packet, including Eth header
	if (!recv_ptr) {
	    continue;
	}
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
