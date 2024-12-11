#include <thread>
#include "util.h"
#include "api.h"
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>


#define RECEIVE_PORT 3149

LogClient::LogClient(YAML::Node config) {
    config = std::make_unique<Config>();
	config->ip_addr = config["ip_addr"].as<std::string>();
	config->port = config["port"].as<uint64_t>();
	uint64_t new_socket = socket(AF_INET, SOCK_STREAM, 0);
	if (new_socket == -1) {
	    std::cerr << "Error creating socket" << std::endl;
        return;
    }
	config->cli_socket = new_socket;
    // Get config object
	// Initialize client variables
}

/* Custom function */
std::unique_ptr<std::string> create_entry(std::string input) {
    LogEntry entry;
    entry.set_allocated_entry(input);
    std::unique_ptr<std::string> output;
    entry.SerializeToString(output);
    return output;
}

std::string* construct_packet_header() {
	// Create AppendEntry header
    // Generate nonce TODO

    entry.SerializeToString(output);
    return output;
}

uint64_t append(std::unique_ptr<std::string> pkt_msg) {
    // Create append header
    // Create rest of packets
    // Send message over the network
    send_pkt.push(pkt_msg);
    int64_t idx = -1;
    while (!recv_append_pkt.has(hash(pkt_msg)) || (idx = recv_append_pkt.at(hash(pkt_msg))) >= 0) {
        sleep(20);
        continue;
    }
    return idx;
}

std::unique_ptr<std::string> read(uint64_t idx) {
	// Create ReadEntry packet
    send
}



