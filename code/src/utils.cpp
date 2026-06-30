#include "utils.h"
#include "spdlog/spdlog.h"
#include <random>
#include <sstream>
#include <cstdint>
#include <cstdio>

/*Log Level*/
void set_spdlog_level(uint64_t log_level) {
    if (log_level == 1) { // Prints all log levels except trace
        spdlog::set_level(spdlog::level::debug);
        spdlog::debug("Log level set to: Debug");
    } else if (log_level == 2) { // Prints error and critical
        spdlog::set_level(spdlog::level::err);
        spdlog::error("Log level set to: Error");
    } else if (log_level == 3) { // Prints warn, error, and critical
        spdlog::set_level(spdlog::level::warn);
        spdlog::warn("Log level set to: Warn");
    } else if (log_level == 4) { // Prints all but debug and trace
        spdlog::set_level(spdlog::level::info);
        spdlog::debug("Log level set to: Info");
    } else if (log_level == 5) { // Prints all log levels
        spdlog::set_level(spdlog::level::trace);
        spdlog::debug("Log level set to: Trace");
    } else { // Prints only critical
        spdlog::set_level(spdlog::level::critical);
        spdlog::critical("Log level set to: Critical");
    }
}

uint64_t get_log_level(YAML::Node config) {
    return config["log_level"].as<uint64_t>();
}

void pin_current_thread_linux(int core_id) {
    // Create a CPU set structure and clear it
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    // Add the desired core to the CPU set
    CPU_SET(core_id, &cpuset);

    // Get the native handle of the current C++ thread
    pthread_t current_thread = pthread_self();

    // Set the affinity of the thread
    if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0) {
        spdlog::critical("Failed to set thread affinity");
    }
}

/* Nonce generation function */
uint32_t generate_nonce() {
    std::random_device rd;
    uint32_t nonce = rd();
    return nonce;
}

/* YAML processing functions */
uint64_t get_threads(YAML::Node config) {
    return config["send_threads"].as<uint64_t>();
}

uint64_t get_append_req_threads(YAML::Node config) {
    return config["append_req_threads"].as<uint64_t>();
}

/*Protocol type*/
std::string get_protocol_type(YAML::Node config) {
    return config["protocol_type"].as<std::string>();
}

/* Packet types and IP addrs */
std::string get_self_ip(YAML::Node config) {
    return config["self_ip"].as<std::string>();
}

uint64_t get_num_pkt_types(YAML::Node config) {
    return config["num_pkt_types"].as<uint64_t>();
}

std::map<uint64_t, std::vector<std::string>> get_packet_types(YAML::Node config) {
    std::map<uint64_t, std::vector<std::string>> pkt_type_to_ips = {};
    std::vector<std::string> ips = {};
    uint64_t num_pkt_types = config["num_pkt_types"].as<uint64_t>();
    for (uint64_t i = 0; i < num_pkt_types; i++) {
	std::vector<std::string> ips = config["packet_types"][i]["ips"].as<std::vector<std::string>>();
	for (std::string ip : ips) {
		spdlog::debug("IP addr: {}", ip);
	}
	pkt_type_to_ips.insert({i, ips});
    }
    return pkt_type_to_ips;
}

/* Batching */
bool get_batch_on(YAML::Node config) {
	bool batch_on = config["batch_on"].as<bool>();
	spdlog::debug("Batch on: {}", batch_on);
	return batch_on;
}

uint64_t get_batch_size(YAML::Node config) {
    return config["batch_size"].as<uint64_t>();
}

uint64_t get_batch_timeout(YAML::Node config) {
    return config["batch_usec_timeout"].as<uint64_t>();
}

/* Network Ports */
uint64_t get_send_port(YAML::Node config) {
    spdlog::debug("Port: {}", config["send_port"].as<uint64_t>());
    return config["send_port"].as<uint64_t>();
}

uint16_t get_recv_port(YAML::Node config) {
    spdlog::debug("Port: {}", config["recv_port"].as<uint16_t>());
    return config["recv_port"].as<uint16_t>();
}

/* Socket type */
std::string get_socket_type(YAML::Node config) {
    spdlog::debug("Socket type: {}", config["socket_type"].as<std::string>());
    return config["socket_type"].as<std::string>();
}

/* Trace file */
std::string get_trace_file(YAML::Node config) { // OPTIONAL
    if (config["trace_file"]) {
        return config["trace_file"].as<std::string>();
    }
    return "";
}

/// NEW ONES ///
std::string get_type(YAML::Node config) {
    return config["type"].as<std::string>();
}

std::string get_interface(YAML::Node config) {
    return config["interface"].as<std::string>();
}

uint64_t get_sequencer_type(YAML::Node config) {
    return config["sequencer_type"].as<uint64_t>();
}

uint64_t get_storage_type(YAML::Node config) {
    return config["storage_type"].as<uint64_t>();
}

uint64_t get_shard_id(YAML::Node config) {
    return config["shard_id"].as<uint64_t>();
}

uint64_t get_shard_switch_id(YAML::Node config) {
    return config["shard_switch_id"].as<uint64_t>();
}

/* Timeouts */
uint64_t get_read_timeout(YAML::Node config) {
    return config["read_timeout"].as<uint64_t>();
}

uint64_t get_write_timeout(YAML::Node config) {
    return config["write_timeout"].as<uint64_t>();
}

/* Run Duration */
uint64_t get_experiment_duration(YAML::Node config) {
    return config["experiment_duration"].as<uint64_t>();
}

/* Warm up */
uint64_t get_warm_up(YAML::Node config) {
    return config["warm_up"].as<uint64_t>();
}

/* Cool down */
uint64_t get_cool_down(YAML::Node config) {
    return config["cool_down"].as<uint64_t>();
}

uint64_t get_payload_size(YAML::Node config) {
    return config["payload_size"].as<uint64_t>();
}

std::vector<std::array<uint8_t, 6>> get_dst_mac_addrs(YAML::Node config) {
    std::vector<std::array<uint8_t, 6>> ret = {};
    uint64_t num_pkt_types = config["num_pkt_types"].as<uint64_t>();
    for (uint64_t i = 0; i < num_pkt_types; i++) {
        unsigned int bytes[6];
	std::vector<std::string> macs = config["packet_types_macs"][i]["macs"].as<std::vector<std::string>>();
	for (std::string mac : macs) {
		spdlog::debug("MAC addr: {}", mac);
		// Use sscanf to parse the hex values separated by colons.
	    	// %x reads a hexadecimal integer.
	    	int result = sscanf(mac.c_str(), "%x:%x:%x:%x:%x:%x",
	                        &bytes[0], &bytes[1], &bytes[2],
	                        &bytes[3], &bytes[4], &bytes[5]);
		uint8_t mac_array[6];
	    	if (result == 6) {
	        	// Cast the parsed unsigned ints back to uint8_t
	        	for (int i = 0; i < 6; ++i) {
	            		mac_array[i] = static_cast<uint8_t>(bytes[i]);
	        	}
	    	}	
		std::array<uint8_t, 6> mac_final_form;
	
	    	// Use std::copy to copy 6 elements from the source C-style array
	    	// into the destination std::array.
	    	std::copy(
	        	std::begin(mac_array), // Start of source array
	        	std::end(mac_array),   // End of source array
	        	mac_final_form.begin()             // Start of destination std::array
	    	);
		ret.push_back(mac_final_form);
	}
    }
    return ret;    
}

std::array<uint8_t,6> get_switch_mac(YAML::Node config) {
    unsigned int bytes[6];
    std::string mac = config["switch_mac"].as<std::string>();
    spdlog::debug("MAC addr: {}", mac);
    // Use sscanf to parse the hex values separated by colons.
    // %x reads a hexadecimal integer.
    int result = sscanf(mac.c_str(), "%x:%x:%x:%x:%x:%x",
                        &bytes[0], &bytes[1], &bytes[2],
                        &bytes[3], &bytes[4], &bytes[5]);
    uint8_t mac_array[6];
    if (result == 6) {
        // Cast the parsed unsigned ints back to uint8_t
        for (int i = 0; i < 6; ++i) {
            mac_array[i] = static_cast<uint8_t>(bytes[i]);
        }
    }	
    std::array<uint8_t, 6> mac_final_form;
    
    // Use std::copy to copy 6 elements from the source C-style array
    // into the destination std::array.
    std::copy(
        std::begin(mac_array), // Start of source array
        std::end(mac_array),   // End of source array
        mac_final_form.begin() // Start of destination std::array
    );
    return mac_final_form;
}

std::string get_switch_ip(YAML::Node config) {
    return config["switch_ip"].as<std::string>();
}

std::array<uint8_t,6> get_cli_mac(YAML::Node config) {
    unsigned int bytes[6];
    std::string mac = config["cli_macs"].as<std::vector<std::string>>()[0]; // TODO
    spdlog::debug("MAC addr: {}", mac);
    // Use sscanf to parse the hex values separated by colons.
    // %x reads a hexadecimal integer.
    int result = sscanf(mac.c_str(), "%x:%x:%x:%x:%x:%x",
                        &bytes[0], &bytes[1], &bytes[2],
                        &bytes[3], &bytes[4], &bytes[5]);
    uint8_t mac_array[6];
    if (result == 6) {
        // Cast the parsed unsigned ints back to uint8_t
        for (int i = 0; i < 6; ++i) {
            mac_array[i] = static_cast<uint8_t>(bytes[i]);
        }
    }	
    std::array<uint8_t, 6> mac_final_form;
    
    // Use std::copy to copy 6 elements from the source C-style array
    // into the destination std::array.
    std::copy(
        std::begin(mac_array), // Start of source array
        std::end(mac_array),   // End of source array
        mac_final_form.begin() // Start of destination std::array
    );
    return mac_final_form;
}

std::vector<std::string> get_cli_ip(YAML::Node config) {
    return config["cli_ips"].as<std::vector<std::string>>();
}

std::string get_seq_recv_port(YAML::Node config) {
    return config["seq_recv_port"].as<std::string>();
}

std::vector<std::string> get_seq_ips(const YAML::Node& config) {
    return config["seq_ips"].as<std::vector<std::string>>();
}

uint64_t get_num_m_per_extent(const YAML::Node& config) {
    return config["machines_per_extent"].as<uint64_t>();
}

uint64_t get_num_m_per_rep_set(const YAML::Node& config) {
    return config["machines_per_replica_set"].as<uint64_t>();
}

uint64_t get_extent_size(const YAML::Node& config) {
    return config["extent_range_size"].as<uint64_t>();
}

std::array<uint8_t,6> get_stor_mac(YAML::Node config) {
    unsigned int bytes[6];
    std::string mac = config["stor_macs"].as<std::vector<std::string>>()[0]; // TODO
    spdlog::debug("MAC addr: {}", mac);
    // Use sscanf to parse the hex values separated by colons.
    // %x reads a hexadecimal integer.
    int result = sscanf(mac.c_str(), "%x:%x:%x:%x:%x:%x",
                        &bytes[0], &bytes[1], &bytes[2],
                        &bytes[3], &bytes[4], &bytes[5]);
    uint8_t mac_array[6];
    if (result == 6) {
        // Cast the parsed unsigned ints back to uint8_t
        for (int i = 0; i < 6; ++i) {
            mac_array[i] = static_cast<uint8_t>(bytes[i]);
        }
    }	
    std::array<uint8_t, 6> mac_final_form;
    
    // Use std::copy to copy 6 elements from the source C-style array
    // into the destination std::array.
    std::copy(
        std::begin(mac_array), // Start of source array
        std::end(mac_array),   // End of source array
        mac_final_form.begin() // Start of destination std::array
    );
    return mac_final_form;
}

std::vector<std::string> get_stor_ips(YAML::Node config) {
    return config["stor_ips"].as<std::vector<std::string>>();
}

uint64_t get_num_client_threads(YAML::Node config) {
    return config["num_client_threads"].as<uint64_t>();
}

uint64_t get_cli_id(YAML::Node config) {
    return config["cli_id"].as<uint64_t>();
}

uint64_t get_stor_id(YAML::Node config) {
    return config["stor_id"].as<uint64_t>();
}

std::string get_json_name(YAML::Node config) {
    return config["json_name"].as<std::string>();
}

uint64_t get_num_failures(YAML::Node config) {
    return config["num_failures"].as<uint64_t>();
}

std::string get_stor_receive_port(YAML::Node config) {
    return config["stor_recv_port"].as<std::string>();
}

std::string get_switch_receive_port(YAML::Node config) {
    return config["switch_recv_port"].as<std::string>();
}

uint64_t get_use_switch(YAML::Node config) {
    return config["use_switch"].as<uint64_t>();
}

uint64_t get_use_stor(YAML::Node config) {
    return config["use_store"].as<uint64_t>();
}

uint64_t get_storage_server(YAML::Node config) {
    return config["num_storage_threads"].as<uint64_t>();
}

uint64_t get_cli_idx(YAML::Node config) {
    return config["cli_idx"].as<uint64_t>();
}

bool get_use_streams(YAML::Node config) {
    return config["use_streams"].as<uint64_t>();
}

bool get_use_shards(YAML::Node config) {
    return config["use_shard"].as<uint64_t>() == 1;
}

std::string get_multicast_addr(YAML::Node config) {
    return config["shard_multicast_addr"].as<std::string>();
}

std::vector<std::vector<std::string>> get_all_shards(YAML::Node config) {
    return config["all_shards"].as<std::vector<std::vector<std::string>>>();
}

std::vector<std::string> get_all_shards_multicast(YAML::Node config) {
    return config["all_shards"].as<std::vector<std::string>>();
}

uint64_t get_ack_threshold(YAML::Node config) {
    return config["ack_threshold"].as<uint64_t>();
}
