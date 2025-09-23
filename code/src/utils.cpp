#include "utils.h"
#include "spdlog/spdlog.h"
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

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

/* Nonce generation function */
std::unique_ptr<unsigned char> generate_nonce() {
    std::unique_ptr<unsigned char> nonce = std::make_unique<unsigned char>(16);
    int rc = RAND_bytes(nonce.get(), sizeof(nonce));
    if(rc != 1) {
        spdlog::critical("Nonce failed to generate!!");
        throw;
    }
    return nonce;
}

/* YAML processing functions */
uint64_t get_threads(YAML::Node config) {
    return config["threads"].as<uint64_t>();
}

// OPTIONAL
std::string get_seq_ip(YAML::Node config) {
    if (config["seq_ip"]) {
        return config["seq_ip"].as<std::string>();
    }
    return "";
}

std::string get_src_ip(YAML::Node config) {
    return config["src_ip"].as<std::string>();
}

std::string get_storage_multicast_addr(YAML::Node config) {
    return config["storage_multicast_addr"].as<std::string>();
}

std::vector<std::string> get_storage_ips(YAML::Node config) {
    std::vector<std::string> ret;
    for (auto ip_list : config["storage_ip_list"]) {
        spdlog::debug("What value? ip: {}", ip_list["ip"].as<std::string>());
        ret.emplace_back(ip_list["ip"].as<std::string>());
    }
    return ret;
}

std::string get_protocol_type(YAML::Node config) {
    return config["protocol_type"].as<std::string>();
}

std::vector<std::string> get_packet_types(YAML::Node config) {
    std::vector<std::string> ret;
    for (auto pkt_types : config["packet_types"]) {
        spdlog::debug("Packet type: {}", pkt_types["type"].as<std::string>());
        ret.emplace_back(pkt_types["type"].as<std::string>());
    }
    return ret;
}

std::string get_send_port(YAML::Node config) {
    spdlog::debug("Port: {}", config["send_port"].as<std::string>());
    return config["send_port"].as<std::string>();
}

std::string get_recv_port(YAML::Node config) {
    spdlog::debug("Port: {}", config["recv_port"].as<std::string>());
    return config["recv_port"].as<std::string>();
}


std::string get_socket_type(YAML::Node config) {
    spdlog::debug("Socket type: {}", config["socket_type"].as<std::string>());
    return config["socket_type"].as<std::string>();
}

uint64_t get_log_level(YAML::Node config) {
    return config["loglevel"].as<uint64_t>();
}

// OPTIONAL
std::string get_trace_file(YAML::Node config) {
    if (config["trace_file"]) {
        return config["trace_file"].as<std::string>();
    }
    return "";
}

/// NEW ONES ///
std::string get_type(YAML::Node config) {
    return config["type"].as<std::string>();
}

uint64_t get_batch_size(YAML::Node config) {
    return config["batch_size"].as<uint64_t>();
}

std::string get_interface(YAML::Node config) {
    return config["interface"].as<std::string>();
}

uint64_t get_read_timeout(YAML::Node config) {
    return config["read_timeout"].as<uint64_t>();
}

uint64_t get_write_timeout(YAML::Node config) {
    return config["write_timeout"].as<uint64_t>();
}
