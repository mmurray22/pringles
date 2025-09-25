#include <cstdint>
#include <vector>
#include <string>
#include <memory>
#include "yaml-cpp/yaml.h"

/* SPDLOG helper functions */
void set_spdlog_level(uint64_t log_level);
uint64_t get_log_level(YAML::Node config);

std::unique_ptr<unsigned char> generate_nonce(); 

/* YAML helper class */
uint64_t get_threads(YAML::Node config);
std::string get_self_ip(YAML::Node config);

std::string get_send_port(YAML::Node config);
std::string get_recv_port(YAML::Node config);

std::string get_socket_type(YAML::Node config);

std::string get_string_entry_payload(YAML::Node config);
std::string get_trace_file(YAML::Node config); 

uint64_t get_batch_size(YAML::Node config);
bool get_batch_on(YAML::Node config);

std::string get_interface(YAML::Node config);
std::map<std::string, std::vector<std::string>> get_packet_types(YAML::Node config);

uint64_t get_read_timeout(YAML::Node config);
uint64_t get_write_timeout(YAML::Node config);

std::string get_protocol_type(YAML::Node config); 
