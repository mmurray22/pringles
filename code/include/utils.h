#include <cstdint>
#include <vector>
#include <string>
#include "yaml-cpp/yaml.h"

/* SPDLOG helper functions */
void set_spdlog_level(uint64_t log_level);

/* YAML helper class */
uint64_t get_threads(YAML::Node config);
std::vector<std::string> get_ips(YAML::Node config);
std::string get_send_port(YAML::Node config);
std::string get_recv_port(YAML::Node config);
uint64_t get_protocol(YAML::Node config);
uint64_t get_log_level(YAML::Node config);
std::string get_string_entry_payload(YAML::Node config);
std::string get_trace_file(YAML::Node config); 
