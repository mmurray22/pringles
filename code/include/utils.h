#pragma once
#include <cstdint>
#include <vector>
#include <string>
#include <memory>
#include "yaml-cpp/yaml.h"

/* SPDLOG helper functions */
void set_spdlog_level(uint64_t log_level); //na
uint64_t get_log_level(YAML::Node config); // covered

void pin_current_thread_linux(int core_id);

uint32_t generate_nonce(); // na

/* YAML helper class */
uint64_t get_threads(YAML::Node config);

std::string get_self_ip(YAML::Node config); // covered
uint64_t get_send_port(YAML::Node config); // covered
uint16_t get_recv_port(YAML::Node config); //covered
std::string get_socket_type(YAML::Node config); // covered
std::string get_interface(YAML::Node config); // covered

std::string get_trace_file(YAML::Node config); 

uint64_t get_batch_size(YAML::Node config); // covered
uint64_t get_batch_timeout(YAML::Node config);
bool get_batch_on(YAML::Node config); // covered

uint64_t get_num_pkt_types(YAML::Node config); // covered 
std::map<uint64_t, std::vector<std::string>> get_packet_types(YAML::Node config);
uint64_t get_sequencer_type(YAML::Node config); // covered
uint64_t get_storage_type(YAML::Node config);  // covered

uint64_t get_shard_id(YAML::Node config); 
std::vector<std::vector<std::string>> get_all_shards(YAML::Node config);
std::vector<std::string> get_all_shards_multicast(YAML::Node config);
uint64_t get_shard_switch_id(YAML::Node config);

uint64_t get_experiment_duration(YAML::Node config);
uint64_t get_warm_up(YAML::Node config);
uint64_t get_cool_down(YAML::Node config);

uint64_t get_payload_size(YAML::Node config);

std::vector<std::array<uint8_t, 6>> get_dst_mac_addrs(YAML::Node config);

uint64_t get_append_req_threads(YAML::Node config);
uint64_t get_num_client_threads(YAML::Node config);
uint64_t get_cli_id(YAML::Node config);
uint64_t get_stor_id(YAML::Node config);

std::string get_json_name(YAML::Node config);
uint64_t get_num_failures(YAML::Node config);

std::array<uint8_t,6> get_switch_mac(YAML::Node config);
std::string get_switch_ip(YAML::Node config);
std::array<uint8_t,6> get_cli_mac(YAML::Node config);
std::vector<std::string> get_cli_ip(YAML::Node config);
std::string get_seq_ip(const YAML::Node& config);
uint64_t get_num_m_per_extent(const YAML::Node& config);
uint64_t get_num_m_per_rep_set(const YAML::Node& config);
uint64_t get_extent_size(const YAML::Node& config);
std::array<uint8_t,6> get_stor_mac(YAML::Node config);
std::vector<std::string> get_stor_ips(YAML::Node config);
std::string get_multicast_addr(YAML::Node config);

std::string get_stor_receive_port(YAML::Node config);
std::string get_switch_receive_port(YAML::Node config);

std::string get_seq_recv_port(YAML::Node config);

uint64_t get_use_switch(YAML::Node config);
uint64_t get_use_stor(YAML::Node config);
bool get_use_streams(YAML::Node config);
bool get_use_shards(YAML::Node config);

uint64_t get_storage_server(YAML::Node config);
uint64_t get_cli_idx(YAML::Node config);

uint64_t get_ack_threshold(YAML::Node config);


/*INACTIVE*/
std::string get_string_entry_payload(YAML::Node config);
uint64_t get_read_timeout(YAML::Node config);
uint64_t get_write_timeout(YAML::Node config);
std::string get_protocol_type(YAML::Node config);  // covered


