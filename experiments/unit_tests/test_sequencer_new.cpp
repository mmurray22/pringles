#include <chrono>
#include <thread>
#include <iostream>
#include <cstring>
#include <utility>
#include <cassert>
#include <fstream>
#include "spdlog/spdlog.h"
#include "corfu_client.h"

#include "corfuclient.pb.h"
#include "corfustorage.pb.h"
#include "corfusequencer.pb.h"

#include "corfu_sequencer.h"
#include "utils.h"

int dummy_client(std::string input_file, uint64_t cid) {
   // Get packet types for sending/receiving
   YAML::Node config = YAML::LoadFile(input_file);

   uint64_t num_pkt_types = 0;

   num_pkt_types = get_num_pkt_types(config);

   std::vector<std::string> seq_ips = get_seq_ips(config);
  
   // Create network
   bool run_threads = false;
   std::string client_send_port = std::to_string(get_recv_port(config));
   std::string client_recv_port = std::to_string(get_send_port(config) + cid);

   std::unique_ptr<Network> net = std::make_unique<Network>(
    client_send_port, 
    client_recv_port,
    get_socket_type(config),
    get_log_level(config),
    get_batch_size(config),
    get_batch_on(config),
    get_batch_timeout(config), 
    get_interface(config),
    get_self_ip(config),
    num_pkt_types,
    run_threads
    );
    spdlog::info("Corfu Client {}: net init", cid);

    std::map<PacketType, std::queue<std::unique_ptr<char[]>>> pkt_q;
    
    // Start receiving thread 
    for (size_t i = 0; i < num_pkt_types; i++) {
        pkt_q.insert(std::pair<PacketType, std::queue<std::unique_ptr<char[]>>>(PacketType(i), std::queue<std::unique_ptr<char[]>>()));
    }

    // TALK WITH SEQUENCER PART

    // we use the network object to send a request to the sequencer for the next log position
    std::unique_ptr<std::string> sequencing_packet = corfu_client_serialize_str_entry("", CORFU_GETTOKEN_PROTO_TYPE, cid, 0, 0);
    uint64_t allocated_packet_size = sequencing_packet->length() + 1;
    spdlog::debug("gettoken packet is of size: {}", allocated_packet_size);
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), sequencing_packet->c_str(), allocated_packet_size);
    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size, 
        static_cast<int>(PacketType::gettoken), 
        ETH_CLI_SEQ, 
        seq_ips[0],
        client_send_port
    );
    char* msg = net->recv_packet();

    // start timer
    auto start_time = std::chrono::high_resolution_clock::now();
    auto read_time = std::chrono::high_resolution_clock::duration::zero();
    while (read_time < TIMEOUT && !msg) {
        msg = net->recv_packet();
        read_time = std::chrono::high_resolution_clock::now() - start_time;
    }

    if (!msg && read_time >= TIMEOUT) {
        spdlog::critical("We did not receive anything from server before timeout");
        return 1; // failure
    }

    // recv packet from the sequencer
    corfusequencer::Payload packet_contents = corfu_sequencer_deserialize_str_entry(std::make_unique<std::string>(msg));

    uint64_t log_idx = packet_contents.send_token().token();

    spdlog::info("cid {} got index value {} from sequencer", cid, log_idx);
    return log_idx;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string cli_input_file = std::string(argv[1]);
    
    YAML::Node config = YAML::LoadFile(cli_input_file);
    std::unique_ptr<CorfuSequencer> seq = std::make_unique<CorfuSequencer>(cli_input_file);

    spdlog::info("creating dummy client thread");
    std::vector<std::thread> client_threads;
    client_threads.reserve(5);

    for (int i = 0; i < 5; i++) {
        client_threads.emplace_back(dummy_client, cli_input_file, i);
    }

    for (int i = 0; i < 5; i++) {
        if (client_threads[i].joinable()) {
            client_threads[i].join();
            spdlog::info("joined on client thread {}", i);
        }
    }

    spdlog::info("all client threads joined");

    return 0;
}
