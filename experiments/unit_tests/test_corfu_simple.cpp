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

int dummy_client(std::string input_file) {
   // Get packet types for sending/receiving
   YAML::Node config = YAML::LoadFile(input_file);
   uint64_t cid = 0;

   std::vector<std::string> seq_ips = get_seq_ips(config);
   std::vector<std::string> storage_ips = get_stor_ips(config);
  
   // Create network
   std::string client_send_port = std::to_string(get_send_port(config));
   std::string client_recv_port = std::to_string(get_recv_port(config));

    std::string multicast_ip = "";

    std::shared_ptr<Network> net = std::make_shared<Network>(
        client_send_port, 
        client_recv_port,
		get_socket_type(config),
        get_log_level(config),
		get_batch_size(config),
		get_batch_on(config),
		get_batch_timeout(config),
	    get_interface(config),
		get_self_ip(config),
		multicast_ip,
		false,
		false);
    spdlog::info("Corfu Client: net init");

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
        seq_ips[0],
        "4951" // THIS IS HARDCODED, FIND A BETTER FIX TODO
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
        spdlog::critical("We did not receive anything from sequencer before timeout");
        return 1; // failure
    }

    // recv packet from the sequencer
    corfusequencer::Payload packet_contents = corfu_sequencer_deserialize_str_entry(std::make_unique<std::string>(msg));

    uint64_t log_idx = packet_contents.send_token().token();

    spdlog::info("cid {} got index value {} from sequencer", cid, log_idx);

    // SPEAK WITH STORAGE SERVER
    std::string entry = "hello";

    std::unique_ptr<std::string> append_packet = corfu_client_serialize_str_entry(entry, CORFU_APPEND_PROTO_TYPE, cid, log_idx, 0);
    allocated_packet_size = append_packet->length() + 1;
    spdlog::debug("append packet is of size: {}", allocated_packet_size);
    packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), append_packet->c_str(), allocated_packet_size);

    spdlog::info("client {} is appending {}", cid, entry);
    net->send_client_udp_packet(
        std::move(packet),
        allocated_packet_size,
        storage_ips[0],
        "4952" // THIS IS HARDCODED, FIND A BETTER FIX TODO
    );
    msg = net->recv_packet();

    // start timer
    start_time = std::chrono::high_resolution_clock::now();
    read_time = std::chrono::high_resolution_clock::duration::zero();
    while (read_time < TIMEOUT && !msg) {
        msg = net->recv_packet();
        read_time = std::chrono::high_resolution_clock::now() - start_time;
    }

    if (!msg && read_time >= TIMEOUT) {
        spdlog::critical("We did not receive anything from server before timeout");
        return 1; // failure
    }

    // recv packet from storage server
    corfustorage::Payload stor_packet_contents = corfu_storage_deserialize_str_entry(std::make_unique<std::string>(msg));

    bool ack_code = stor_packet_contents.ack().ack_code();

    if (ack_code) {
        spdlog::info("cid {} got an ack from storage", cid);
    } else {
        spdlog::info("cid {} did not receive an ack from storage", cid);
    }

    // CLIENT READ VALUE WRITTEN
    std::unique_ptr<std::string> read_packet = corfu_client_serialize_str_entry("", CORFU_READ_PROTO_TYPE, cid, log_idx, 0);
    allocated_packet_size = read_packet->length() + 1;
    spdlog::debug("read packet is of size: {}", allocated_packet_size);
    packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), read_packet->c_str(), allocated_packet_size);
    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size,
        seq_ips.at(0),
        "4952" // THIS IS HARDCODED, FIND A BETTER FIX TODO
    );
    msg = net->recv_packet();

    // start timer
    start_time = std::chrono::high_resolution_clock::now();
    read_time = std::chrono::high_resolution_clock::duration::zero();
    while (read_time < TIMEOUT && !msg) {
        msg = net->recv_packet();
        read_time = std::chrono::high_resolution_clock::now() - start_time;
    }

    if (!msg && read_time >= TIMEOUT) {
        spdlog::critical("We did not receive anything from server before timeout");
        return 1; // failure
    }

    // recv packet from storage server
    corfustorage::Payload read_packet_contents = corfu_storage_deserialize_str_entry(std::make_unique<std::string>(msg));

    std::string read_content = read_packet_contents.read().content();
    spdlog::info("read request from client {} is: {}", cid, read_content);

    return 0;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string cli_input_file = std::string(argv[1]);
    std::string seq_input_file = std::string(argv[2]);
    std::string storage_input_file = std::string(argv[3]);

    spdlog::info("creating dummy sequencer");
    std::unique_ptr<CorfuSequencer> seq = std::make_unique<CorfuSequencer>(seq_input_file);

    spdlog::info("creating dummy storage server");
    std::unique_ptr<CorfuStorage> storage = std::make_unique<CorfuStorage>(0, storage_input_file);

    spdlog::info("creating dummy client thread");
    std::thread cli_thread(dummy_client, cli_input_file);

    /** join threads **/
    cli_thread.join();
    spdlog::info("joined on client thread, exiting");
    return 0;
}
