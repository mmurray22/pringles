/*
 * Corfu Test File
 *
 * Arguments:
 * - Path to yaml file(s)
 */

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


void dummy_client(std::string cli_input) {
    // create network object
    YAML::Node config = YAML::LoadFile(cli_input);
    // make it so each object has its own network object :o
    // so put this init in the corfu sequencer and storage constructors
    std::unique_ptr<Network> net = std::make_unique<Network>(get_threads(config), 
                                                                    get_send_port(config), 
                                                                    get_recv_port(config),
								    get_socket_type(config),
                                                                    get_log_level(config),
								    get_batch_size(config),
								    get_batch_on(config),
								    get_interface(config),
								    get_self_ip(config),
								    get_packet_types(config));
    
    // get the trace
    std::unique_ptr<Trace<std::string>> trace = std::make_unique<Trace<std::string>>(get_trace_file(config));

    spdlog::info("Corfu Client sending!");
    for (auto it = trace->trace_vals.begin(); it != trace->trace_vals.end(); it++) {
        spdlog::debug("Message to queue: {}, of packet type {}", it->second, it->first);
        
        /*Create Corfu protobuf packet manually*/
        std::unique_ptr<std::string> output = std::make_unique<std::string>();
        
        corfuclient::Payload corfu_payload;
        corfu_payload.set_packet_type(CORFU_GETTOKEN_PROTO_TYPE);
        corfu_payload.set_clientid(get_self_ip(config));

        corfuclient::GetToken* pkt = new corfuclient::GetToken(); 
        pkt->set_reqtoken(true);
        corfu_payload.set_allocated_token_req(pkt);

        corfu_payload.set_allocated_token_req(&pkt);
        corfu_payload.SerializeToString(output.get());

        /**
         * Sending a packet:
         * self_ip -> pkt_type[ip address list]
         *
         * Receiving a  packet 
         * pkt_type[ip] -> self_ip
         *
         */
        
        /*Finish packet creation*/

        std::string pkt_type = it->second;
        net->add_to_send_queue(std::move(output), pkt_type);

        auto start_time = std::chrono::high_resolution_clock::now();
        std::unique_ptr<std::string> rcv_str;
        while (true) {
            auto now = std::chrono::high_resolution_clock::now();
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();
            if (static_cast<uint64_t>(elapsed_ms) >= MAX_WAIT_TIME) {  // MAX_WAIT_TIME should be in milliseconds
                spdlog::debug("!!!!!!!!!!!!!!No packets received! (timeout)");
                break;
            }
            //
            //wait_time += 10;
            rcv_str = net->read_from_recv_queue();
            if (!rcv_str) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            /*Deserialize the packet*/
            corfusequencer::Payload corfu_payload;
            corfu_payload.ParseFromString(*(rcv_str.get()));

            uint64_t log_idx = corfu_payload.send_token().token();
            uint64_t proto_type = corfu_payload.packet_type();

            spdlog::debug("Sequencing packet received back: log idx: {}, of proto_type {}", log_idx, proto_type);
        }

    }
    spdlog::info("Dummy client done sending trace.");

}

void dummy_storage() {
    // Test dummy storage server here
    // not required yet for first sequencer test
}


int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string cli_input_file = std::string(argv[1]);
    // std::string seq_input_file = std::string(argv[2]);
    // std::string stor_input_file = std::string(argv[3]);
    
    YAML::Node config = YAML::LoadFile(cli_input_file);
    std::unique_ptr<CorfuSequencer> seq = std::make_unique<CorfuSequencer>(config);

    spdlog::info("creating dummy client thread");
    std::thread cli_thread(dummy_client, cli_input_file);
    //std::thread(storage, stor_input_file);
    

    /** join threads **/
    cli_thread.join();
    spdlog::info("joined on client thread, exiting");
    return 0;
}
