/*
 * Simple Network Test File
 *
 * This test is meant to test basic sending and receiving sockets on a single host.
 * Arguments:
 * - Path to yaml file 
 */

#include "network.h"
#include <chrono>
#include <thread>
#include <iostream>
#include <cstring>
#include <utility>
#include <cassert>
#include <fstream>

#include "spdlog/spdlog.h"
#include "utils.h"
#include "trace.h"

const uint64_t MAX_WAIT_TIME = 100;

void custom_client(std::unique_ptr<Network> net, std::shared_ptr<Trace<std::string>> trace) {
    spdlog::info("Simple Net Client!");
    std::string pkt_type_term = "";
    for (auto it = trace->trace_vals.begin(); it != trace->trace_vals.end(); it++) {
        spdlog::debug("Message to queue: {}", it->first);
        std::unique_ptr<std::string> buf = std::make_unique<std::string>(it->first);
        std::string pkt_type = it->second;
        pkt_type_term = it->second;
        net->add_to_send_queue(std::move(buf), pkt_type);
    }
    std::string term = "done!";
    std::unique_ptr<std::string> buf = std::make_unique<std::string>(term);
    net->add_to_send_queue(std::move(buf), pkt_type_term);
    spdlog::debug("Terminating message: {}", term);

    spdlog::info("Simple Net Server!");
    std::unique_ptr<std::string> recvd_str_ptr = NULL;
    uint64_t wait_time = 1;
    uint64_t it_over_trace = 0;
    while (it_over_trace < trace->trace_vals.size()) {
        if (wait_time >= MAX_WAIT_TIME) {
            spdlog::debug("!!!!!!!!!!!!!!No more packets to receive.");
            break;
        }
	
        recvd_str_ptr = net->read_from_recv_queue();
	if (recvd_str_ptr == NULL) { // nothing in the receive queue, so we sleep
	    //spdlog::debug("Nothing receive! Going to sleep...");
            //std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
            continue;
        }

        spdlog::debug("Received string: {}", std::to_string((*recvd_str_ptr.get()).length()));
        assert (trace->trace_vals.find(*recvd_str_ptr.get()) != trace->trace_vals.end()); // TODO: worry about duplicates??
        spdlog::info("SUCCESS: Strings match! Received string was {}", *recvd_str_ptr.get());
        it_over_trace += 1;
    }
    if (it_over_trace < trace->trace_vals.size()) {
        spdlog::critical("Failed to receive the entire trace! Only received: {}", it_over_trace);
        assert(1 == 0);
    }
    spdlog::critical("SUCCESS: All strings matched and the entire trace was received.");
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string input_file = std::string(argv[1]);
    YAML::Node config = YAML::LoadFile(input_file);

    std::unique_ptr<Network> custom_net = std::make_unique<Network>(get_threads(config), 
		    						    get_seq_ip(config),
                                                                    get_storage_multicast_addr(config), 
                                                                    get_send_port(config), 
                                                                    get_recv_port(config),
								    get_socket_type(config),
                                                                    get_log_level(config),
								    get_batch_size(config),
								    get_interface(config),
								    get_src_ip(config),
								    get_packet_types(config));
    set_spdlog_level(get_log_level(config));
    spdlog::info("Simple Network: Sending to remote host");
    std::shared_ptr<Trace<std::string>> trace = std::make_shared<Trace<std::string>>(get_trace_file(config));
    std::thread client_thread(&custom_client, std::move(custom_net), trace);
    client_thread.join();
    custom_net->done();

    return 0;
}

