/*
 * Simple Network Test File
 *
 * This test is meant to test basic sending and receiving sockets on a single host.
 * Arguments:
 * - Path to yaml file 
 */

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
#include "network.h"

const uint64_t MAX_WAIT_TIME = 100;

void custom_server(std::unique_ptr<Network> net, std::string pkt_type) {
    spdlog::info("Simple Net Server!");
    std::unique_ptr<std::string> rcv_str = NULL;
    uint64_t wait_time = 1;
    bool waiting_for_pkt = true;
    std::vector<std::string> recv_strs = {};
    while (waiting_for_pkt) {
        if (wait_time >= MAX_WAIT_TIME) {
            spdlog::debug("!!!!!!!!!!!!!!No more packets to receive.");
            break;
        }

        auto recvd_str_ptr = net->read_from_recv_queue();
        if (recvd_str_ptr == NULL) { // nothing in the receive queue, so we sleep
	    //spdlog::debug("Nothing receive! Going to sleep...");
           // std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
            continue;
        }
 	std::string recvd_str = *recvd_str_ptr.get();
        spdlog::debug("Received string: {}", recvd_str);
	if (recvd_str == "done!") {
		waiting_for_pkt = false;
		continue;
	}
	recv_strs.push_back(recvd_str);
        //spdlog::info("SUCCESS: Strings match! Received string was {}", );
    }

    // Send back data
    spdlog::info("Simple Net Client!");
    for (uint64_t i = 0; i < recv_strs.size(); i++) {
        spdlog::debug("Message to queue: {}", recv_strs[i]);
        std::unique_ptr<std::string> buf = std::make_unique<std::string>(recv_strs[i]);
        net->add_to_send_queue(std::move(buf), pkt_type);
    }
    while (true) {
	    /*nothing*/
    }
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
    spdlog::info("Simple Network! Sending remote!");
    //std::shared_ptr<Trace<std::string>> trace = std::make_shared<Trace<std::string>>(get_trace_file(config));
    std::string pkt_type = get_packet_types(config)[0];
    std::thread server_thread(custom_server, std::move(custom_net), pkt_type);
    server_thread.join();
    custom_net->done();
    return 0;
}

