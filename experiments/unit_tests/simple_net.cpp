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

void custom_client(std::shared_ptr<Network> net, std::shared_ptr<Trace<std::string>> trace) {
    spdlog::info("Simple Net Client!");
    for (auto it = trace->trace_vals.begin(); it != trace->trace_vals.end(); it++) {
        spdlog::debug("Message to queue: {}", it->first);
        std::unique_ptr<std::string> buf = std::make_unique<std::string>(it->first);
        std::string pkt_type = it->second;
        net->add_to_send_queue(std::move(buf), pkt_type);
    }
}

void custom_server(std::shared_ptr<Network> net, std::shared_ptr<Trace<std::string>> trace) {
    (void) net;
    (void) trace;
    spdlog::info("Simple Net Server!");
    std::unique_ptr<std::string> rcv_str = NULL;
    uint64_t wait_time = 10;
    uint64_t it_over_trace = 0;
    while (it_over_trace < trace->trace_vals.size()) {
        if (wait_time >= MAX_WAIT_TIME) {
            spdlog::debug("!!!!!!!!!!!!!!No more packets to receive.");
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
        wait_time += 10;
        rcv_str = net->read_from_recv_queue();
        if (rcv_str == NULL) {
            continue;
        }
        spdlog::debug("Received string: {}", std::to_string((*rcv_str.get()).length()));
        assert (trace->trace_vals.find(*rcv_str.get()) != trace->trace_vals.end()); // TODO: worry about duplicates??
        spdlog::info("SUCCESS: Strings match! Received string was {}", *rcv_str.get());
        it_over_trace += 1;
        wait_time = 10;
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

    std::shared_ptr<Network> custom_net = std::make_shared<Network>(get_threads(config), 
                                                                    get_ips(config), 
                                                                    get_send_port(config), 
                                                                    get_recv_port(config),
                                                                    get_protocol(config), 
                                                                    get_log_level(config));
    set_spdlog_level(get_log_level(config));
    spdlog::info("Simple Network! Sending on localhost 127.0.0.1");
    std::shared_ptr<Trace<std::string>> trace = std::make_shared<Trace<std::string>>(get_trace_file(config));
   
    std::thread server_thread(custom_server, custom_net, trace);
    std::thread client_thread(custom_client, custom_net, trace);
    client_thread.join();
    server_thread.join();
    custom_net->done();

    return 0;
}

