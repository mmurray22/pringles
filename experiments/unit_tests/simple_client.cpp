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
#include "simple_client.h"

const uint64_t MAX_WAIT_TIME = 100;

void client(std::shared_ptr<Network> net) {
    spdlog::info("Simple Net Client!");
    for (int i = 0; i < 3; i++) {
        std::unique_ptr<std::string> buf = std::make_unique<std::string>("Hello, World!");
        std::string val = *buf.get();
        net->add_to_send_queue(std::move(buf));
        spdlog::debug("Message {} queued!", val);
    }
}

void server(std::shared_ptr<Network> net) {
    spdlog::info("Simple Net Server!");
    std::string expected_string = "Hello, World!";
    std::unique_ptr<std::string> rcv_str = NULL;
    uint64_t wait_time = 10;
    while (true) {
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
        assert (*rcv_str.get() == expected_string);
        spdlog::info("SUCCESS: Strings match! Received string was {}", *rcv_str.get());
        wait_time = 10;
    }
}

void custom_client(std::shared_ptr<Network> net, std::shared_ptr<Trace<std::string>> trace) {
    spdlog::info("Simple Net Client!");
    for (auto it = trace->trace_vals.begin(); it != trace->trace_vals.end(); it++) {
        std::unique_ptr<std::string> buf = std::make_unique<std::string>(it->second);
        std::string pkt_type = it->first;
        net->add_to_send_queue(std::move(buf), pkt_type);
        spdlog::debug("Message {} queued!", *buf.get());
    }
}

void custom_server(std::shared_ptr<Network> net, std::shared_ptr<Trace<std::string>> trace) {
    spdlog::info("Simple Net Server!");
    std::unique_ptr<std::string> rcv_str = NULL;
    uint64_t wait_time = 10;
    auto it = trace->trace_vals.begin();
    while (it != trace->trace_vals.end()) {
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
        assert (*rcv_str.get() == it->second);
        spdlog::info("SUCCESS: Strings match! Received string was {}", *rcv_str.get());
        ++it;
        wait_time = 10;
    }
    if (it != trace->trace_vals.end()) {
        spdlog::critical("Failed to receive the entire trace!");
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
    std::shared_ptr<SimpleClient> simpleCli = std::make_shared<SimpleClient>(input_file);
    set_spdlog_level(get_log_level(config));
    std::string entry = "hello";
    uint64_t idx = simpleCli->append("hello");
    std::string read_entry = simpleCli->read(idx);
    assert(read_entry == entry);
    return 0;
}

