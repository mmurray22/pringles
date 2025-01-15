/*
 * Simple Network Test File
 *
 * This test is meant to test basic sending and receiving sockets on a single host.
 * Arguments [expecting ]:
 * - Expected string: This is the string that we want the sender to send & receiver to receive 
 */

#include "network.h"
#include <chrono>
#include <thread>
#include <iostream>
#include <cstring>
#include <utility>
#include <cassert>
#include "spdlog/spdlog.h"
#include "utils.h"

const std::string PATH_TO_YAML = "/home/micahrocks/Programming/ringlog/experiments/unit_tests/yaml/simple_net_ips.yaml";
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

void custom_client(std::shared_ptr<Network> net) {
    spdlog::info("Simple Net Client!");
    for (int i = 0; i < 3; i++) {
        std::unique_ptr<std::string> buf = std::make_unique<std::string>("Hello, World!");
        std::string val = *buf.get();
        std::string pkt_type = "append_req";
        net->add_to_send_queue(std::move(buf), pkt_type);
        spdlog::debug("Message {} queued!", val);
    }
}

void custom_server(std::shared_ptr<Network> net) {
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
        spdlog::debug("Received string: {}", std::to_string((*rcv_str.get()).length()));
        assert (*rcv_str.get() == expected_string);
        spdlog::info("SUCCESS: Strings match! Received string was {}", *rcv_str.get());
        wait_time = 10;
    }
}

int main(/*int argc, char* argv[]*/) {
    uint64_t log_level = 1;
    set_spdlog_level(log_level);
    /*if (argc < 2) {
        spdlog::critical("Not enough arguments provided!");
    }*/
    spdlog::info("Simple Network! Sending on localhost 127.0.0.1");
    std::string str = "4950";
    /*uint64_t protocol_id = 0;
    std::shared_ptr<Network> net = std::make_shared<Network>(1, PATH_TO_YAML, str, protocol_id);
    std::thread server_thread(server, net);
    std::thread client_thread(client, net);
    client_thread.join();
    server_thread.join();
    net->done();*/

    uint64_t protocol_id = 2;
    std::shared_ptr<Network> custom_net = std::make_shared<Network>(1, PATH_TO_YAML, str, protocol_id);
    std::thread server_thread(custom_server, custom_net);
    std::thread client_thread(custom_client, custom_net);
    client_thread.join();
    server_thread.join();
    custom_net->done();

    return 0;
}
