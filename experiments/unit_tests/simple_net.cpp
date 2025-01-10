#include "network.h"
#include <chrono>
#include <thread>
#include <iostream>
#include <cstring>
#include <utility>
#include <cassert>
#include "spdlog/spdlog.h"

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

int main() {
    uint64_t log_level = 1;
    if (log_level == 1) { // Prints all log levels except trace
        spdlog::set_level(spdlog::level::debug);
        spdlog::debug("Log level set to: Debug");
    } else if (log_level == 2) { // Prints error and critical
        spdlog::set_level(spdlog::level::err);
        spdlog::error("Log level set to: Error");
    } else if (log_level == 3) { // Prints warn, error, and critical
        spdlog::set_level(spdlog::level::warn);
        spdlog::warn("Log level set to: Warn");
    } else if (log_level == 4) { // Prints all but debug and trace
        spdlog::set_level(spdlog::level::info);
        spdlog::debug("Log level set to: Info");
    } else if (log_level == 5) { // Prints all log levels
        spdlog::set_level(spdlog::level::trace);
        spdlog::debug("Log level set to: Trace");
    } else { // Prints only critical
        spdlog::set_level(spdlog::level::critical);
        spdlog::critical("Log level set to: Critical");
    }
    spdlog::info("Simple Network! Sending on localhost 127.0.0.1");
    std::string str = "4950";
    std::shared_ptr<Network> net = std::make_shared<Network>(1, PATH_TO_YAML, str);
    std::thread server_thread(server, net);
    std::thread client_thread(client, net);
    client_thread.join();
    server_thread.join();
    net->done();
    return 0;
}
