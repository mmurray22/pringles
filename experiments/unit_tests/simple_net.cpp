#include "network.h"
#include <chrono>
#include <thread>
#include <iostream>
#include <cstring>
#include <utility>

const std::string PATH_TO_YAML = "yaml/simple_net_ips.yaml";
const uint64_t MAX_WAIT_TIME = 100;

void client() {
    std::cout << "Simple Net: Client" << std::endl;
    std::unique_ptr<Network> net = std::make_unique<Network>(0, PATH_TO_YAML);
    std::unique_ptr<std::string> buf = std::make_unique<std::string>("Hello, World!");
    std::string val = *buf.get();
    net->add_to_send_queue(std::move(buf));
    std::cout << "Message -" << val << "- sent!" << std::endl;
    return;
}

void server() {
    std::cout << "Simple Net Server!" << std::endl;
    std::unique_ptr<Network> net = std::make_unique<Network>(0, PATH_TO_YAML);
    std::string expected_string = "Hello, World!";
    std::unique_ptr<std::string> rcv_str = NULL;
    uint64_t wait_time = 10;
    while (!rcv_str) {
        if (wait_time >= MAX_WAIT_TIME) {
            std::cout << "FAIL: Packet never received." << std::endl;
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
        wait_time += 10;
        rcv_str = net->read_from_recv_queue();
    }
    if (rcv_str != NULL && *rcv_str.get() == expected_string) {
        std::cout << "SUCCESS: Strings match! Received string was " << *rcv_str.get() << std::endl;
    } else {
        std::cout << "FAIL: Received string " << *rcv_str.get() << " differs from expected string " << expected_string << std::endl;
    }
    return;
}

int main() {
    std::cout << "Simple Network! Sending on localhost 127.0.0.1" << std::endl;
    std::thread server_thread(server);
    std::thread client_thread(client);
    client_thread.join();
    server_thread.join();
    return 0;
}
