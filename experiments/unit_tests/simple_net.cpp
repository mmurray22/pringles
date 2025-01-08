#include "network.h"
#include <chrono>
#include <thread>
#include <iostream>
#include <cstring>
#include <utility>

const std::string PATH_TO_YAML = "/home/micahrocks/Programming/ringlog/experiments/unit_tests/yaml/simple_net_ips.yaml";
const uint64_t MAX_WAIT_TIME = 100000;

void client(std::shared_ptr<Network> net) {
    std::cout << "Simple Net Client!" << std::endl;
    for (int i = 0; i < 3; i++) {
        std::unique_ptr<std::string> buf = std::make_unique<std::string>("Hello, World!");
        std::string val = *buf.get();
        net->add_to_send_queue(std::move(buf));
        std::cout << "Message -" << val << "- queued!" << std::endl;
    }
    return;
}

void server(std::shared_ptr<Network> net) {
    std::cout << "Simple Net Server!" << std::endl;
    std::string expected_string = "Hello, World!";
    std::unique_ptr<std::string> rcv_str = NULL;
    uint64_t wait_time = 10;
    while (true) {
        if (wait_time >= MAX_WAIT_TIME) {
            std::cout << "No more packets to receive." << std::endl;
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
        wait_time += 10;
        rcv_str = net->read_from_recv_queue();
        if (rcv_str != NULL && *rcv_str.get() == expected_string) {
            std::cout << "SUCCESS: Strings match! Received string was " << *rcv_str.get() << std::endl;
        } else if (rcv_str == NULL) {
            //std::cout << "FAIL: Received string is NULL" << std::endl;
        } else {
            std::cout << "FAIL: Received string " << *rcv_str.get() << " differs from expected string " << expected_string << std::endl;
        }
        wait_time = 10;
    }
}

int main() {
    std::cout << "Simple Network! Sending on localhost 127.0.0.1" << std::endl;
    std::shared_ptr<Network> net = std::make_shared<Network>(1, PATH_TO_YAML);
    std::thread server_thread(server, net);
    std::thread client_thread(client, net);
    client_thread.join();
    server_thread.join();
    return 0;
}
