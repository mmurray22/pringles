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

void run_client(std::string input_file, uint64_t i) {
    CorfuClient corfu_cli = CorfuClient(input_file, i);
    spdlog::debug("Corfu client created and started!");

    uint64_t idx = 0;
    std::string entry = "";

    std::string payload_x = "X";
    idx = corfu_cli.append(payload_x);
    spdlog::critical("APPEND idx {}", idx);
    assert(idx == 1);
    entry = corfu_cli.read(idx);
    spdlog::critical("READ entry {}", entry);
    assert(entry == payload_x);

    std::string payload_y = "Y";
    idx = corfu_cli.append(payload_y);
    spdlog::critical("APPEND idx {}", idx);
    assert(idx == 2);
    entry = corfu_cli.read(idx);
    spdlog::critical("READ entry {}", entry);
    assert(entry == payload_y);

    std::string payload_z = "Z";
    idx = corfu_cli.append(payload_z);
    spdlog::critical("APPEND idx {}", idx);
    assert(idx == 3);
    entry = corfu_cli.read(idx);
    spdlog::critical("READ entry {}", entry);
    assert(entry == payload_z);

    corfu_cli.wait_to_cooldown();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string input_file = std::string(argv[1]);
    YAML::Node config = YAML::LoadFile(input_file);
    set_spdlog_level(get_log_level(config));

    std::vector<std::thread> cli_threads = {};
    uint64_t num_work_threads = get_num_client_threads(config);
    for (uint64_t i = 0; i < num_work_threads; i++) {
        cli_threads.emplace_back(std::thread(&run_client, input_file, i));	
    }
    for (uint64_t i = 0; i < cli_threads.size(); i++) {
        cli_threads[i].join();
    }
    return 0;
}
