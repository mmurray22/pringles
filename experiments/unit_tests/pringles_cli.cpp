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
#include "pringles_client.h"

void run_append_client(std::string input_file, uint64_t i, uint64_t num_threads) {
    LogClient pringles_cli = LogClient(input_file, i, num_threads);
    spdlog::debug("Pringles client created and started!");
    pringles_cli.wait_to_warmup();
    spdlog::debug("Pringles warmup is done!");
    pringles_cli.wait_to_finish(true);
    spdlog::debug("Pringles experiment data collection is done!");
    pringles_cli.wait_to_cooldown();
    spdlog::debug("Pringles client cooldown is done!");
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
        cli_threads.emplace_back(std::thread(&run_append_client, input_file, i, num_work_threads));	
    }
    for (uint64_t i = 0; i < cli_threads.size(); i++) {
        cli_threads[i].join();
    }
    return 0;
}

