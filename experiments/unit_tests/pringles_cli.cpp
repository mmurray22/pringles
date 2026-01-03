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
#include "pringles_client.h"

bool experiment_ongoing = true;
bool collect_stats = false;

void run_append_test(std::string input_file, uint64_t i, uint64_t num_threads) {
    LogClient pringles_cli = LogClient(input_file, i, num_threads);
    std::string payload(pringles_cli.get_client_payload_size(), 'X');
    while (experiment_ongoing) {
	pringles_cli.update_stats(collect_stats);
        uint64_t idx = pringles_cli.append(payload);
        spdlog::debug("The entry was given index: {}", idx);
    }
    pringles_cli.finish();
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
        cli_threads.emplace_back(std::thread(&run_append_test, input_file, i, num_work_threads));	
    }
    
    uint64_t max_duration = get_experiment_duration(config);
    uint64_t warm_up = get_warm_up(config);
    uint64_t cool_down = get_cool_down(config);
    
    spdlog::debug("Pringles client created and started!");
    wait_to_warmup(warm_up);
    spdlog::debug("Pringles warmup is done!");
    collect_stats = true;
    wait_to_finish(max_duration);
    spdlog::debug("Pringles experiment data collection is done!");
    collect_stats = false;
    wait_to_cooldown(cool_down);
    experiment_ongoing = false;
    
    for (uint64_t i = 0; i < cli_threads.size(); i++) {
        cli_threads[i].join();
    }
    return 0;
}
