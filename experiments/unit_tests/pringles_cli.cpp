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

// Performance test of append
void run_append_client(std::string input_file, uint64_t i, uint64_t num_threads) {
    LogClient pringles_cli = LogClient(input_file, i, num_threads);
    pringles_cli.launch_append_execute();
    spdlog::critical("Pringles client created and started!");
    pringles_cli.wait_to_warmup();
    spdlog::critical("Pringles warmup is done!");
    pringles_cli.wait_to_finish(true);
    spdlog::critical("Pringles experiment data collection is done!");
    pringles_cli.wait_to_cooldown();
    spdlog::critical("Pringles client cooldown is done!");
}


// Simplest Correctness Test of subscribe
void run_sub_client(std::string input_file, uint64_t i, uint64_t num_threads) {
    LogClient pringles_cli = LogClient(input_file, i, num_threads);
    pringles_cli.subscribe(0);
}


// Simplest Correctness Test of append, read, getTail
void run_basic_client(std::string input_file, uint64_t i, uint64_t num_threads, uint64_t payload_size) {
    LogClient pringles_cli = LogClient(input_file, i, num_threads);
    std::string payload(payload_size, 'X');
    uint64_t idx = pringles_cli.append(payload);
    spdlog::critical("Got append idx {}", idx);
    std::string entry = pringles_cli.read(idx);
    spdlog::critical("Got read entry {}", entry);
    assert(entry == payload);
    uint64_t tail = pringles_cli.getTail();
    spdlog::critical("Tail idx: {}", tail);
    assert(idx == tail);
    pringles_cli.wait_to_cooldown();
}


int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string input_file = std::string(argv[1]);
    YAML::Node config = YAML::LoadFile(input_file);
    set_spdlog_level(get_log_level(config));

    // Basic functionality experiment
    //std::thread basics(&run_basic_client, input_file, 0, 1, get_payload_size(config));	
    //basics.join();

    // Basic test subscribe
    std::thread basicsub(&run_sub_client, input_file, 0, 2);	
    std::thread basics(&run_basic_client, input_file, 1, 2, get_payload_size(config));	
    basicsub.join();
    basics.join();

    //// Basic scaling experiments
    //std::vector<std::thread> cli_threads = {};
    //uint64_t num_work_threads = get_num_client_threads(config);
    //for (uint64_t i = 0; i < num_work_threads; i++) {
    //    cli_threads.emplace_back(std::thread(&run_append_client, input_file, i, num_work_threads));	
    //}
    //for (uint64_t i = 0; i < cli_threads.size(); i++) {
    //    cli_threads[i].join();
    //}
    return 0;
}

