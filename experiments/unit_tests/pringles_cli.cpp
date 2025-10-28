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
#include <iostream>

#include "spdlog/spdlog.h"
#include "utils.h"
#include "trace.h"
#include "pringles_client.h"


void client_subroutine(std::string input_file, uint64_t cli_id, uint64_t payload_size, uint64_t thread_id) {
    LogClient pringles_client = LogClient(input_file, cli_id, thread_id);
    spdlog::debug("Pringles client created!");

    std::string payload(payload_size, 'X');
    spdlog::debug("Experiment status to start: {}", pringles_client.experiment_status());
    uint64_t cnt = 0;
    while (pringles_client.experiment_status()) {	    
 	uint32_t idx = pringles_client.append(payload);
        spdlog::debug("The entry was given index: {}", idx);
	cnt += 1;
    }
    spdlog::critical("Total cnt: {}", cnt);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string input_file = std::string(argv[1]);

    YAML::Node config = YAML::LoadFile(input_file);
    set_spdlog_level(get_log_level(config));
    uint64_t cli_id = get_cli_id(config); // move into LogClient TODO
    uint64_t num_threads = get_num_client_threads(config);
    uint64_t payload_size = get_payload_size(config);
    std::vector<std::thread> cli_threads;
    for (uint64_t i = 0; i < num_threads; i++) {
             cli_threads.emplace_back(std::thread(&client_subroutine, input_file, cli_id, payload_size, i));	
    }
    for (uint64_t i = 0; i < num_threads; i++) {
	    cli_threads[i].join();
    }
    
    // Trace 
    /*std::shared_ptr<Trace<std::string>> trace = std::make_shared<Trace<std::string>>(get_trace_file(config));
    spdlog::debug("Starting to append!");
    for (auto it = trace->trace_vals.begin(); it != trace->trace_vals.end(); it++) { // TODO trace setup?
        spdlog::debug("Message to queue: {} of irrelevant packet type {}", it->first, it->second);
    	uint64_t idx = pringles_client.append(it->first);
	spdlog::debug("The entry was given index: {}", idx);
    }*/
    spdlog::info("Finished the experiment");
    return 0;
}

