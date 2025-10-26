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


int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string input_file = std::string(argv[1]);
    uint64_t cli_id = std::stoi(argv[2]);
    YAML::Node config = YAML::LoadFile(input_file);
    set_spdlog_level(get_log_level(config));

    LogClient pringles_client = LogClient(input_file, cli_id);
    spdlog::debug("Pringles client created!");
    uint64_t payload_size = get_payload_size(config);
    std::string payload(payload_size, 'X');
    // TODO generate string of size payload
    spdlog::debug("Experiment status to start: {}", pringles_client.experiment_status());
    //while (!pringles_client.experiment_status()) {	    
    uint64_t idx = pringles_client.append(payload);
    spdlog::debug("The entry was given index: {}", idx);
    //}

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

