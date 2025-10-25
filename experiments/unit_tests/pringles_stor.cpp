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
#include "pringles_storage.h"


int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string input_file = std::string(argv[1]);
    uint64_t stor_id = std::stoi(argv[2]); // TODO put into yaml file
    YAML::Node config = YAML::LoadFile(input_file);
    set_spdlog_level(get_log_level(config));

    LogStorage pringles_store = LogStorage(input_file, stor_id);
    spdlog::debug("Pringles storage created and started!");
    pringles_store.wait_to_finish();
    return 0;
}

