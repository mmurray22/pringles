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
    CorfuClient client = CorfuClient(input_file, i);
    spdlog::debug("Corfu client created and started!");
    
    for (int j = 0; j < 5; j++) {
        std::string payload = fmt::format("client {} writing {}", i, j);
        client.append(payload);
    }

    // for (int j = 0; j < 11; j++) {
    //     std::string contents = client.read(j);
    //     spdlog::debug("Corfu client received contents: {}", contents);
    // }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string input_file = std::string(argv[1]);
    std::string seq_input_file = std::string(argv[2]);
    std::string storage_input_file = std::string(argv[3]);

    YAML::Node config = YAML::LoadFile(input_file);
    set_spdlog_level(get_log_level(config));

    spdlog::info("creating sequencer");
    CorfuSequencer seq = CorfuSequencer(seq_input_file);

    spdlog::info("creating storage servers");
    uint64_t num_storage_machines = get_storage_ips(config).size();
    std::vector<std::unique_ptr<CorfuStorage>> storage_servers;

    for (uint64_t ssid = 0; ssid < num_storage_machines; ssid++) {
        storage_servers.push_back(std::make_unique<CorfuStorage>(ssid, storage_input_file));
    }

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
