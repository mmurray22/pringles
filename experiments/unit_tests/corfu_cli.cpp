#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include "corfu_client.h"
#include "utils.h"
#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"

void run_append_client(std::string input_file, uint64_t i) {
    pin_current_thread_linux(i);
    CorfuClient corfu_cli = CorfuClient(input_file, i);

    corfu_cli.launch_append_execute();
    spdlog::critical("Corfu client created and started!");

    corfu_cli.wait_to_warmup();
    spdlog::critical("Corfu warmup is done!");

    corfu_cli.wait_to_finish(true);
    spdlog::critical("Corfu experiment data collection is done!");

    corfu_cli.wait_to_cooldown();
    spdlog::critical("Corfu client cooldown is done!");
}

void dummy_run_append_client(std::string input_file, uint64_t i) {
    CorfuClient corfu_cli = CorfuClient(input_file, i);

    spdlog::critical("Corfu client created, starting basic appends");
    corfu_cli.collect_stats = true;
    
    corfu_cli.execute(i); 

    spdlog::critical("Appends done! Printing stats");
    
    corfu_cli.stat->getAvgLatency();
    corfu_cli.stat->getThroughput(1);
    corfu_cli.stat->getTotalOps();
    corfu_cli.stat->dumpAllLatencies();
    corfu_cli.stat->exportResultsToJson();
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
        cli_threads.emplace_back(std::thread(&run_append_client, input_file, i));	
    }
    for (uint64_t i = 0; i < cli_threads.size(); i++) {
        cli_threads[i].join();
    }
    return 0;
}
