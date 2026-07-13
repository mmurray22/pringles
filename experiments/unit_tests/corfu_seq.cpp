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

int main(int argc, char* argv[]) {
    if (argc < 2) {
        spdlog::critical("Not enough arguments provided! Need YAML file");
    }
    std::string input_file = std::string(argv[1]);
    
    YAML::Node config = YAML::LoadFile(input_file);

    CorfuSequencer corfu_seq = CorfuSequencer(input_file);
    corfu_seq.wait_to_finish();
    spdlog::info("sequencer done");
    return 0;
}
