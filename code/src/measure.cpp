#include <numeric>
#include "measure.h"
#include "spdlog/spdlog.h"
#include <fstream>
#include <string>
#include <sstream>
#include <iostream>
#include <thread>

Stats::Stats(uint64_t batch_size, bool batch_on, std::string json_name, uint64_t thread_id) {
    this->batch_size = batch_size;
    this->batch_on = batch_on;
    this->json_name = json_name;
    this->thread_id = thread_id;
    this->latencies = {};
    this->numOps = 0;
}

Stats::~Stats() {
}

// Latency
void Stats::startLatTimer(uint64_t nonce) {
    auto duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();

    // 3. Cast the duration to milliseconds and get the count as uint64_t
    uint64_t start_time_s = std::chrono::duration_cast<std::chrono::seconds>(duration_since_epoch).count();
    lat_map.insert(std::pair<uint64_t, uint64_t>(nonce, start_time_s));
}

bool Stats::endLatTimer(uint64_t nonce) {
    if (lat_map.count(nonce)) {
	auto duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
        uint64_t end_time_s = std::chrono::duration_cast<std::chrono::seconds>(duration_since_epoch).count();
        uint64_t dur = end_time_s - lat_map[nonce];
	latencies.push_back(dur);
	return true;
    }
    return false;
}

double Stats::getAvgLatency() {
    final_avg_latency = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    spdlog::critical("Average latency: {}s", final_avg_latency);
    return final_avg_latency;
}

//Throughput
void Stats::addOp() {
    //std::lock_guard<std::mutex> guard(numOps_lock);
    numOps += 1;
}

uint64_t Stats::getTotalOps() {
    spdlog::critical("Number of ops: {}", numOps);
    return numOps;
}

double Stats::getThroughput(uint64_t elapsed) {
    final_throughput = (double) numOps / elapsed;
    spdlog::critical("Throughput: {}", final_throughput);
    return final_throughput;
}

void Stats::exportResultsToJson() {
    std::string filename = json_name + std::to_string(thread_id) + ".json";

    // NOTE: In a robust C++ project, you MUST use a dedicated JSON library
    // (like nlohmann/json.hpp) to ensure proper formatting and handle complex
    // data types, which is far safer than manual string building.
    
    // --- 2. Build the JSON Content Manually ---
    std::stringstream json_stream;
    
    // Apply stream manipulators: std::fixed for float precision, std::boolalpha for 'true'/'false' output
    json_stream << std::fixed << std::boolalpha;
    
    json_stream << "{\n";
    json_stream << "  \"avg_latency\": " << final_avg_latency << ",\n";
    json_stream << "  \"throughput\": " << final_throughput << ",\n";
    //json_stream << "  \"goodput\": " << goodput << ",\n";
    json_stream << "  \"batch_size\": " << batch_size << ",\n";
    json_stream << "  \"batch_on\": " << batch_on << ",\n";
    json_stream << "  \"num_ops\": " << numOps << "\n";
    json_stream << "}";
    
    // --- 3. Write to File ---
    std::ofstream outfile(filename);

    if (outfile.is_open()) {
        outfile << json_stream.str();
        outfile.close();
        std::cout << "Successfully exported performance data to " << filename << std::endl;
    } else {
        std::cerr << "ERROR: Unable to open file " << filename << std::endl;
    } 
}
