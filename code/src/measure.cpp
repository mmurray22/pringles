#include <numeric>
#include "measure.h"
#include "spdlog/spdlog.h"
#include <fstream>
#include <string>
#include <sstream>
#include <iostream>
#include <thread>

Stats::Stats(uint64_t batch_size, bool batch_on, std::string json_name, uint64_t thread_id, std::string client_ip) {
    this->batch_size = batch_size;
    this->batch_on = batch_on;
    this->json_name = json_name;
    this->thread_id = thread_id; //default
    this->latencies = {};
    this->numOps = 0;
    this->subscribe_latency = 0;
    this->client_ip = client_ip;
}

Stats::~Stats() {
}

// Microbenchmark: Send call
/*double Stats::getStartSendTo() {
    auto duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();

    // 3. Cast the duration to milliseconds and get the count as uint64_t
    return std::chrono::duration_cast<std::chrono::duration<double>>(duration_since_epoch).count();
}

void Stats::getSendToDuration(double start_time) {
    auto duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
    double end_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(duration_since_epoch).count();
    double dur = end_time_s - start_time;
    sendto.push_back(dur);
}*/

// Latency
void Stats::startLatTimer(uint64_t nonce) {
    auto duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();

    // 3. Cast the duration to milliseconds and get the count as uint64_t
    double start_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(duration_since_epoch).count();
    
    std::unique_lock<std::mutex> lock(lat_map_lock);
    lat_map.insert(std::pair<uint64_t, double>(nonce, start_time_s));
}

double Stats::getStartLat() {
    auto duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();

    // 3. Cast the duration to milliseconds and get the count as uint64_t
    return std::chrono::duration_cast<std::chrono::duration<double>>(duration_since_epoch).count();
}

void Stats::getDuration(double start_time) {
    auto duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
    double end_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(duration_since_epoch).count();
    double dur = end_time_s - start_time;
    //spdlog::critical("Duration: {}", dur);
    latencies.push_back(dur);
}

void Stats::addDuration(double dur) {
    //spdlog::critical("Duration: {}", dur);
    latencies.push_back(dur);
}


bool Stats::endLatTimer(uint64_t nonce) {
    if (lat_map.count(nonce) > 0) {
	auto duration_since_epoch = (std::chrono::steady_clock::now()).time_since_epoch();
        double end_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(duration_since_epoch).count();
        std::unique_lock<std::mutex> lock(lat_map_lock);
        double dur = end_time_s - lat_map[nonce];
	//spdlog::critical("For nonce {}, started {}, ended {}, for duration {}", nonce, lat_map[nonce], end_time_s, dur);
	latencies.push_back(dur);
	lat_map.erase(nonce);
	return true;
    }
    return false;
}

double Stats::getAvgLatency() {
    final_avg_latency = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    final_avg_latency *= 1000;
    spdlog::critical("Average latency: {}ms", final_avg_latency);
    return final_avg_latency;
}

//Throughput
void Stats::addOp() {
    //std::lock_guard<std::mutex> lock(num_ops_lock);
    numOps += 1;
}

uint64_t Stats::getTotalOps() {
    //spdlog::critical("Number of ops: {}", numOps);
    return numOps;
}

double Stats::getThroughput(uint64_t elapsed) {
    final_throughput = (double) numOps / elapsed;
    //spdlog::critical("Throughput: {}", final_throughput);
    return final_throughput;
}

void Stats::putSubLatency(double sub_lat) {
    subscribe_latency = sub_lat;
}

// Written with the help of LLMs
void Stats::exportResultsToJson() {
    std::string filename = json_name + "_" + std::to_string(thread_id) + "_" + client_ip + ".json";

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
    json_stream << "  \"sub_lat\": " << subscribe_latency << ",\n";
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

void Stats::dumpAllLatencies() {
    // spdlog::info("--- Raw Latency Dump for Thread {} ---");
    // for (size_t i = 0; i < latencies.size(); ++i) {
    //     spdlog::info("Latency: {}", latencies[i]);
    // }
}