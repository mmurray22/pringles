#include <numeric>
#include "measure.h"
#include "spdlog/spdlog.h"

Stats::Stats() {
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
    double avgLat = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    spdlog::critical("Average latency: {}s", avgLat);
    return avgLat;
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

float Stats::getThroughput(uint64_t elapsed) {
    float tput = numOps / elapsed;
    spdlog::critical("Throughput: {}", tput);
    return tput;
}

