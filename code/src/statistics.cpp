#include "statistics.h"
#include <algorithm>
#include <cmath>
#include <numeric>

Stats::Stats() : lats() {}

Stats::~Stats() {}

/** Throughput functions **/
bool Stats::addOperations(uint64_t num_ops) {
    total_operations += num_ops;
    return true;
}

void Stats::recordStartTput() {
    start_tput = std::chrono::steady_clock::now();
}

void Stats::recordEndTput() {
    end_tput = std::chrono::steady_clock::now();
}

double Stats::calculateTput() {
    auto dur = std::chrono::duration_cast<std::chrono::microseconds>(end_tput - start_tput);
    return total_operations/dur.count();
}

/** Latency functions **/
void Stats::recordStartLat() {
    start_lat = std::chrono::steady_clock::now();
}

void Stats::recordEndLat() {
    end_lat = std::chrono::steady_clock::now();
}

double Stats::calculatePointLatency() {
    auto lat = std::chrono::duration_cast<std::chrono::microseconds>(end_lat - start_lat);
    lats.emplace_back(lat.count());
    return lat.count();
}

double Stats::calculateAvgLatency() {
    double sum = std::accumulate(lats.begin(), lats.end(), 0);
    return sum/lats.size();
}

double Stats::calculateMedLatency() {
    std::sort(lats.begin(), lats.end());
    return lats[std::ceil(lats.size()/2)];
}
