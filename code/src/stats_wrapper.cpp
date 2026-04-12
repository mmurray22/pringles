// Made with Gemini 
#include "measure.h"
#include "stats_wrapper.h"
#include <string>

extern "C" {

StatsPtr NewStats(uint64_t batch_size, bool batch_on, const char* json_name, uint64_t thread_id, const char* client_ip) {
    return static_cast<StatsPtr>(new Stats(batch_size, batch_on, std::string(json_name), thread_id, std::string(client_ip)));
}

void FreeStats(StatsPtr s) {
    delete static_cast<Stats*>(s);
}

void StatsStartLatTimer(StatsPtr s, uint64_t nonce) {
    static_cast<Stats*>(s)->startLatTimer(nonce);
}

bool StatsEndLatTimer(StatsPtr s, uint64_t nonce) {
    return static_cast<Stats*>(s)->endLatTimer(nonce);
}

double StatsGetStartLat(StatsPtr s) {
    return static_cast<Stats*>(s)->getStartLat();
}

void StatsGetDuration(StatsPtr s, double start_time) {
    static_cast<Stats*>(s)->getDuration(start_time);
}

void StatsAddDuration(StatsPtr s, double start_time) {
    static_cast<Stats*>(s)->addDuration(start_time);
}

double StatsGetAvgLatency(StatsPtr s) {
    return static_cast<Stats*>(s)->getAvgLatency();
}

void StatsAddOp(StatsPtr s) {
    static_cast<Stats*>(s)->addOp();
}

uint64_t StatsGetTotalOps(StatsPtr s) {
    return static_cast<Stats*>(s)->getTotalOps();
}

double StatsGetThroughput(StatsPtr s, uint64_t elapsed) {
    return static_cast<Stats*>(s)->getThroughput(elapsed);
}

void StatsExportResultsToJson(StatsPtr s) {
    static_cast<Stats*>(s)->exportResultsToJson();
}

}
