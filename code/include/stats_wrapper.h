// Made with help from Google Gemini
#ifndef STATS_WRAPPER_H
#define STATS_WRAPPER_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handle to the C++ Stats object
typedef void* StatsPtr;

StatsPtr NewStats(uint64_t batch_size, bool batch_on, const char* json_name, uint64_t thread_id, const char* client_ip);
void FreeStats(StatsPtr s);

// Latency methods
void StatsStartLatTimer(StatsPtr s, uint64_t nonce);
bool StatsEndLatTimer(StatsPtr s, uint64_t nonce);
double StatsGetStartLat(StatsPtr s);
void StatsGetDuration(StatsPtr s, double start_time);
void StatsAddDuration(StatsPtr s, double start_time);
double StatsGetAvgLatency(StatsPtr s);

// Throughput methods
void StatsAddOp(StatsPtr s);
uint64_t StatsGetTotalOps(StatsPtr s);
double StatsGetThroughput(StatsPtr s, uint64_t elapsed);

// Export
void StatsExportResultsToJson(StatsPtr s);

#ifdef __cplusplus
}
#endif

#endif
