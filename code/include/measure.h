#include <mutex>
#include <vector>
#include <chrono>
#include <map>
#include <string>

class Stats {
	public:
	    Stats(uint64_t batch_size, bool batch_on, std::string json_name, uint64_t thread_id);
	    ~Stats();

	    // Latency
	    void startLatTimer(uint64_t nonce);
	    bool endLatTimer(uint64_t nonce);
	    uint64_t get_duration();

	    //Throughput
	    void addOp();
	    uint64_t getTotalOps();
	    double getThroughput(uint64_t elapsed);
	    double getAvgLatency();
	    void exportResultsToJson();
	    // Write to external file TODO

	private:
	    uint64_t thread_id;

	    uint64_t numOps;
	    std::mutex numOps_lock;
		
	    std::map<int64_t, uint64_t> lat_map;
	    std::vector<uint64_t> latencies;

	    // Final values
	    uint64_t batch_size; // Size of the message batches
	    bool batch_on; // Boolean indicating whether batch is on
	    std::string json_name; // Filename for the stats json
	    double final_throughput; // ops/sec - total throughput for this thread
	    double final_avg_latency; // seconds - total avg latency
};
