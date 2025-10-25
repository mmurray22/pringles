#include <mutex>
#include <vector>
#include <chrono>
#include <map>

class Stats {
	public:
	    Stats();
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

	    // Write to external file TODO

	private:
	    uint64_t numOps;
	    std::mutex numOps_lock;
		
	    std::map<int64_t, uint64_t> lat_map;
	    std::vector<uint64_t> latencies;

};
