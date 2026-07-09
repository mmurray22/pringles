#include <mutex>
#include <vector>
#include <chrono>
#include <unordered_map>
#include <string>
#include <atomic>

class Stats {
	public:
	    Stats(uint64_t batch_size, bool batch_on, std::string json_name, uint64_t thread_id, std::string client_ip);
	    ~Stats();

	    // Latency
	    void startLatTimer(uint64_t nonce);
	    bool endLatTimer(uint64_t nonce);
	    uint64_t get_duration();

	    double getStartLat();
            void getDuration(double start_time);
	    void addDuration(double dur);


	    //Throughput
	    void addOp();
	    uint64_t getTotalOps();
	    double getThroughput(uint64_t elapsed);
	    double getAvgLatency();
	    void putSubLatency(double sub_lat);
	    void exportResultsToJson();

		void dumpAllLatencies();

	private:
	    uint64_t thread_id;
	    std::string client_ip;

	    std::atomic<uint64_t> numOps;
	    //std::mutex num_ops_lock;
	    
            std::mutex lat_map_lock;	    
	    std::unordered_map<int64_t, double> lat_map;
	    std::vector<double> latencies;

	    // Final values
	    uint64_t batch_size; // Size of the message batches
	    bool batch_on; // Boolean indicating whether batch is on
	    std::string json_name; // Filename for the stats json
	    double final_throughput; // ops/sec - total throughput for this thread
	    double final_avg_latency; // milliseconds - total avg latency
	    double subscribe_latency; // milliseconds - total avg latency
};
