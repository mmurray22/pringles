#include <chrono>
#include <vector>

/** WARNING: Not thread safe!!**/
// TODO: Add documentation and print statements
class Stats {
    public:
        Stats();
        ~Stats();


        //Throughput
        bool addOperations(uint64_t num_ops);
        void recordStartTput();
        void recordEndTput();
        double calculateTput();

        // Latency
        void recordStartLat();
        void recordEndLat();
        double calculatePointLatency();
        double calculateAvgLatency();
        double calculateMedLatency();


    private:
        // tput
        uint64_t total_operations;
        std::chrono::time_point<std::chrono::steady_clock> start_tput;
        std::chrono::time_point<std::chrono::steady_clock> end_tput;

        // lat
        std::chrono::time_point<std::chrono::steady_clock> start_lat;
        std::chrono::time_point<std::chrono::steady_clock> end_lat;
        std::vector<uint64_t> lats;
};
