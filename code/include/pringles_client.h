/*
 * Core assumption: The ringclient protobuf has an object called Entry
 */
#include <vector>
#include <queue>
#include <map>
#include <mutex>

#include "base_client.h"
#include "ringclient.h"
#include "ring_headers.h"

#define MAX_WAIT_TIME 5

/* Client class */
class LogClient : public BaseClient {
    public:
	LogClient(std::string input_file, uint64_t cli_id);
	~LogClient();
        
	// Append entries to the log
	uint64_t append(std::string entry);
        // Read from idx in the log
	std::unique_ptr<std::string> read(uint64_t idx);
        // Get latest committed entry
        uint64_t getTail();
        // Subscribe to get all log updates after supplied index
        void subscribe(uint64_t idx);
        // Garbage collect all log entries up to some index
        bool trim(uint64_t idx);


    private:
	uint64_t wait_for_append(std::unique_ptr<std::string> entry);       
        void log_updates(uint64_t idx);
	std::unique_ptr<std::string> create_pkt();
	ringclient::Payload deser_pkt(std::unique_ptr<std::string> pkt);
	void read_pringles_recv_queue();

	/* Receive queue which slots messages */
	std::map<PacketType, std::queue<ringclient::Payload>> pkt_q;
	std::vector<std::string> pkt_types;
	bool end_thread = false;

	/* Hash/ID of pending append entries */
        std::vector<uint64_t> pending_append_entries;
        std::vector<uint64_t> pending_read_entries;

        /* Local list of appended and read log entries and corresponding lock*/
	std::vector<LogEntry> cached_log_entries;
	std::mutex cached_log_lock;

}
