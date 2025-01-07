/*
 * Core assumption: The ringclient protobuf has an object called Entry
 */
#include <vector>
#include <mutex>

#include "base_client.h"
#include "ringclient.pb.h" 

/* Client class */
class LogClient extends BaseClient {
public:
	/*** Basic Log API functions ***/
	
	/*
	 * Instantiates ring log client:
     * - ingest all info from the yaml file 
     * - setup networking
     * - generate client id
	 */
	LogClient(YAML::Node config);

	/*
	 * Append entries to the log
	 */
	uint64_t append(LogEntry log_entry);

	/*
	 * Read entries from the log
	 */
	LogEntry read(uint64_t idx);

	/*
	 * Gets latest committed log index
	 */
	LogEntry getTail();

	/*
	 * Subscribe to getting updates for all log
	 * entry additions after the supplied index
	 */
	void subscribe(uint64_t idx);

	/*** Additional Log stream functionality ***/
	///TODO
    ///

private:
	// Immutable
	/* Unique client identifier */
	uint64_t client_id;

	// Mutable
	/* ID of client's contact switch*/
	uint64_t switch_id;
	std::mutex switch_lock;

	/* Local list of appended and read log entries and corresponding lock*/
	std::vector<LogEntry> cached_log_entries;
	std::mutex log_lock;

    /*Networking information*/
    std::queue<char* buf> send_pkt;
    std::queue<char* buf> rcv_pkt;

	// Functions
    void send_packet();
	void receive_packets();
}
