#include <vector>
#include <mutex>

template<class Entry>
class LogClient {
public:
	/*** Basic Log API functions ***/

	/*
	 * Append entries to the log
	 */
	template<class Entry>
	uint64_t append(Entry log_entry);

	/*
	 * Read entries from the log
	 */
	template<class Entry>
	Entry read(uint64_t idx);

	/*
	 * Gets latest committed log index
	 */
	template<class Entry>
	Entry getTail();

	/*
	 * Subscribe to getting updates for all log
	 * entry additions after the supplied index
	 */
	void subscribe(uint64_t idx);

	/*** Additional Log stream functionality ***/
	///TODO
	
private:
	// Immutable
	/* Unique client identifier */
	uint64_t client_id;

	// Mutable
	/* ID of client's contact switch*/
	uint64_t switch_id;
	std::mutex switch_lock;
	/* Current view number and corresponding lock */
	uint64_t view_num;
	std::mutex view_num_lock;
	/* Local list of appended and read log entries and corresponding lock*/
	std::vector<class Entry> cached_log_entries;
	std::mutex log_lock;

	// Functions
	void receive_packets();
	void change_view(uint64_t new_view_num);
}
