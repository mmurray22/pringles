/*
 * Core assumption: The ringclient protobuf has an object called Entry
 */
#include <vector>
#include <mutex>

#include "base_client.h"
 

/* Client class */
class LogClient : public BaseClient {

    public:
	    LogClient(std::string input_file, uint64_t cli_id);
        ~LogClient();

    private:
        /* Local list of appended and read log entries and corresponding lock*/
	    std::vector<LogEntry> cached_log_entries;
	    std::mutex cached_log_lock;
}
