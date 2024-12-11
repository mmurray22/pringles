// Simple client
#include <map>
#include "base_client.h"

/*
 * SimpleClient
 *
 * This is a basic client class which is used largely for testing 
 * the logging infrastructure. The client adds string objects to the 
 * log.
 */
class SimpleClient : public BaseClient {
    public:
        SimpleClient(YAML::Node config);
        std::unique_ptr<std::string> create_entry(std::string content);

    private:
        // This is a simulation of the remote log which should be stored elsewhere
        std::mutex remote_log_lock;
        std::map<uint64_t, std::string> remote_log;

        // Cached log entries 
        std::mutex local_lock;
        std::map<uint64_t, std::string> personal_log;
}
