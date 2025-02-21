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

    private:
        // Cached log entries 
        std::mutex local_lock;
        std::map<uint64_t, std::string> personal_log;
}
