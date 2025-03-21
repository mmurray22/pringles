// Corfu client
#include <map>
#include "base_client.h"

/*
 * CorfuClient
 *
 * This is a client class for the CORFU system. Clients can append and read
 * from the log, but they also have the responsibility of maintaining the log
 * by performing actions including fill, trim, reconfiguration, and ensuring
 * valid replication.
 */

#define TIMEOUT 10

class CorfuClient : public BaseClient {
    public:
        CorfuClient(YAML::Node config);
        std::byte read();
        uint64_t append();
        uint64_t fill();
        uint64_t trim();

    private:
        /*** Functions ***/
        uint64_t get_tail();
        uint64_t reconfigure();

        /*** Local Shared State ***/

        /*** Timeouts ***/
        std::mutex local_lock;
        std::map<uint64_t, std::string> personal_log;
}