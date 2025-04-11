// Corfu client
#include <vector>
#include <map>
#include <cstdint>
#include <cstddef>
#include "base_client.h"
#include "flash_id.h"

/*
 * CorfuClient
 *
 * This is a client class for the CORFU system. Clients can append and read
 * from the log, but they also have the responsibility of maintaining the log
 * by performing actions including fill, trim, reconfiguration, and ensuring
 * valid replication.
 */

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
        std::vector<FlashID> corfu_log;
        std::map<uint64_t, bool> junk;
        std::vector<std::map<std::pair<uint64_t, uint64_t>, std::vector<FlashID>>> auxiliary;
        uint64_t curr_epoch = 0;
        bool projection_sealed = false;

        /*** Timeouts ***/
        static constexpr uint64_t RECONFIGURATION_TIMEOUT 10
        static constexpr uint64_t READ_TIMEOUT 10
        static constexpr uint64_t APPEND_TIMEOUT 10
        static constexpr uint64_t TRIM_TIMEOUT 10
}