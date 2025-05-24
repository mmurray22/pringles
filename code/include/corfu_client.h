#include "base_client.h"
#include "corfu_sequencer.h"
#include <vector>
#include <map>
#include <cstdint>
#include <cstddef>
#include <chrono>
#include "base_client.h"
#include "corfu_storage.h"

#define TIMEOUT 10
#define CORFU_PROTO_TYPE 2

class CorfuClient : public BaseClient {
    protected:
        CorfuSequencer sequencer;
        uint64_t get_tail();
        uint64_t curr_epoch = 0;
        
        std::vector<CorfuStorage> corfu_log;

        std::vector<std::map<std::pair<uint64_t, uint64_t>, std::vector<CorfuStorage>>> auxiliary;
        bool projection_sealed = false;

    public:
        CorfuClient(YAML::Node config);
        uint64_t append(std::unique_ptr<std::string> entry);
        std::unique_ptr<std::string> read(uint64_t log_idx);
        uint64_t fill(uint64_t log_idx);
        uint64_t trim(uint64_t log_idx);
}
