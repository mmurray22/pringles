
#include "base_client.h"
#include "corfu_sequencer.h"
#include <vector>
#include <map>
#include <cstdint>
#include <cstddef>
#include <chrono>
#include "corfu_storage.h"
#include "trace.h"

#define TIMEOUT 10

class CorfuClient : public BaseClient {
    protected:
        CorfuSequencer sequencer;
        uint64_t curr_epoch = 0;

        std::vector<std::map<std::pair<uint64_t, uint64_t>, std::vector<CorfuStorage>>> auxiliary;
        bool projection_sealed = false;

    public:
        CorfuClient(YAML::Node config);
        ~CorfuClient();

        void reconfigure(uint64_t log_idx, CorfuStorage failing_unit);
        uint64_t append(std::unique_ptr<std::string> entry);
        std::unique_ptr<std::string> read(uint64_t log_idx) override;
        uint64_t fill(uint64_t log_idx);
        bool trim(uint64_t log_idx) override;
        uint64_t getTail() override;
        void subscribe(uint64_t idx) override;
};