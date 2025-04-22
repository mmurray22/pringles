#include "base_client.h"
#include "corfu_sequencer.h"

#define TIMEOUT 10

class CorfuClient : public BaseClient {
    protected:
        std::mutex local_lock;
        // std::map<uint64_t, std::string> personal_log;
        CorfuSequencer sequencer;
        uint64_t CorfuClient::get_tail();
        uint64_t curr_epoch = 0;

    public:
        CorfuClient(YAML::Node config);
        uint64_t CorfuClient::append(std::unique_ptr<std::string> entry);
        std::unique_ptr<std::string> SimpleClient::read(uint64_t idx);
        uint64_t CorfuClient::fill(uint64_t idx);
        uint64_t CorfuClient::trim(uint64_t idx);
}
