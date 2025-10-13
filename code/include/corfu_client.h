
#include "base_client.h"
#include "corfu_sequencer.h"
#include <vector>
#include <map>
#include <cstdint>
#include <cstddef>
#include <chrono>
#include "base_client.h"
#include "corfu_storage.h"

#define CORFU_APPEND_PROTO_TYPE 1
#define CORFU_READ_PROTO_TYPE 2
#define CORFU_TRIM_PROTO_TYPE 3
#define CORFU_SEAL_PROTO_TYPE 4
#define CORFU_GETTOKEN_PROTO_TYPE 5

#define CORFU_ACK_PROTO_TYPE 6
#define CORFU_SEALED_PROTO_TYPE 7
#define CORFU_UNWRITTEN_PROTO_TYPE 8
#define CORFU_WRITTEN_PROTO_TYPE 9
#define CORFU_STORE_READ_PROTO_TYPE 10
#define CORFU_STORE_SEAL_PROTO_TYPE 11
#define CORFU_DELETED_PROTO_TYPE 12

#define CORFU_GETTOKEN_REPLY_PROTO_TYPE 13

#define TIMEOUT 10
#define CORFU_PROTO_TYPE 2

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
        uint64_t append(std::unique_ptr<std::string> entry) override;
        std::unique_ptr<std::string> read(uint64_t log_idx) override;
        uint64_t fill(uint64_t log_idx) override;
        uint64_t trim(uint64_t log_idx) override;
        uint64_t getTail() override;
        void subscribe(uint64_t idx) override;
}