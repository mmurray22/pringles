#include <string>
#include <cstdint>
#include <unordered_map>
#include <memory>
#include <thread>
#include <mutex>
#include "base_storage.h"
#include "network.h"
#include "corfustorage.pb.h"
#include "FlashUnit.h"

class CorfuStorage : public BaseStorage {
    public:
        CorfuStorage(uint64_t ssid);
        ~CorfuStorage();

        bool sync_store(uint64_t idx, std::string entry) override;
        bool lazy_store(uint64_t idx, std::string entry) override;
        std::string get(uint64_t idx) override;

        // Flash unit management
        void add_flash_unit();
        void remove_flash_unit(uint64_t unit_id);

        // Protocol-compatible request handlers
        void handle_write_request(const std::string& request_data, const std::string& client_id);
        void handle_read_request(const std::string& request_data, const std::string& client_id);
        void handle_trim_request(const std::string& request_data, const std::string& client_id);
        void handle_fill_request(const std::string& request_data, const std::string& client_id);
        void handle_seal_request(const std::string& request_data, const std::string& client_id);

        // Main server loop
        void server(std::shared_ptr<Network> net);

        // Server control
        void start_server(const std::string& address, uint16_t port);
        void stop_server();

    private:
        // Flash unit registry
        std::unordered_map<uint64_t, std::shared_ptr<FlashUnit>> flash_units_;
        uint64_t next_unit_id_ = 0;
        
        // Position to page mapping
        uint64_t position_to_page(uint64_t position) const;
        std::shared_ptr<FlashUnit> get_flash_unit_for_position(uint64_t position);
        
        uint64_t s_epoch = 0;      // Sealed epoch
        mutable std::mutex storage_mutex_;
        
        // Network and threading
        std::shared_ptr<Network> net_;
        std::thread server_thread_;
        bool terminate_ = false;
        
        // Helper methods
        void send_response(const std::string& response_data, const std::string& client_id);
        bool is_request_type(const std::string& request_data, const std::string& message_type);
};