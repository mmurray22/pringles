#ifndef CORFU_CLIENT_H
#define CORFU_CLIENT_H

#include <vector>
#include <string>
#include <cstdint>
#include <map>
#include <memory>
#include "Auxiliary.h"

// Forward declarations
class Network;

// Represents the single storage server endpoint
struct StorageServer {
    std::string address;
    uint16_t port;
    std::string server_id;
};

// Range mapping to flash units
struct RangeMapping {
    uint64_t start_position;
    uint64_t end_position;
    uint64_t flash_unit_id;
    uint64_t epoch;
};

// Simplified projection for single storage server
struct Projection {
    std::vector<RangeMapping> range_mappings;  // Multiple ranges mapping to flash units
    uint64_t global_epoch;  // Overall projection epoch
    
    // Helper method to find which flash unit handles a position
    uint64_t get_flash_unit_for_position(uint64_t position) const {
        for (const auto& mapping : range_mappings) {
            if (position >= mapping.start_position && position <= mapping.end_position) {
                return mapping.flash_unit_id;
            }
        }
        return 0;  // Default flash unit if no mapping found
    }
    
    // Helper method to get epoch for a position
    uint64_t get_epoch_for_position(uint64_t position) const {
        for (const auto& mapping : range_mappings) {
            if (position >= mapping.start_position && position <= mapping.end_position) {
                return mapping.epoch;
            }
        }
        return global_epoch;  // Fall back to global epoch
    }
};

class CorfuClient {
public:
    // Initialize global network (call once per process)
    static void initialize(Network* network);
    
    // Constructor for single storage server deployment
    CorfuClient(const Projection& projection, const StorageServer& storage_server, 
               const std::string& sequencer_ip, const std::string& auxiliary_ip);
    
    // Append an entry, returns log position
    uint64_t append(const std::vector<uint8_t>& entry);
    // Read entry at log position
    std::vector<uint8_t> read(uint64_t position);
    // Trim log position (returns 0 on success, 1 on failure)
    int trim(uint64_t position);
    // Fill log position with junk (returns 0 on success, 1 on failure)
    int fill(uint64_t position);
    
private:
    static Network* global_network_;
    
    Projection projection_;
    StorageServer storage_server_;  // Single storage server
    std::string sequencer_ip_;
    std::string auxiliary_ip_;
    std::unique_ptr<AuxiliaryClient> auxiliary_client_;  // Connection to shared auxiliary service
    
    // Request a token from the shared sequencer service
    uint64_t requestTokenFromSequencer();
    // Reconfigure projection via shared auxiliary service
    void reconfigure_storage_server();
    
    // Direct storage server communication methods
    bool write_to_storage_server(uint64_t position, const std::vector<uint8_t>& data, uint64_t epoch, int timeout_ms = 100);
    std::vector<uint8_t> read_from_storage_server(uint64_t position, uint64_t epoch, int timeout_ms = 100);
    void trim_on_storage_server(uint64_t position, uint64_t epoch, int timeout_ms = 100);
    void fill_junk_on_storage_server(uint64_t position, uint64_t epoch, int timeout_ms = 100);
    void seal_storage_server(uint64_t epoch, int timeout_ms = 100);
};

#endif // CORFU_CLIENT_H
