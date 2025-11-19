
#include <unordered_map>
#include <stdexcept>
#include <chrono>
#include <memory>

#include "corfu_client.h"
#include "base_sequencer.h"
#include "base_storage.h"
#include "network.h"
#include "corfusequencer.pb.h"
#include "corfustorage.pb.h"

// Static member definitions
Network* CorfuClient::global_network_ = nullptr;

// Helper function to create server endpoint string for network communication
std::string create_server_endpoint(const StorageServer& server) {
    return server.address + ":" + std::to_string(server.port);
}

// Initialize global network instance
void CorfuClient::initialize(Network* network) {
    global_network_ = network;
}

// Get the global network instance
// Note: This method is deprecated - network is now internal to CorfuClient

// Constructs a CorfuClient for single storage server deployment.
CorfuClient::CorfuClient(const Projection& projection, const StorageServer& storage_server,
                        const std::string& sequencer_ip, const std::string& auxiliary_ip)
    : projection_(projection), storage_server_(storage_server), sequencer_ip_(sequencer_ip), auxiliary_ip_(auxiliary_ip) {
    if (!global_network_) {
        throw std::runtime_error("CorfuClient::initialize() must be called before creating clients");
    }
    if (sequencer_ip.empty()) {
        throw std::runtime_error("sequencer_ip cannot be empty");
    }
    if (auxiliary_ip.empty()) {
        throw std::runtime_error("auxiliary_ip cannot be empty");
    }
    if (storage_server.address.empty()) {
        throw std::runtime_error("storage server address cannot be empty");
    }
    if (storage_server.port == 0) {
        throw std::runtime_error("storage server port cannot be zero");
    }
    
    // Create connection to the shared auxiliary service
    auxiliary_client_ = std::make_unique<AuxiliaryClient>(global_network_, auxiliary_ip);
}

// Requests a token from the remote sequencer via network communication
uint64_t CorfuClient::requestTokenFromSequencer() {
    // Create protobuf message to request token
    corfusequencer::Payload request;
    request.set_packet_type(1); // Token request type
    
    // Serialize the request
    std::string serialized_request;
    if (!request.SerializeToString(&serialized_request)) {
        throw std::runtime_error("Failed to serialize token request");
    }
    
    // Send request to sequencer via network
    auto request_buf = std::make_unique<std::string>(serialized_request);
    global_network_->add_to_send_queue(std::move(request_buf), "sequencer");
    
    // Wait for response from sequencer
    auto response_buf = global_network_->read_from_recv_queue();
    if (!response_buf) {
        throw std::runtime_error("Failed to receive token response from sequencer");
    }
    
    // Parse the response
    corfusequencer::Payload response;
    if (!response.ParseFromString(*response_buf)) {
        throw std::runtime_error("Failed to parse token response");
    }
    
    if (!response.has_send_token()) {
        throw std::runtime_error("Token response missing send_token field");
    }
    
    return response.send_token().token();
}

// Direct storage server communication methods

// Writes data to the storage server
bool CorfuClient::write_to_storage_server(uint64_t position, const std::vector<uint8_t>& data, uint64_t epoch, int timeout_ms) {
    // Create protobuf message for storage write
    corfustorage::WriteRequest request;
    request.set_page(position);  // Use position directly as page
    request.set_data(data.data(), data.size());
    request.set_epoch(epoch);
    request.set_timeout_ms(timeout_ms);
    
    // Serialize the request
    std::string serialized_request;
    if (!request.SerializeToString(&serialized_request)) {
        throw std::runtime_error("Failed to serialize write request");
    }
    
    // Send request to storage server via network
    std::string server_endpoint = create_server_endpoint(storage_server_);
    auto request_buf = std::make_unique<std::string>(serialized_request);
    global_network_->add_to_send_queue(std::move(request_buf), server_endpoint);
    
    // Wait for response from storage server
    auto response_buf = global_network_->read_from_recv_queue();
    if (!response_buf) {
        return false; // Network failure
    }
    
    // Parse the response
    corfustorage::WriteResponse response;
    if (!response.ParseFromString(*response_buf)) {
        return false; // Parse failure
    }
    
    return response.success();
}

// Reads data from the storage server
std::vector<uint8_t> CorfuClient::read_from_storage_server(uint64_t position, uint64_t epoch, int timeout_ms) {
    // Create protobuf message for storage read
    corfustorage::ReadRequest request;
    request.set_page(position);  // Use position directly as page
    request.set_epoch(epoch);
    request.set_timeout_ms(timeout_ms);
    
    // Serialize the request
    std::string serialized_request;
    if (!request.SerializeToString(&serialized_request)) {
        return {}; // Serialization failure
    }
    
    // Send request to storage server via network
    std::string server_endpoint = create_server_endpoint(storage_server_);
    auto request_buf = std::make_unique<std::string>(serialized_request);
    global_network_->add_to_send_queue(std::move(request_buf), server_endpoint);
    
    // Wait for response from storage server
    auto response_buf = global_network_->read_from_recv_queue();
    if (!response_buf) {
        return {}; // Network failure
    }
    
    // Parse the response
    corfustorage::ReadResponse response;
    if (!response.ParseFromString(*response_buf)) {
        return {}; // Parse failure
    }
    
    if (!response.success() || !response.has_data()) {
        return {}; // Read failure or no data
    }
    
    const std::string& data_str = response.data();
    return std::vector<uint8_t>(data_str.begin(), data_str.end());
}

// Trims a position on the storage server
void CorfuClient::trim_on_storage_server(uint64_t position, uint64_t epoch, int timeout_ms) {
    // Create protobuf message for storage trim
    corfustorage::TrimRequest request;
    request.set_page(position);  // Use position directly as page
    request.set_epoch(epoch);
    request.set_timeout_ms(timeout_ms);
    
    // Serialize and send (fire and forget for trim operations)
    std::string serialized_request;
    if (request.SerializeToString(&serialized_request)) {
        std::string server_endpoint = create_server_endpoint(storage_server_);
        auto request_buf = std::make_unique<std::string>(serialized_request);
        global_network_->add_to_send_queue(std::move(request_buf), server_endpoint);
    }
}

// Fills a position with junk on the storage server
void CorfuClient::fill_junk_on_storage_server(uint64_t position, uint64_t epoch, int timeout_ms) {
    // Create protobuf message for storage fill
    corfustorage::FillRequest request;
    request.set_page(position);  // Use position directly as page
    request.set_epoch(epoch);
    request.set_timeout_ms(timeout_ms);
    
    // Serialize and send (fire and forget for fill operations)
    std::string serialized_request;
    if (request.SerializeToString(&serialized_request)) {
        std::string server_endpoint = create_server_endpoint(storage_server_);
        auto request_buf = std::make_unique<std::string>(serialized_request);
        global_network_->add_to_send_queue(std::move(request_buf), server_endpoint);
    }
}

// Seals the storage server for a specific epoch
void CorfuClient::seal_storage_server(uint64_t epoch, int timeout_ms) {
    // Create protobuf message for storage seal
    corfustorage::SealRequest request;
    request.set_epoch(epoch);
    request.set_timeout_ms(timeout_ms);
    
    // Serialize and send (fire and forget for seal operations)
    std::string serialized_request;
    if (request.SerializeToString(&serialized_request)) {
        std::string server_endpoint = create_server_endpoint(storage_server_);
        auto request_buf = std::make_unique<std::string>(serialized_request);
        global_network_->add_to_send_queue(std::move(request_buf), server_endpoint);
    }
}

// Appends an entry to the shared log and returns the log position assigned.
// Uses the remote sequencer to get a new position and writes to the single storage server.
uint64_t CorfuClient::append(const std::vector<uint8_t>& entry) {
    uint64_t position = requestTokenFromSequencer();
    
    // Check if position is covered by any range mapping
    uint64_t flash_unit_id = projection_.get_flash_unit_for_position(position);
    if (flash_unit_id == 0) {
        throw std::runtime_error("Position " + std::to_string(position) + " not covered by any projection range mapping");
    }
    
    // Get the appropriate epoch for this position
    uint64_t epoch = projection_.get_epoch_for_position(position);
    
    // Write directly to the single storage server
    if (!write_to_storage_server(position, entry, epoch)) {
        reconfigure_storage_server();
        throw std::runtime_error("Append failed: storage server communication failure");
    }
    
    return position;
}

// Reads the entry at the given log position from the shared log.
// Reads directly from the single storage server.
std::vector<uint8_t> CorfuClient::read(uint64_t position) {
    // Check if position is covered by any range mapping
    uint64_t flash_unit_id = projection_.get_flash_unit_for_position(position);
    if (flash_unit_id == 0) {
        throw std::runtime_error("Position " + std::to_string(position) + " not covered by any projection range mapping");
    }
    
    // Get the appropriate epoch for this position
    uint64_t epoch = projection_.get_epoch_for_position(position);
    
    // Read directly from the single storage server
    auto value = read_from_storage_server(position, epoch);
    
    if (value.empty()) {
        // Could implement retry logic or reconfiguration here
        std::cerr << "Warning: Failed to read from storage server at position " << position << std::endl;
    }
    
    return value;
}

// Trims the given log position, marking it as no longer valid.
// Returns 0 on success, 1 on failure.
int CorfuClient::trim(uint64_t position) {
    // Check if position is covered by any range mapping
    uint64_t flash_unit_id = projection_.get_flash_unit_for_position(position);
    if (flash_unit_id == 0) {
        return 1; // Failure - position not covered by any range mapping
    }
    
    // Get the appropriate epoch for this position
    uint64_t epoch = projection_.get_epoch_for_position(position);
    
    trim_on_storage_server(position, epoch);
    return 0; // Success
}

// Fills the given log position with junk (used for hole-filling).
// Returns 0 on success, 1 on failure.
int CorfuClient::fill(uint64_t position) {
    // Check if position is covered by any range mapping
    uint64_t flash_unit_id = projection_.get_flash_unit_for_position(position);
    if (flash_unit_id == 0) {
        return 1; // Failure - position not covered by any range mapping
    }
    
    // Get the appropriate epoch for this position
    uint64_t epoch = projection_.get_epoch_for_position(position);
    
    fill_junk_on_storage_server(position, epoch);
    return 0; // Success
}

// Reconfigures the client by handling storage server failures.
void CorfuClient::reconfigure_storage_server() {
    // Seal the current storage server using global epoch
    seal_storage_server(projection_.global_epoch);
    
    // Increment global epoch for new configuration
    projection_.global_epoch = projection_.global_epoch + 1;
    
    // In a real implementation, this would:
    // 1. Query service discovery for a new storage server
    // 2. Update storage_server_ with new server details
    // 3. Coordinate with auxiliary service for new projection
    // 4. Update range mappings with new flash unit assignments
    
    std::cout << "Storage server reconfiguration triggered at epoch " << projection_.global_epoch << std::endl;
}
