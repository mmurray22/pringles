#include "corfu_storage.h"
#include <iostream>
#include <chrono>
#include <thread>

const uint64_t MAX_WAIT_TIME = 100;

CorfuStorage::CorfuStorage(uint64_t ssid) : BaseStorage(ssid) {
    // Constructor - server will be started separately
    std::cout << "CorfuStorage initialized with ID: " << ssid << std::endl;
    
    // Create default flash unit
    auto default_flash = std::make_shared<FlashUnit>();
    add_flash_unit(current_flash_unit_id_, default_flash);
}

CorfuStorage::~CorfuStorage() {
    stop_server();
}

bool CorfuStorage::sync_store(uint64_t idx, std::string entry) {
    std::lock_guard<std::mutex> lock(storage_mutex_);
    std::vector<uint8_t> data(entry.begin(), entry.end());
    auto flash_unit = get_flash_unit_for_position(idx);
    if (flash_unit) {
        uint64_t page = position_to_page(idx);
        return flash_unit->write(page, data, s_epoch);
    }
    return false;
}

bool CorfuStorage::lazy_store(uint64_t idx, std::string entry) {
    return sync_store(idx, entry);
}

std::string CorfuStorage::get(uint64_t idx) {
    std::lock_guard<std::mutex> lock(storage_mutex_);
    auto flash_unit = get_flash_unit_for_position(idx);
    if (flash_unit) {
        uint64_t page = position_to_page(idx);
        auto data = flash_unit->read(page, s_epoch);
        return std::string(data.begin(), data.end());
    }
    return "";
}

void CorfuStorage::add_flash_unit(const std::string& unit_id, std::shared_ptr<FlashUnit> flash_unit) {
    std::lock_guard<std::mutex> lock(storage_mutex_);
    flash_units_[unit_id] = flash_unit;
    std::cout << "Added flash unit: " << unit_id << std::endl;
}

void CorfuStorage::remove_flash_unit(const std::string& unit_id) {
    std::lock_guard<std::mutex> lock(storage_mutex_);
    flash_units_.erase(unit_id);
    std::cout << "Removed flash unit: " << unit_id << std::endl;
}

uint64_t CorfuStorage::position_to_page(uint64_t position) const {
    // Simple 1:1 mapping from position to page
    // In a real implementation, this could implement more complex mapping schemes
    return position;
}

std::shared_ptr<FlashUnit> CorfuStorage::get_flash_unit_for_position(uint64_t position) {
    // Simple implementation: use the current active flash unit
    // In a real implementation, this could implement load balancing, 
    // wear leveling, or other distribution strategies
    auto it = flash_units_.find(current_flash_unit_id_);
    if (it != flash_units_.end()) {
        return it->second;
    }
    return nullptr;
}

void CorfuStorage::start_server(const std::string& address, uint16_t port) {
    net_ = std::make_shared<Network>();
    terminate_ = false;
    
    // Bind to network address and start server thread
    net_->bind_server(address, port);
    server_thread_ = std::thread(&CorfuStorage::server, this, net_);
    
    std::cout << "CorfuStorage server started on " << address << ":" << port << std::endl;
    std::cout << "Storage server will write data to flash units" << std::endl;
}

void CorfuStorage::stop_server() {
    terminate_ = true;
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
}

void CorfuStorage::server(std::shared_ptr<Network> net) {
    std::cout << "CorfuStorage server loop starting..." << std::endl;
    
    while (!terminate_) {
        try {
            // Wait for incoming request
            auto request_buf = net->read_from_recv_queue();
            if (!request_buf) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            
            // For now, we'll use "client" as the client ID
            // In a real implementation, this would come from the network layer
            std::string client_id = "client";
            
            // Try to parse as different request types and handle them
            if (is_request_type(*request_buf, "WriteRequest")) {
                handle_write_request(*request_buf, client_id);
            } else if (is_request_type(*request_buf, "ReadRequest")) {
                handle_read_request(*request_buf, client_id);
            } else if (is_request_type(*request_buf, "TrimRequest")) {
                handle_trim_request(*request_buf, client_id);
            } else if (is_request_type(*request_buf, "FillRequest")) {
                handle_fill_request(*request_buf, client_id);
            } else if (is_request_type(*request_buf, "SealRequest")) {
                handle_seal_request(*request_buf, client_id);
            } else {
                std::cerr << "Unknown request type received" << std::endl;
            }
            
        } catch (const std::exception& e) {
            std::cerr << "Error in server loop: " << e.what() << std::endl;
        }
    }
    
    std::cout << "CorfuStorage server loop terminated" << std::endl;
}

void CorfuStorage::handle_write_request(const std::string& request_data, const std::string& client_id) {
    corfustorage::WriteRequest request;
    if (!request.ParseFromString(request_data)) {
        std::cerr << "Failed to parse WriteRequest" << std::endl;
        return;
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex_);
    corfustorage::WriteResponse response;
    
    uint64_t position = request.page();  // Position from client
    uint64_t epoch = request.epoch();
    
    // Check if epoch is sealed
    if (epoch < s_epoch) {
        response.set_success(false);
        response.set_error_message("Epoch sealed: current=" + std::to_string(s_epoch) + 
                                  ", requested=" + std::to_string(epoch));
    } else {
        // Get flash unit for this position
        auto flash_unit = get_flash_unit_for_position(position);
        if (!flash_unit) {
            response.set_success(false);
            response.set_error_message("No flash unit available for position");
        } else {
            uint64_t page = position_to_page(position);
            std::string data_str = request.data();
            std::vector<uint8_t> data(data_str.begin(), data_str.end());
            
            // Write to flash unit
            if (flash_unit->write(page, data, epoch)) {
                response.set_success(true);
                std::cout << "Wrote position " << position << " to flash page " << page 
                          << " (epoch " << epoch << ", " << data.size() << " bytes)" << std::endl;
            } else {
                response.set_success(false);
                response.set_error_message("Flash write failed");
            }
        }
    }
    
    // Send response
    std::string response_data;
    response.SerializeToString(&response_data);
    send_response(response_data, client_id);
}

void CorfuStorage::handle_read_request(const std::string& request_data, const std::string& client_id) {
    corfustorage::ReadRequest request;
    if (!request.ParseFromString(request_data)) {
        std::cerr << "Failed to parse ReadRequest" << std::endl;
        return;
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex_);
    corfustorage::ReadResponse response;
    
    uint64_t position = request.page();  // Position from client
    uint64_t epoch = request.epoch();
    
    // Check if epoch is sealed
    if (epoch < s_epoch) {
        response.set_success(false);
        response.set_error_message("Epoch sealed");
    } else {
        // Get flash unit for this position
        auto flash_unit = get_flash_unit_for_position(position);
        if (!flash_unit) {
            response.set_success(false);
            response.set_error_message("No flash unit available for position");
        } else {
            uint64_t page = position_to_page(position);
            auto data = flash_unit->read(page, epoch);
            
            if (!data.empty()) {
                response.set_success(true);
                response.set_data(data.data(), data.size());
                response.set_epoch_written(epoch);
                std::cout << "Read position " << position << " from flash page " << page 
                          << " (" << data.size() << " bytes)" << std::endl;
            } else {
                response.set_success(false);
                response.set_error_message("Data not found or trimmed");
            }
        }
    }
    
    // Send response
    std::string response_data;
    response.SerializeToString(&response_data);
    send_response(response_data, client_id);
}

void CorfuStorage::handle_trim_request(const std::string& request_data, const std::string& client_id) {
    corfustorage::TrimRequest request;
    if (!request.ParseFromString(request_data)) {
        std::cerr << "Failed to parse TrimRequest" << std::endl;
        return;
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex_);
    
    uint64_t position = request.page();  // Position from client
    uint64_t epoch = request.epoch();
    
    // Get flash unit for this position
    auto flash_unit = get_flash_unit_for_position(position);
    if (flash_unit) {
        uint64_t page = position_to_page(position);
        flash_unit->trim(page, epoch);
        std::cout << "Trimmed position " << position << " (flash page " << page << ")" << std::endl;
    }
    
    // Send ack
    corfustorage::TrimResponse response;
    response.set_success(true);
    
    std::string response_data;
    response.SerializeToString(&response_data);
    send_response(response_data, client_id);
}

void CorfuStorage::handle_fill_request(const std::string& request_data, const std::string& client_id) {
    corfustorage::FillRequest request;
    if (!request.ParseFromString(request_data)) {
        std::cerr << "Failed to parse FillRequest" << std::endl;
        return;
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex_);
    
    uint64_t position = request.page();  // Position from client
    uint64_t epoch = request.epoch();
    
    // Get flash unit for this position
    auto flash_unit = get_flash_unit_for_position(position);
    if (flash_unit) {
        uint64_t page = position_to_page(position);
        flash_unit->fill_junk(page, epoch);
        std::cout << "Filled position " << position << " (flash page " << page << ") with junk (epoch " << epoch << ")" << std::endl;
    }
    
    // Send ack
    corfustorage::FillResponse response;
    response.set_success(true);
    
    std::string response_data;
    response.SerializeToString(&response_data);
    send_response(response_data, client_id);
}

void CorfuStorage::handle_seal_request(const std::string& request_data, const std::string& client_id) {
    corfustorage::SealRequest request;
    if (!request.ParseFromString(request_data)) {
        std::cerr << "Failed to parse SealRequest" << std::endl;
        return;
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex_);
    
    uint64_t epoch = request.epoch();
    if (epoch > s_epoch) {
        s_epoch = epoch;
        
        // Seal all flash units
        for (auto& [unit_id, flash_unit] : flash_units_) {
            flash_unit->seal(epoch);
        }
        
        std::cout << "Sealed all flash units at epoch " << s_epoch << std::endl;
    }
    
    // Send response with current sealed epoch
    corfustorage::SealResponse response;
    response.set_success(true);
    response.set_sealed_epoch(s_epoch);
    
    std::string response_data;
    response.SerializeToString(&response_data);
    send_response(response_data, client_id);
}

void CorfuStorage::send_response(const std::string& response_data, const std::string& client_id) {
    auto response_buf = std::make_unique<std::string>(response_data);
    net_->add_to_send_queue(std::move(response_buf), client_id);
}

bool CorfuStorage::is_request_type(const std::string& request_data, const std::string& message_type) {
    // Simple heuristic: try to parse as the expected message type
    if (message_type == "WriteRequest") {
        corfustorage::WriteRequest req;
        return req.ParseFromString(request_data);
    } else if (message_type == "ReadRequest") {
        corfustorage::ReadRequest req;
        return req.ParseFromString(request_data);
    } else if (message_type == "TrimRequest") {
        corfustorage::TrimRequest req;
        return req.ParseFromString(request_data);
    } else if (message_type == "FillRequest") {
        corfustorage::FillRequest req;
        return req.ParseFromString(request_data);
    } else if (message_type == "SealRequest") {
        corfustorage::SealRequest req;
        return req.ParseFromString(request_data);
    }
    return false;
}