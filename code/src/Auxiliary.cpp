// Auxiliary.cpp
// Implements simple append-only auxiliary core + network service layer

#include "Auxiliary.h"
#include "network.h"
#include "auxiliary.pb.h"
#include <stdexcept>
#include <sstream>
#include <iostream>
#include <thread>
#include <chrono>
#include <memory>

// Constructor for simple in-memory auxiliary
Auxiliary::Auxiliary() 
    : latest_epoch_(0), network_(nullptr), running_(false), is_service_mode_(false) {}

// Constructor for network service mode  
Auxiliary::Auxiliary(Network* network, const std::string& filename)
    : latest_epoch_(0), network_(network), filename_(filename), running_(false), is_service_mode_(true) {
    if (!network_) {
        throw std::runtime_error("Network instance required for Auxiliary service mode");
    }
}

Auxiliary::~Auxiliary() {
    stop();
}

void Auxiliary::start() {
    if (!is_service_mode_) {
        throw std::runtime_error("start() can only be called in service mode");
    }
    running_ = true;
    std::thread service_thread(&Auxiliary::serviceLoop, this);
    service_thread.join(); // Block until service stops
}

void Auxiliary::stop() {
    if (is_service_mode_) {
        running_ = false;
        if (network_) {
            network_->done(); // Signal network to stop
        }
    }
}

// ===== Core Auxiliary Operations (Simple Append-Only List) =====

uint64_t Auxiliary::append_projection(const Projection& proj) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Assign the next sequential epoch
    uint64_t new_epoch = latest_epoch_ + 1;
    
    // Create a copy of the projection with the correct epoch
    Projection new_proj = proj;
    new_proj.epoch = new_epoch;
    
    // Append to the list
    projections_.push_back(new_proj);
    latest_epoch_ = new_epoch;
    
    return new_epoch;
}

bool Auxiliary::get_projection(uint64_t epoch, Projection& proj) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if epoch is valid
    if (epoch == 0 || epoch > latest_epoch_) {
        return false;
    }
    
    // Get projection (epochs are 1-indexed, vector is 0-indexed)
    proj = projections_[epoch - 1];
    return true;
}

uint64_t Auxiliary::get_latest_epoch() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return latest_epoch_;
}

// ===== Network Service Layer =====

void Auxiliary::serviceLoop() {
    while (running_) {
        // Read incoming requests from network
        auto request_buf = network_->read_from_recv_queue();
        if (request_buf) {
            handleRequest(*request_buf);
        }
        
        // Small delay to prevent busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void Auxiliary::handleRequest(const std::string& request_data) {
    try {
        // Parse the incoming request
        auxiliary::Payload request;
        if (!request.ParseFromString(request_data)) {
            return; // Invalid request, ignore
        }
        
        switch (request.packet_type()) {
            case 1: // Append projection request
                handleAppendProjectionRequest(request_data);
                break;
            case 2: // Get projection request
                handleGetProjectionRequest(request_data);
                break;
            case 3: // Get latest epoch request
                handleGetLatestEpochRequest(request_data);
                break;
            default:
                // Unknown packet type, ignore
                break;
        }
        
    } catch (const std::exception& e) {
        // Log error but continue serving
    }
}

void Auxiliary::handleAppendProjectionRequest(const std::string& request_data) {
    auxiliary::Payload request;
    if (!request.ParseFromString(request_data) || !request.has_append_projection_request()) {
        return;
    }
    
    const auto& append_req = request.append_projection_request();
    
    // Convert protobuf projection to internal format
    Projection proj;
    protoToProjection(append_req.projection(), proj);
    
    // Perform the append operation using core auxiliary
    uint64_t assigned_epoch = append_projection(proj);
    
    // Send response
    auxiliary::Payload response;
    response.set_packet_type(1);
    
    auxiliary::AppendProjectionResponse* append_resp = response.mutable_append_projection_response();
    if (assigned_epoch > 0) {
        append_resp->set_success(true);
        append_resp->set_assigned_epoch(assigned_epoch);
    } else {
        append_resp->set_success(false);
        append_resp->set_error_message("Failed to append projection");
    }
    
    // Serialize and send response
    std::string serialized_response;
    if (response.SerializeToString(&serialized_response)) {
        auto response_buf = std::make_unique<std::string>(serialized_response);
        network_->add_to_send_queue(std::move(response_buf), "client");
    }
}

void Auxiliary::handleGetProjectionRequest(const std::string& request_data) {
    auxiliary::Payload request;
    if (!request.ParseFromString(request_data) || !request.has_get_projection_request()) {
        return;
    }
    
    const auto& get_req = request.get_projection_request();
    
    // Perform the get operation using core auxiliary
    Projection proj;
    bool success = get_projection(get_req.epoch(), proj);
    
    // Send response
    auxiliary::Payload response;
    response.set_packet_type(2);
    
    auxiliary::GetProjectionResponse* get_resp = response.mutable_get_projection_response();
    get_resp->set_success(success);
    
    if (success) {
        auxiliary::Projection* proto_proj = get_resp->mutable_projection();
        projectionToProto(proj, *proto_proj);
    } else {
        get_resp->set_error_message("Projection not found for this epoch");
    }
    
    // Serialize and send response
    std::string serialized_response;
    if (response.SerializeToString(&serialized_response)) {
        auto response_buf = std::make_unique<std::string>(serialized_response);
        network_->add_to_send_queue(std::move(response_buf), "client");
    }
}

void Auxiliary::handleGetLatestEpochRequest(const std::string& request_data) {
    auxiliary::Payload request;
    if (!request.ParseFromString(request_data) || !request.has_get_latest_epoch_request()) {
        return;
    }
    
    // Perform the operation using core auxiliary
    uint64_t latest_epoch = get_latest_epoch();
    
    // Send response
    auxiliary::Payload response;
    response.set_packet_type(3);
    
    auxiliary::GetLatestEpochResponse* epoch_resp = response.mutable_get_latest_epoch_response();
    epoch_resp->set_latest_epoch(latest_epoch);
    
    // Serialize and send response
    std::string serialized_response;
    if (response.SerializeToString(&serialized_response)) {
        auto response_buf = std::make_unique<std::string>(serialized_response);
        network_->add_to_send_queue(std::move(response_buf), "client");
    }
}

// Convert internal Projection to protobuf Projection
void Auxiliary::projectionToProto(const Projection& proj, auxiliary::Projection& proto_proj) const {
    proto_proj.set_epoch(proj.epoch);
    proto_proj.clear_ranges();
    
    for (const auto& range : proj.ranges) {
        auxiliary::Range* proto_range = proto_proj.add_ranges();
        proto_range->set_start(range.start);
        proto_range->set_end(range.end);
        
        for (const auto& extent : range.extents) {
            auxiliary::Extent* proto_extent = proto_range->add_extents();
            proto_extent->set_flash_unit_id(extent.flash_unit_id);
            proto_extent->set_start_page(extent.start_page);
            proto_extent->set_length(extent.length);
        }
    }
}

// Convert protobuf Projection to internal Projection
void Auxiliary::protoToProjection(const auxiliary::Projection& proto_proj, Projection& proj) const {
    proj.epoch = proto_proj.epoch();
    proj.ranges.clear();
    
    for (const auto& proto_range : proto_proj.ranges()) {
        Projection::Range range;
        range.start = proto_range.start();
        range.end = proto_range.end();
        
        for (const auto& proto_extent : proto_range.extents()) {
            Projection::Extent extent;
            extent.flash_unit_id = proto_extent.flash_unit_id();
            extent.start_page = proto_extent.start_page();
            extent.length = proto_extent.length();
            range.extents.push_back(extent);
        }
        proj.ranges.push_back(range);
    }
}

// ===== AuxiliaryClient Implementation (Network Client) =====

AuxiliaryClient::AuxiliaryClient(Network* network, const std::string& auxiliary_ip)
    : network_(network), auxiliary_ip_(auxiliary_ip) {
    if (!network_) {
        throw std::runtime_error("Network instance required for AuxiliaryClient");
    }
}

uint64_t AuxiliaryClient::append_projection(const Projection& proj) {
    try {
        // Create request
        auxiliary::Payload request;
        request.set_packet_type(1);
        
        auxiliary::AppendProjectionRequest* append_req = request.mutable_append_projection_request();
        
        auxiliary::Projection* proto_proj = append_req->mutable_projection();
        
        // Convert internal projection to protobuf
        proto_proj->set_epoch(proj.epoch);
        for (const auto& range : proj.ranges) {
            auxiliary::Range* proto_range = proto_proj->add_ranges();
            proto_range->set_start(range.start);
            proto_range->set_end(range.end);
            
            for (const auto& extent : range.extents) {
                auxiliary::Extent* proto_extent = proto_range->add_extents();
                proto_extent->set_flash_unit_id(extent.flash_unit_id);
                proto_extent->set_start_page(extent.start_page);
                proto_extent->set_length(extent.length);
            }
        }
        
        // Serialize and send request
        std::string serialized_request;
        if (!request.SerializeToString(&serialized_request)) {
            return 0;
        }
        
        std::string response_data = sendRequest(serialized_request);
        if (response_data.empty()) {
            return 0;
        }
        
        // Parse response
        auxiliary::Payload response;
        if (!response.ParseFromString(response_data) || !response.has_append_projection_response()) {
            return 0;
        }
        
        const auto& append_resp = response.append_projection_response();
        if (append_resp.success()) {
            return append_resp.assigned_epoch();
        }
        
        return 0;
        
    } catch (const std::exception& e) {
        return 0;
    }
}

bool AuxiliaryClient::get_projection(uint64_t epoch, Projection& proj) const {
    try {
        // Create request
        auxiliary::Payload request;
        request.set_packet_type(2);
        
        auxiliary::GetProjectionRequest* get_req = request.mutable_get_projection_request();
        get_req->set_epoch(epoch);
        
        // Serialize and send request
        std::string serialized_request;
        if (!request.SerializeToString(&serialized_request)) {
            return false;
        }
        
        std::string response_data = sendRequest(serialized_request);
        if (response_data.empty()) {
            return false;
        }
        
        // Parse response
        auxiliary::Payload response;
        if (!response.ParseFromString(response_data) || !response.has_get_projection_response()) {
            return false;
        }
        
        const auto& get_resp = response.get_projection_response();
        if (!get_resp.success()) {
            return false;
        }
        
        // Convert protobuf projection to internal format
        const auto& proto_proj = get_resp.projection();
        proj.epoch = proto_proj.epoch();
        proj.ranges.clear();
        
        for (const auto& proto_range : proto_proj.ranges()) {
            Projection::Range range;
            range.start = proto_range.start();
            range.end = proto_range.end();
            
            for (const auto& proto_extent : proto_range.extents()) {
                Projection::Extent extent;
                extent.flash_unit_id = proto_extent.flash_unit_id();
                extent.start_page = proto_extent.start_page();
                extent.length = proto_extent.length();
                range.extents.push_back(extent);
            }
            proj.ranges.push_back(range);
        }
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

uint64_t AuxiliaryClient::get_latest_epoch() const {
    try {
        // Create request
        auxiliary::Payload request;
        request.set_packet_type(3);
        
        auxiliary::GetLatestEpochRequest* epoch_req = request.mutable_get_latest_epoch_request();
        // No parameters needed
        
        // Serialize and send request
        std::string serialized_request;
        if (!request.SerializeToString(&serialized_request)) {
            return 0;
        }
        
        std::string response_data = sendRequest(serialized_request);
        if (response_data.empty()) {
            return 0;
        }
        
        // Parse response
        auxiliary::Payload response;
        if (!response.ParseFromString(response_data) || !response.has_get_latest_epoch_response()) {
            return 0;
        }
        
        return response.get_latest_epoch_response().latest_epoch();
        
    } catch (const std::exception& e) {
        return 0;
    }
}

std::string AuxiliaryClient::sendRequest(const std::string& serialized_request) const {
    // Send request to auxiliary service via network
    auto request_buf = std::make_unique<std::string>(serialized_request);
    network_->add_to_send_queue(std::move(request_buf), "auxiliary");
    
    // Wait for response from auxiliary service
    auto response_buf = network_->read_from_recv_queue();
    if (!response_buf) {
        return "";
    }
    
    return *response_buf;
}
