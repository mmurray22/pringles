#include "corfu_sequencer.h"
#include "network.h"
#include "corfusequencer.pb.h"
#include <thread>
#include <chrono>
#include <stdexcept>
#include <memory>

CorfuSequencer::CorfuSequencer(Network* network) 
    : network_(network), currentToken_(0), running_(false) {
    if (!network_) {
        throw std::runtime_error("Network instance required for CorfuSequencer");
    }
}

CorfuSequencer::~CorfuSequencer() {
    stop();
}

void CorfuSequencer::start() {
    running_ = true;
    std::thread service_thread(&CorfuSequencer::serviceLoop, this);
    service_thread.join(); // Block until service stops
}

void CorfuSequencer::stop() {
    running_ = false;
    network_->done(); // Signal network to stop
}

uint64_t CorfuSequencer::requestToken() {
    return currentToken_.fetch_add(1);
}

uint64_t CorfuSequencer::getHighestToken() const {
    uint64_t current = currentToken_.load();
    return current == 0 ? 0 : current - 1;
}

void CorfuSequencer::serviceLoop() {
    while (running_) {
        // Read incoming requests from network
        auto request_buf = network_->read_from_recv_queue();
        if (request_buf) {
            handleTokenRequest(*request_buf);
        }
        
        // Small delay to prevent busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void CorfuSequencer::handleTokenRequest(const std::string& request_data) {
    try {
        // Parse the incoming request
        corfusequencer::Payload request;
        if (!request.ParseFromString(request_data)) {
            // Invalid request, ignore
            return;
        }
        
        // Check if this is a token request (packet_type = 1)
        if (request.packet_type() == 1) {
            // Get next token
            uint64_t token = requestToken();
            
            // Create response
            corfusequencer::Payload response;
            response.set_packet_type(1);
            
            corfusequencer::SendToken* send_token = response.mutable_send_token();
            send_token->set_token(token);
            
            // Serialize response
            std::string serialized_response;
            if (response.SerializeToString(&serialized_response)) {
                // Send response back via network
                auto response_buf = std::make_unique<std::string>(serialized_response);
                network_->add_to_send_queue(std::move(response_buf), "client");
            }
        }
        
    } catch (const std::exception& e) {
        // Log error but continue serving
        // In a real implementation, you'd want proper logging
    }
}
