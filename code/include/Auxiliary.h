// Auxiliary.h
// Manages an append-only sequence of projections (epochs) for CORFU reconfiguration.
// Core: Simple in-memory list with epoch-based indexing.
// Service: Network layer for distributed access.

#ifndef AUXILIARY_H
#define AUXILIARY_H

#include <vector>
#include <mutex>
#include <atomic>
#include <string>
#include "corfu_client.h"

// Forward declaration
class Network;

class Auxiliary {
public:
    // Constructor for simple in-memory auxiliary
    Auxiliary();
    
    // Constructor for network service mode
    Auxiliary(Network* network, const std::string& filename);
    
    ~Auxiliary();
    
    // Start auxiliary network service (for service mode) - blocks and serves requests
    void start();
    
    // Stop auxiliary network service
    void stop();
    
    // Core operations - simple append-only list
    uint64_t append_projection(const Projection& proj);
    bool get_projection(uint64_t epoch, Projection& proj) const;
    uint64_t get_latest_epoch() const;
    
private:
    // Core data (always present)
    std::vector<Projection> projections_;  // Indexed by (epoch - 1)
    uint64_t latest_epoch_;               // Highest epoch number stored
    mutable std::mutex mutex_;            // Thread-safe access
    
    // Network service data (only for service mode)
    Network* network_;
    std::string filename_;
    std::atomic<bool> running_;
    bool is_service_mode_;
    
    // Service mode methods
    void serviceLoop();
    void handleRequest(const std::string& request_data);
    void handleAppendProjectionRequest(const std::string& request_data);
    void handleGetProjectionRequest(const std::string& request_data);
    void handleGetLatestEpochRequest(const std::string& request_data);
    
    // Convert between internal Projection and protobuf Projection
    void projectionToProto(const Projection& proj, class auxiliary::Projection& proto_proj) const;
    void protoToProjection(const class auxiliary::Projection& proto_proj, Projection& proj) const;
};

// Client-side auxiliary proxy for network communication
class AuxiliaryClient {
public:
    AuxiliaryClient(Network* network, const std::string& auxiliary_ip);
    
    // Same interface as core Auxiliary but sends requests over network
    uint64_t append_projection(const Projection& proj);
    bool get_projection(uint64_t epoch, Projection& proj) const;
    uint64_t get_latest_epoch() const;
    
private:
    Network* network_;
    std::string auxiliary_ip_;
    
    // Helper methods for network communication
    std::string sendRequest(const std::string& serialized_request) const;
};

#endif // AUXILIARY_H
