#ifndef CORFU_SEQUENCER_H
#define CORFU_SEQUENCER_H

#include <cstdint>
#include <vector>
#include <string>
#include <mutex>
#include <atomic>

// Forward declaration
class Network;

class CorfuSequencer {
public:
    CorfuSequencer(Network* network);
    ~CorfuSequencer();

    // Start the sequencer service (blocks and listens for requests)
    void start();
    
    // Stop the sequencer service
    void stop();

    // Returns the next available token and increments (thread-safe)
    uint64_t requestToken();

    // Returns the highest token issued (currentToken - 1)
    uint64_t getHighestToken() const;

private:
    Network* network_;
    std::atomic<uint64_t> currentToken_;
    std::atomic<bool> running_;
    
    // Main service loop that handles incoming requests
    void serviceLoop();
    
    // Handle a single token request
    void handleTokenRequest(const std::string& request_data);
};

#endif // CORFU_SEQUENCER_H
