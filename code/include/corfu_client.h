#ifndef CORFU_CLIENT_H
#define CORFU_CLIENT_H

#include <vector>
#include <string>
#include <cstdint>
#include <map>
#include "Auxiliary.h"

// Forward declarations
class Sequencer;
class FlashUnit;

// Represents a mapping from log positions to flash pages
struct Projection {
    struct Extent {
        std::string flash_unit_id;
        uint64_t start_page;
        uint64_t length;
    };
    struct Range {
        uint64_t start;
        uint64_t end;
        std::vector<Extent> extents;
    };
    std::vector<Range> ranges;
    uint64_t epoch;
};

class CorfuClient {
public:
    CorfuClient(CorfuSequencer* sequencer, const Projection& projection, Auxiliary* auxiliary);
    // Append an entry, returns log position
    uint64_t append(const std::vector<uint8_t>& entry);
    // Read entry at log position
    std::vector<uint8_t> read(uint64_t position);
    // Trim log position
    void trim(uint64_t position);
    // Fill log position with junk
    void fill(uint64_t position);
    // Reconfigure projection (e.g., on failure)
    void reconfigure(const Projection& new_projection);
private:
    CorfuSequencer* sequencer_;
    Projection projection_;
    Auxiliary* auxiliary_;
    // Helper: map log position to flash pages
    std::vector<std::pair<FlashUnit*, uint64_t>> map_position(uint64_t position);
    // Helper: chain replication write
    bool chain_write(const std::vector<std::pair<FlashUnit*, uint64_t>>& pages, const std::vector<uint8_t>& data);
};

#endif // CORFU_CLIENT_H
