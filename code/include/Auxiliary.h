// Auxiliary.h
// Manages the sequence of projections (epochs) for CORFU reconfiguration.
// Simple file-based implementation for durability.

#ifndef AUXILIARY_H
#define AUXILIARY_H

#include <string>
#include <vector>
#include <fstream>
#include "corfu_client.h"

class Auxiliary {
public:
    Auxiliary(const std::string& filename);
    // Write a new projection at the given epoch (if not already present)
    bool write_projection(uint64_t epoch, const Projection& proj);
    // Read the projection at the given epoch
    bool read_projection(uint64_t epoch, Projection& proj);
    // Get the latest epoch stored
    uint64_t get_latest_epoch() const;
private:
    std::string filename_;
    // Helper to serialize/deserialize Projection
    std::string serialize(const Projection& proj) const;
    bool deserialize(const std::string& data, Projection& proj) const;
};

#endif // AUXILIARY_H
