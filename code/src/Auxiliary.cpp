// Auxiliary.cpp
// Implements Auxiliary class for CORFU projection log

#include "Auxiliary.h"
#include <sstream>
#include <iostream>

Auxiliary::Auxiliary(const std::string& filename) : filename_(filename) {}

// Writes a new projection for the given epoch to the auxiliary log.
// Returns false if a projection for this epoch already exists, true if written successfully.
bool Auxiliary::write_projection(uint64_t epoch, const Projection& proj) {
    Projection tmp;
    if (read_projection(epoch, tmp)) return false;
    std::ofstream out(filename_, std::ios::app);
    if (!out) return false;
    out << epoch << " " << serialize(proj) << "\n";
    return true;
}

// Reads the projection for the given epoch from the auxiliary log.
// Returns true and fills proj if found, false otherwise.
bool Auxiliary::read_projection(uint64_t epoch, Projection& proj) {
    std::ifstream in(filename_);
    if (!in) return false;
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream iss(line);
        uint64_t e;
        iss >> e;
        if (e == epoch) {
            std::string data;
            std::getline(iss, data);
            return deserialize(data, proj);
        }
    }
    return false;
}

// Returns the highest epoch number stored in the auxiliary log.
uint64_t Auxiliary::get_latest_epoch() const {
    std::ifstream in(filename_);
    if (!in) return 0;
    uint64_t latest = 0;
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream iss(line);
        uint64_t e;
        iss >> e;
        if (e > latest) latest = e;
    }
    return latest;
}

// Serializes a Projection object into a string for storage in the auxiliary log.
// Stores epoch, number of ranges, and for each range: start, end, extents.
std::string Auxiliary::serialize(const Projection& proj) const {
    std::ostringstream oss;
    oss << proj.epoch << " " << proj.ranges.size();
    for (const auto& range : proj.ranges) {
        oss << " " << range.start << " " << range.end << " " << range.extents.size();
        for (const auto& ext : range.extents) {
            oss << " " << ext.flash_unit_id << " " << ext.start_page << " " << ext.length;
        }
    }
    return oss.str();
}

// Deserializes a string from the auxiliary log into a Projection object.
// Returns true if successful, false otherwise.
bool Auxiliary::deserialize(const std::string& data, Projection& proj) const {
    std::istringstream iss(data);
    iss >> proj.epoch;
    size_t num_ranges;
    iss >> num_ranges;
    proj.ranges.clear();
    for (size_t i = 0; i < num_ranges; ++i) {
        Projection::Range r;
        size_t num_ext;
        iss >> r.start >> r.end >> num_ext;
        for (size_t j = 0; j < num_ext; ++j) {
            Projection::Extent ext;
            iss >> ext.flash_unit_id >> ext.start_page >> ext.length;
            r.extents.push_back(ext);
        }
        proj.ranges.push_back(r);
    }
    return true;
}
