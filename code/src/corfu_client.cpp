
#include <unordered_map>
#include <stdexcept>
#include <chrono>

#include "corfu_client.h"
#include "base_sequencer.h"
#include "base_storage.h"
#include "corfu_sequencer.h"

// Example registry for flash units
// Maps flash unit IDs to FlashUnit pointers for lookup during reconfiguration.
static std::unordered_map<std::string, FlashUnit*> flash_unit_registry;

// Helper to get FlashUnit* by ID
// Returns a pointer to the FlashUnit with the given ID, or nullptr if not found.
FlashUnit* get_flash_unit(const std::string& id) {
    auto it = flash_unit_registry.find(id);
    if (it != flash_unit_registry.end()) {
        return it->second;
    }
    return nullptr;
}

// Dummy FlashUnit
class FlashUnit {
public:
    bool write(uint64_t page, const std::vector<uint8_t>& data, uint64_t epoch, int timeout_ms = 100);
    std::vector<uint8_t> read(uint64_t page, uint64_t epoch, int timeout_ms = 100);
    void trim(uint64_t page, uint64_t epoch, int timeout_ms = 100);
    void fill_junk(uint64_t page, uint64_t epoch, int timeout_ms = 100);
    void seal(uint64_t epoch, int timeout_ms = 100);
};


// Constructs a CorfuClient with a given sequencer, projection mapping, and auxiliary log.
CorfuClient::CorfuClient(CorfuSequencer* sequencer, const Projection& projection, Auxiliary* auxiliary)
    : sequencer_(sequencer), projection_(projection), auxiliary_(auxiliary) {}

// Appends an entry to the shared log and returns the log position assigned.
// Uses the sequencer to get a new position, maps it to flash pages, and writes using chain replication.
uint64_t CorfuClient::append(const std::vector<uint8_t>& entry) {
    uint64_t position = sequencer_->requestToken();
    auto pages = map_position(position);
    std::vector<std::string> failed_units;
    bool chain_success = true;
    for (auto& p : pages) {
        if (!p.first->write(p.second, entry, projection_.epoch)) {
            chain_success = false;
            // Collect failed unit id for reconfiguration
            // If FlashUnit does not have id, use mapping from extent
            // Here, assume extent.flash_unit_id is available
            // Find the corresponding extent
            for (const auto& range : projection_.ranges) {
                for (const auto& extent : range.extents) {
                    if (get_flash_unit(extent.flash_unit_id) == p.first) {
                        failed_units.push_back(extent.flash_unit_id);
                    }
                }
            }
        }
    }
    if (!chain_success) {
        reconfigure(failed_units);
        throw std::runtime_error("Append failed: reconfiguration triggered due to unit failure");
    }
    return position;
}

// Reads the entry at the given log position from the shared log.
// If replication is incomplete, repairs the chain by copying the value from the written prefix to unwritten suffix.
std::vector<uint8_t> CorfuClient::read(uint64_t position) {
    auto pages = map_position(position);
    int timeout_ms = 100;
    std::vector<std::string> failed_units;
    // Try to read from the last unit in chain for durability
    auto& last = pages.back();
    std::vector<uint8_t> value;
    auto start = std::chrono::steady_clock::now();
    while (true) {
        value = last.first->read(last.second, projection_.epoch, timeout_ms);
        if (!value.empty()) break;
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count() > timeout_ms) {
            break;
        }
    }
    // If last replica failed, collect its id
    if (value.empty()) {
        for (const auto& range : projection_.ranges) {
            for (const auto& extent : range.extents) {
                if (get_flash_unit(extent.flash_unit_id) == last.first) {
                    failed_units.push_back(extent.flash_unit_id);
                }
            }
        }
    }
    // Optionally, check other replicas for persistent failures (for repair/hole-filling)
    for (size_t i = 0; i < pages.size() - 1; ++i) {
        std::vector<uint8_t> prefix_value;
        auto start_prefix = std::chrono::steady_clock::now();
        while (true) {
            prefix_value = pages[i].first->read(pages[i].second, projection_.epoch, timeout_ms);
            if (!prefix_value.empty()) break;
            auto now_prefix = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::milliseconds>(now_prefix - start_prefix).count() > timeout_ms) {
                break;
            }
        }
        if (prefix_value.empty()) {
            for (const auto& range : projection_.ranges) {
                for (const auto& extent : range.extents) {
                    if (get_flash_unit(extent.flash_unit_id) == pages[i].first) {
                        failed_units.push_back(extent.flash_unit_id);
                    }
                }
            }
        }
    }
    if (!failed_units.empty()) {
        reconfigure(failed_units);
        throw std::runtime_error("Read failed: reconfiguration triggered due to unit failure");
    }
    if (value.empty()) {
        // ...existing code for repair and hole-filling...
        for (size_t i = 0; i < pages.size() - 1; ++i) {
            std::vector<uint8_t> prefix_value;
            auto start_prefix = std::chrono::steady_clock::now();
            while (true) {
                prefix_value = pages[i].first->read(pages[i].second, projection_.epoch, timeout_ms);
                if (!prefix_value.empty()) break;
                auto now_prefix = std::chrono::steady_clock::now();
                if (std::chrono::duration_cast<std::chrono::milliseconds>(now_prefix - start_prefix).count() > timeout_ms) {
                    break;
                }
            }
            if (!prefix_value.empty()) {
                // Repair unwritten suffix by copying value to unwritten replicas
                for (size_t j = i + 1; j < pages.size(); ++j) {
                    std::vector<uint8_t> suffix_value;
                    auto start_suffix = std::chrono::steady_clock::now();
                    while (true) {
                        suffix_value = pages[j].first->read(pages[j].second, projection_.epoch, timeout_ms);
                        if (!suffix_value.empty()) break;
                        auto now_suffix = std::chrono::steady_clock::now();
                        if (std::chrono::duration_cast<std::chrono::milliseconds>(now_suffix - start_suffix).count() > timeout_ms) {
                            break;
                        }
                    }
                    if (suffix_value.empty()) {
                        pages[j].first->write(pages[j].second, prefix_value, projection_.epoch, timeout_ms);
                    }
                }
                return prefix_value;
            }
        }
        // If no valid value found, fill with junk
        for (auto& p : pages) {
            std::vector<uint8_t> v;
            auto start_junk = std::chrono::steady_clock::now();
            while (true) {
                v = p.first->read(p.second, projection_.epoch, timeout_ms);
                if (!v.empty()) break;
                auto now_junk = std::chrono::steady_clock::now();
                if (std::chrono::duration_cast<std::chrono::milliseconds>(now_junk - start_junk).count() > timeout_ms) {
                    break;
                }
            }
            if (v.empty()) {
                p.first->fill_junk(p.second, projection_.epoch, timeout_ms);
            }
        }
        return std::vector<uint8_t>(); // Indicate hole filled with junk
    }
    return value;
}

// Trims the given log position, marking it as no longer valid in all replicas.
void CorfuClient::trim(uint64_t position) {
    auto pages = map_position(position);
    for (auto& p : pages) {
        p.first->trim(p.second, projection_.epoch);
    }
}

// Fills the given log position with junk in all replicas (used for hole-filling).
void CorfuClient::fill(uint64_t position) {
    auto pages = map_position(position);
    for (auto& p : pages) {
        p.first->fill_junk(p.second, projection_.epoch);
    }
}

// Reconfigures the client by building a new projection, replacing failed units with available units from the registry.
// Takes a list of failed flash unit IDs.
void CorfuClient::reconfigure(const std::vector<std::string>& failed_units) {
    // Build a set of available units (not failed)
    std::unordered_map<std::string, FlashUnit*> available_units = flash_unit_registry;
    for (const auto& id : failed_units) {
        available_units.erase(id);
    }

    // Seal only the failed units in the old projection
    for (const auto& range : projection_.ranges) {
        for (const auto& extent : range.extents) {
            if (std::find(failed_units.begin(), failed_units.end(), extent.flash_unit_id) != failed_units.end()) {
                FlashUnit* unit = get_flash_unit(extent.flash_unit_id);
                if (unit) {
                    unit->seal(projection_.epoch);
                }
            }
        }
    }

    // Build new projection by replacing failed units with available ones
    Projection new_projection = projection_;
    new_projection.epoch = projection_.epoch + 1;
    for (auto& range : new_projection.ranges) {
        for (auto& extent : range.extents) {
            if (std::find(failed_units.begin(), failed_units.end(), extent.flash_unit_id) != failed_units.end()) {
                // Replace with an available unit
                if (!available_units.empty()) {
                    extent.flash_unit_id = available_units.begin()->first;
                    available_units.erase(available_units.begin());
                } else {
                    // No available units left, remove extent
                    extent.flash_unit_id = "";
                }
            }
        }
        // Remove any extents with empty flash_unit_id
        range.extents.erase(
            std::remove_if(range.extents.begin(), range.extents.end(), [](const Projection::Extent& ext) {
                return ext.flash_unit_id.empty();
            }),
            range.extents.end()
        );
    }

    // Try to write the new projection to the auxiliary log
    if (!auxiliary_->write_projection(new_projection.epoch, new_projection)) {
        // If another client already wrote it, read the existing projection
        Projection aux_proj;
        if (auxiliary_->read_projection(new_projection.epoch, aux_proj)) {
            projection_ = aux_proj;
            return;
        }
    }
    // Switch to the new projection
    projection_ = new_projection;
}

// Maps a logical log position to the corresponding flash pages and units using the current projection.
// Uses round-robin mapping for replication within a range.
std::vector<std::pair<FlashUnit*, uint64_t>> CorfuClient::map_position(uint64_t position) {
    for (const auto& range : projection_.ranges) {
        if (position >= range.start && position < range.end) {
            uint64_t rel_pos = position - range.start;
            std::vector<std::pair<FlashUnit*, uint64_t>> result;
            // Round-robin mapping for replication
            for (size_t i = 0; i < range.extents.size(); ++i) {
                const auto& extent = range.extents[i];
                uint64_t page = extent.start_page + (rel_pos / range.extents.size());
                FlashUnit* unit = get_flash_unit(extent.flash_unit_id);
                result.push_back({unit, page});
            }
            return result;
        }
    }
    throw std::runtime_error("Position not mapped in projection");
}

// Writes data to a set of flash pages using chain replication.
// Writes in order, waiting for each to succeed before moving to the next.
// Returns true if all writes succeed, false if any fail.
bool CorfuClient::chain_write(const std::vector<std::pair<FlashUnit*, uint64_t>>& pages, const std::vector<uint8_t>& data) {
    for (auto& p : pages) {
        if (!p.first->write(p.second, data, projection_.epoch)) {
            return false; // Overwrite or error
        }
    }
    return true;
}
