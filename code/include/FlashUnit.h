// Lightweight FlashUnit interface used by CorfuClient.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

class FlashUnit {
public:
    FlashUnit() = default;
    ~FlashUnit() = default;

    // Write a page. Returns true on success, false on failure.
    bool write(uint64_t page, const std::vector<uint8_t>& data, uint64_t epoch, int timeout_ms = 100);

    // Read a page. Returns an empty vector when no data is present or on timeout.
    std::vector<uint8_t> read(uint64_t page, uint64_t epoch, int timeout_ms = 100);

    // Trim a page (mark as invalid).
    void trim(uint64_t page, uint64_t epoch, int timeout_ms = 100);

    // Fill a page with junk (hole filling).
    void fill_junk(uint64_t page, uint64_t epoch, int timeout_ms = 100);

    // Seal the unit for the given epoch (prevent further writes at lower epochs).
    void seal(uint64_t epoch, int timeout_ms = 100);
};
