#include "../include/FlashUnit.h"

#include <unordered_map>
#include <mutex>
#include <chrono>
#include <thread>

// Simple in-memory simulation of a flash unit used for testing and local runs.
class FlashPage {
public:
    std::vector<uint8_t> data;
    uint64_t epoch_written = 0;
    bool trimmed = false;
};

static std::mutex global_flash_mutex;
static std::unordered_map<uint64_t, FlashPage> global_flash_storage;
static uint64_t sealed_epoch = 0;

bool FlashUnit::write(uint64_t page, const std::vector<uint8_t>& data, uint64_t epoch, int timeout_ms) {
    // Simulate a small delay to mimic IO
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    std::lock_guard<std::mutex> lg(global_flash_mutex);
    if (epoch < sealed_epoch) return false; // cannot write below sealed epoch

    FlashPage& p = global_flash_storage[page];
    p.data = data;
    p.epoch_written = epoch;
    p.trimmed = false;
    return true;
}

std::vector<uint8_t> FlashUnit::read(uint64_t page, uint64_t epoch, int timeout_ms) {
    // Simulate read delay
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    std::lock_guard<std::mutex> lg(global_flash_mutex);
    auto it = global_flash_storage.find(page);
    if (it == global_flash_storage.end()) return {};
    if (it->second.trimmed) return {};
    return it->second.data;
}

void FlashUnit::trim(uint64_t page, uint64_t epoch, int timeout_ms) {
    std::lock_guard<std::mutex> lg(global_flash_mutex);
    auto it = global_flash_storage.find(page);
    if (it != global_flash_storage.end()) {
        it->second.trimmed = true;
        it->second.data.clear();
    }
}

void FlashUnit::fill_junk(uint64_t page, uint64_t epoch, int timeout_ms) {
    std::lock_guard<std::mutex> lg(global_flash_mutex);
    FlashPage& p = global_flash_storage[page];
    p.data = std::vector<uint8_t>(8, 0xFF); // small junk payload
    p.epoch_written = epoch;
    p.trimmed = false;
}

void FlashUnit::seal(uint64_t epoch, int timeout_ms) {
    std::lock_guard<std::mutex> lg(global_flash_mutex);
    if (epoch > sealed_epoch) sealed_epoch = epoch;
}
