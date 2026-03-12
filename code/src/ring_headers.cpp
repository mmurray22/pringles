#include "ring_headers.h"

// Payload size and num entries needs to be set manually
std::unique_ptr<struct ring_append_entry> create_ring_append_entry(uint32_t nonce, uint32_t cid) {
    std::unique_ptr<struct ring_append_entry> app_entry_hdr = std::make_unique<struct ring_append_entry>();
    app_entry_hdr.get()->nonce = nonce;
    app_entry_hdr.get()->cid = cid;
    return app_entry_hdr;
}


size_t get_ring_append_size() {
    return sizeof(struct ring_append_entry);
}

// Ethernet type
std::unique_ptr<struct ring_type> create_ring_type(uint16_t eth_type) {
    std::unique_ptr<struct ring_type> type_hdr = std::make_unique<struct ring_type>();
    type_hdr.get()->type = eth_type;
    return type_hdr;
}

size_t get_ring_type_size() {
    return sizeof(struct ring_type);
}


