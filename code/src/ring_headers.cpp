#include "ring_headers.h"

// Payload size and num entries needs to be set manually
std::unique_ptr<struct ring_append_entry> create_ring_append_entry(uint32_t nonce, uint32_t cid) {
    std::unique_ptr<struct ring_append_entry> app_entry_hdr = std::make_unique<struct ring_append_entry>();
    app_entry_hdr.get()->nonce = nonce;
    app_entry_hdr.get()->cid = cid;
    return app_entry_hdr;
}
