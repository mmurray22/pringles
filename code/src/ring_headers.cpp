#include "ring_headers.h"

std::unique_ptr<struct ring_append_entry> create_ring_append_entry(uint32_t nonce, uint32_t cid, uint64_t payload_size) {
    std::unique_ptr<struct ring_append_entry> app_entry_hdr = std::make_unique<struct ring_append_entry>();
    app_entry_hdr.get()->nonce = nonce;
    app_entry_hdr.get()->cid = cid;
    app_entry_hdr.get()->payload_size = payload_size;
    return app_entry_hdr;
}
