#include "structs.h"
#include <unique_ptr>

std::unique_ptr<struct get_sequence_number> create_get_sequence_num(int64_t cid) {
    std::unique_ptr<struct get_sequence_number> g_hdr = std::make_unique<struct get_sequence_number>();
    g_hdr.get()->cid = cid;
    return g_hdr;
}

size_t get_sequence_num_size() {
    return sizeof struct get_sequence_number;  
}

std::unique_ptr<struct ring_append_entry> create_ring_append_entry(int64_t nonce, int64_t cid) {
    std::unique_ptr<struct ring_append_entry> app_entry_hdr = std::make_unique<struct ring_append_entry>();
    app_entry_hdr.get()->nonce = nonce;
    app_entry_hdr.get()->cid = cid;
    return app_entry_hdr;
}

size_t get_ring_append_entry_size() {
    return sizeof struct ring_append_entry;
}

std::unique_ptr<struct ring_append_success> create_ring_append_reply(int64_t nonce, int64_t cid) {
    std::unique_ptr<struct ring_append_success> app_reply_hdr = std::make_unique<struct ring_append_success>();
    app_reply_hdr.get()->nonce = nonce;
    app_reply_hdr.get()->cid = cid;
    return app_reply_hdr;
}

size_t get_ring_append_reply_size() {
    return sizeof struct ring_append_success;
}
