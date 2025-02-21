#include "structs.h"
#include <memory>

/*<template T>
std::unique_ptr<T> get_ptr_and_size(std::string pkt_type, size_t &size_of_hdr, int64_t cid = 0, int64_t nonce = 0) {
    if (protocol == 1) { // Corfu
        if (pkt_type == "get_seq_no") {
            size_of_hdr = get_sequence_num_size();
            return create_get_sequence_num(cid);
        }
    } else if (protocol == 2) { // Ringlog
        if (pkt_type == "append_req") {
            size_of_hdr = get_ring_append_entry_size();
            return create_ring_append_entry(nonce, cid);
        } else if (pkt_type == "append_resp") {
            size_of_hdr = get_ring_append_reply_size();
            return create_ring_append_reply(nonce, cid);
        }
    }
    return NULL;
}*/

size_t get_size_of_hdr(std::string pkt_type, uint64_t protocol_id) {
    if (protocol_id == 1) {
        if (pkt_type == "get_seq_no") {
            return sizeof(struct get_sequence_number);
        }
    } else if (protocol_id == 2) {
        if (pkt_type == "append_req") {
            return sizeof(struct ring_append_entry);
        } else if (pkt_type == "append_resp") {
            return sizeof(struct ring_append_success);
        }
    }
    return 0;
}

size_t get_size_of_hdr_int(int pkt_type, uint64_t protocol_id) {
    if (protocol_id == 1) {
        if (pkt_type == ETH_CLI_SEQ) {
            return sizeof(struct get_sequence_number);
        }
    } else if (protocol_id == 2) {
        if (pkt_type == ETH_APPEND_REQ) {
            return sizeof(struct ring_append_entry);
        } else if (pkt_type == ETH_APPEND_RESP) {
            return sizeof(struct ring_append_success);
        }
    }
    return 0;
}


int get_eth_type(std::string pkt_type, uint64_t protocol_id) {
    if (protocol_id == 1) {
        if (pkt_type == "get_seq_no") {
            return ETH_CLI_SEQ;
        }
    } else if (protocol_id == 2) {
        if (pkt_type == "append_req") {
            return ETH_APPEND_REQ;
        } else if (pkt_type == "append_resp") {
            return ETH_APPEND_RESP;
        }
    }
    return -1; // No ethernet type found
}

std::unique_ptr<struct get_sequence_number> create_get_sequence_num(int64_t cid) {
    std::unique_ptr<struct get_sequence_number> g_hdr = std::make_unique<struct get_sequence_number>();
    g_hdr.get()->cid = cid;
    return g_hdr;
}

std::unique_ptr<struct ring_append_entry> create_ring_append_entry(int64_t nonce, int64_t cid) {
    std::unique_ptr<struct ring_append_entry> app_entry_hdr = std::make_unique<struct ring_append_entry>();
    app_entry_hdr.get()->nonce = nonce;
    app_entry_hdr.get()->cid = cid;
    return app_entry_hdr;
}

std::unique_ptr<struct ring_append_success> create_ring_append_reply(int64_t nonce, int64_t cid) {
    std::unique_ptr<struct ring_append_success> app_reply_hdr = std::make_unique<struct ring_append_success>();
    app_reply_hdr.get()->nonce = nonce;
    app_reply_hdr.get()->cid = cid;
    return app_reply_hdr;
}
