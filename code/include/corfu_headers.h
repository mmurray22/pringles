#pragma once

#include <memory>
#include <cstdint>

/*New ethernet header types*/
#define CORFU_APPEND_PROTO_TYPE 1
#define CORFU_READ_PROTO_TYPE 2
#define CORFU_TRIM_PROTO_TYPE 3
#define CORFU_SEAL_PROTO_TYPE 4
#define CORFU_GETTOKEN_PROTO_TYPE 5

#define CORFU_ACK_PROTO_TYPE 6
#define CORFU_SEALED_PROTO_TYPE 7
#define CORFU_UNWRITTEN_PROTO_TYPE 8
#define CORFU_WRITTEN_PROTO_TYPE 9
#define CORFU_STORE_READ_PROTO_TYPE 10
#define CORFU_STORE_SEAL_PROTO_TYPE 11
#define CORFU_DELETED_PROTO_TYPE 12

#define CORFU_GETTOKEN_REPLY_PROTO_TYPE 13

// The header
// REQUIRED FOR ALL PACKETS
struct corfu_cli_header {
    uint16_t proto_type;
    uint32_t client_id;
    uint32_t thread_id;
};

struct corfu_seq_header {
    uint16_t proto_type;
};

struct corfu_append {
    uint32_t log_idx;
    uint32_t curr_epoch;
    uint32_t entry_size;
};

struct corfu_read {
    uint32_t log_idx;
    uint32_t curr_epoch;
};

struct corfu_trim {
    uint32_t log_idx;
};

struct corfu_seal {
    uint32_t curr_epoch;
};

struct corfu_gettoken_reply {
    uint32_t log_idx;
};

inline size_t get_corfu_gettoken_reply_size() {
    return sizeof(struct corfu_gettoken_reply);
}

inline size_t get_corfu_cli_header_size() {
    return sizeof(struct corfu_cli_header);
}

inline size_t get_corfu_seq_header_size() {
    return sizeof(struct corfu_seq_header);
}