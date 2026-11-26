/*
 * ringworld_hdrs.h — C wire-format mirror of 64_bit_headers.p4 for the DPDK sequencer.
 *
 * Every multi-byte field is BIG-ENDIAN on the wire. Read with ntohs/ntohl/be64toh,
 * write with htons/htonl/htobe64 (or the rte_be_to_cpu_* / rte_cpu_to_be_* equivalents).
 * All Ringworld headers are byte-aligned, so plain packed structs match P4 exactly.
 * The _Static_asserts at the bottom pin each size to the P4 definition.
 *
 * Packet layouts (from the P4 parser):
 *   Control:      eth(type 0x0820) | rw_control | rw_canary
 *   View change:  eth(type 0x0890/0x0900/0x0910) | <view hdr> | rw_canary
 *   Data path:    eth(type 0x0800) | ipv4 | udp | rw_ring_type | <type-specific hdr> | rw_canary
 *                 (unknown ring types: rw_ring_type | rw_canary)
 */
#ifndef RINGWORLD_HDRS_H
#define RINGWORLD_HDRS_H

#include <stdint.h>
#include <arpa/inet.h>   /* ntohl, htonl */
#include <endian.h>      /* be64toh, htobe64 */
#include <string.h>      /* memcpy */

/* ---- EtherTypes (outer Ethernet type field) ---- */
#define RW_ETH_CONTROL        0x0820
#define RW_ETH_VIEW_INDUCE    0x0890
#define RW_ETH_VIEW_CHANGE    0x0900   /* parsed as trigger_view */
#define RW_ETH_VIEW_ACK       0x0910   /* parsed as finalize_view */
#define RW_ETH_IPV4           0x0800

/* ---- ring_type.type values ---- */
#define RW_TYPE_APPEND_PRE    0x0863
#define RW_TYPE_APPEND        0x0860
#define RW_TYPE_APPEND_WAIT   0x0862
#define RW_TYPE_APPEND_RESP   0x0861
#define RW_TYPE_READ          0x0870
#define RW_TYPE_READ_RESP     0x0871
#define RW_TYPE_TAIL          0x0840
#define RW_TYPE_SUB           0x0850
#define RW_TYPE_SUB_RESP      0x0851
#define RW_TYPE_VIEW_TRIGGER  0x0891

#define RW_CANARY_VALUE       0xA5
#define RW_MAX_INFLIGHT_ACKS  65536u
#define RW_MAX_GSN_MAP_SIZE   65535u
#define RW_MAX_GSN_MAP_IDX    65534u

/* ---- Control packet (follows Ethernet directly) : 14 bytes ---- */
struct rw_control {
    uint32_t upper_global_seq_no;
    uint32_t lower_global_seq_no;
    uint32_t temp_local_seq_no;
    uint16_t ring_view;
} __attribute__((packed));

/* ---- Ring type (after UDP) : 8 bytes ---- */
struct rw_ring_type {
    uint16_t type;
    uint16_t shard_id;
    uint16_t pipe_ts;
    uint16_t start_ts;
} __attribute__((packed));

/* ---- Append entry : 26 bytes ---- */
struct rw_append {
    uint32_t stream_id;
    uint32_t upper_g_idx;
    uint32_t lower_g_idx;
    uint32_t local_seq_cntr;
    uint16_t cntrl_pkt_it;
    uint8_t  exp_type;
    uint8_t  seq_recirc;
    uint32_t start_ts;
    uint16_t payload_size;
} __attribute__((packed));

/* ---- Read entry : 22 bytes ---- */
struct rw_read {
    uint16_t nonce;
    uint32_t payload_size;
    uint32_t stream_id;
    uint64_t g_idx;
    uint16_t ring_view;
    uint16_t recv_port;
} __attribute__((packed));

/* ---- Tail request : 14 bytes ---- */
struct rw_tail {
    uint16_t nonce;
    uint8_t  hops;
    uint8_t  start_switch_id;
    uint32_t upper_tail_seq_no;
    uint32_t lower_tail_seq_no;
    uint16_t recv_port;
} __attribute__((packed));

/* ---- Subscribe request : 12 bytes ---- */
struct rw_subscribe {
    uint32_t g_idx;
    uint32_t stream_id;
    uint16_t recv_port;
    uint16_t subscribe_port;
} __attribute__((packed));

/* ---- View change headers ---- */
struct rw_induce_view {            /* 1 byte */
    uint8_t drop_cntrl;
} __attribute__((packed));

struct rw_trigger_view {           /* 13 bytes */
    uint8_t  leader_switch_id;
    uint16_t current_ring_view;
    uint8_t  view_member_switch_id;
    uint32_t highest_seen_upper_seq_no;
    uint32_t highest_seen_lower_seq_no;
    uint8_t  hops;
} __attribute__((packed));

struct rw_start_view {             /* 8 bytes (declared in P4, never parsed by ingress) */
    uint8_t  leader_switch_id;
    uint16_t current_ring_view;
    uint8_t  view_member_switch_id;
    uint32_t highest_seen_seq_no;
} __attribute__((packed));

struct rw_finalize_view {          /* 14 bytes */
    uint8_t  leader_switch_id;
    uint8_t  view_member_switch_id;
    uint16_t current_ring_view;
    uint8_t  next_switch_id;
    uint32_t highest_quorum_upper_seq_no;
    uint32_t highest_quorum_lower_seq_no;
    uint8_t  hops;
} __attribute__((packed));

struct rw_canary {                 /* 1 byte */
    uint8_t magic;
} __attribute__((packed));

/* ---- 64-bit GSN helpers: wire format stays split, C code uses one uint64_t ---- */
static inline uint64_t rw_gsn_get(uint32_t upper_be, uint32_t lower_be)
{
    return ((uint64_t)ntohl(upper_be) << 32) | (uint64_t)ntohl(lower_be);
}

/* Takes void* so callers can pass &hdr->field of packed structs without
 * unaligned-pointer warnings; memcpy handles any alignment. */
static inline void rw_gsn_set(void *upper_be, void *lower_be, uint64_t gsn)
{
    uint32_t u = htonl((uint32_t)(gsn >> 32)), l = htonl((uint32_t)gsn);
    memcpy(upper_be, &u, sizeof(u));
    memcpy(lower_be, &l, sizeof(l));
}

/* ---- Size checks against the P4 definitions ---- */
_Static_assert(sizeof(struct rw_control)       == 14, "control size");
_Static_assert(sizeof(struct rw_ring_type)     == 8,  "ring_type size");
_Static_assert(sizeof(struct rw_append)        == 26, "append size");
_Static_assert(sizeof(struct rw_read)          == 22, "read size");
_Static_assert(sizeof(struct rw_tail)          == 14, "tail size");
_Static_assert(sizeof(struct rw_subscribe)     == 12, "subscribe size");
_Static_assert(sizeof(struct rw_induce_view)   == 1,  "induce_view size");
_Static_assert(sizeof(struct rw_trigger_view)  == 13, "trigger_view size");
_Static_assert(sizeof(struct rw_start_view)    == 8,  "start_view size");
_Static_assert(sizeof(struct rw_finalize_view) == 14, "finalize_view size");
_Static_assert(sizeof(struct rw_canary)        == 1,  "canary size");

#endif /* RINGWORLD_HDRS_H */
