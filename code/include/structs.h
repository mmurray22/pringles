/*
 * Contains custom network headers required by certain logging
 * systems. Many use the standard UDP headers, but some require custom
 * headers with custom information.
 */
#include <stdint.h>

#define UDP 0
#define CORFU 1
#define RING 2

/* UDP - no need to define, already in SOCK_DGRAM */

/* Corfu Log w/ Programmable Switches API */

// Sequence number requester
// Size: 128 bytes
struct get_sequence_number {
    // FROM CLIENT: client unique ID
    uint32_t cid;
    // FROM CLIENT: number of sequence numbers requested
    uint32_t num_idx_req;
    // FROM NETWORK: First assigned global sequence number 
    uint32_t first_g_idx;
    // FROM NETWORK: Last assigned global sequence number
    uint32_t last_g_idx;
}

/* Ring Log API */

// Append request - from client
// Size: 224 bytes
struct ring_append_entry {
    // FROM CLIENT: client unique ID - TODO do you need this? src addr
    uint32_t cid;
    // FROM CLIENT: nonce to uniquely identify this append message
    uint32_t nonce;
    // FROM NETWORK: Global sequence number of the message
    uint32_t g_idx;
    // FROM NETWORK: ID of shard message is written to
    uint32_t shard_id;
    // FROM NETWORK: view number of switch forwarding entry
    uint32_t ring_view;
    // FROM NETWORK: status bits to indicate processing stage
    uint32_t status;
    // FROM NETWORK: number of times control packet was seen
    uint32_t cntrl_pkt_it;
}

// Append reply - from storage server
// Size: 96 bytes
struct ring_append_success {
    // client unique ID - TODO Do you need this?
    uint32_t cid;
}

// Read requests
// Tail requests
// Trim requests

/*Helper functions*/

//Creation functions
std::unique_ptr<struct get_sequence_number> create_get_sequence_num(int64_t cid);
size_t get_sequence_num_size();
std::unique_ptr<struct ring_append_entry> create_ring_append_entry(int64_t nonce, int64_t cid);
size_t get_ring_append_entry();
std::unique_ptr<struct ring_append_success> create_ring_append_reply(int64_t nonce, int64_t cid);
size_t get_ring_append_reply();
