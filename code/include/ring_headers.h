/*
 * Contains custom network headers required by certain logging
 * systems. Many use the standard UDP headers, but some require custom
 * headers with custom information.
 */
#include <memory>
#include <cstdint>

/*New ethernet header types*/
#define ETH_CLI_SEQ 0x1414
#define ETH_APPEND_REQ 0x0860
#define ETH_APPEND_RESP 0x861
#define ETH_READ_REQ 0x870
#define ETH_READ_RESP 0x871
#define ETH_TAIL 0x840

// The type indicator header
// REQUIRED FOR ALL PACKETS
struct ring_type {
    // Type of packet
    uint16_t type;
    uint16_t num_entries;
    // Filled in by switch
    uint32_t shard_id;
    // FROM CLIENT: client unique ID
    uint32_t cid;
    // Switch ID - filled in by switch operations
    uint32_t switch_to_process;
};


// Append request - from client
// Size: 448 bytes TODO old
struct ring_append_entry {
    // FROM CLIENT: nonce to uniquely identify this append message
    uint32_t nonce;
    // FROM CLIENT: To tell the storage server how many bytes each payload is
    uint32_t payload_size;
    // FROM CLIENT: To tell the storage server how many entries are in the payload
    uint32_t num_entries;
    // FROM NETWORK: Global sequence number of the message
    uint32_t g_idx;
    // Number of payloads in batch
    uint32_t batch_size;
    // FROM NETWORK: view number of switch forwarding entry
    uint32_t ring_view;
    // FROM NETWORK: status bits to indicate processing stage
    uint32_t status;
    // FROM NETWORK: number of times control packet was seen
    uint32_t cntrl_pkt_it;
    // FROM CLIENT
    uint32_t thread_id;
    // FROM CLIENT
    uint32_t client_ip;
    // FROM CLIENT
    uint16_t recv_port;
    // TESTING
    uint64_t timestamp; // Ingress timestamp? TODO

    // MEASUREMENT?
};

// Read request - from client
// Size: 352 bytes TODO old
struct ring_read_entry {
    // FROM CLIENT: nonce to uniquely identify this append message
    uint32_t nonce;
    // FROM NETWORK: Global sequence number of the message
    uint32_t g_idx;
    // FROM NETWORK: view number of switch forwarding entry
    uint32_t ring_view;
    // FROM NETWORK: status bits to indicate processing stage
    uint32_t status;
    // FROM CLIENT
    uint32_t thread_id;
    // FROM CLIENT
    uint16_t recv_port;
    // FROM CLIENT
    uint32_t client_ip;
    // TESTING
    uint64_t timestamp;
    uint32_t circs; // to keep track of how many times the packet has recirculated, for timeout reasons
};

// Append ACK - from storage server TODO not going to use?
// Size: 96 bytes
struct ring_append_ack {
    // client unique ID
    uint32_t cid;
    // Nonce for corresponding the request 
    uint32_t nonce;
    // Global index being acked
    uint32_t g_idx;

};

// Subscribe 
struct ring_subscribe_entry {
    // FROM NETWORK: Global sequence number of the message
    uint32_t g_idx;
    uint32_t subscribe;
    // FROM CLIENT
    uint32_t client_ip;
    // FROM CLIENT
    uint16_t recv_port;
};

// Tail requests
struct ring_tail_req {
    // Nonce for corresponding the request 
    uint32_t nonce;
    // Keeps track of how many nodes in the ring the tail request has been passed to
    uint32_t hops;
    // Tail sequence number
    uint32_t tail_seq_no;
    // FROM CLIENT
    uint32_t client_ip;
    // FROM CLIENT
    uint16_t recv_port;

};


inline size_t get_ring_append_size() {
    return sizeof(struct ring_append_entry);
}

inline size_t get_ring_type_size() {
    return sizeof(struct ring_type);
}

inline size_t get_ring_append_ack_size() { // TODO not going to use
    return sizeof(struct ring_append_ack); 
}

inline size_t get_ring_read_size() {
    return sizeof(struct ring_read_entry);
}

inline size_t get_ring_subscribe_size() {
    return sizeof(struct ring_subscribe_entry);
}
// TODO Trim requests
