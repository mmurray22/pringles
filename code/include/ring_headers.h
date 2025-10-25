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
#define ETH_READ 0x870
#define ETH_TAIL 0x840

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
};

// Append reply - from storage server
// Size: 96 bytes
struct ring_append_success {
    // client unique ID - TODO Do you need this?
    uint32_t cid;
    // Nonce for corresponding the request 
    uint32_t nonce;
};

// Subscribe 
struct ring_subscribe_entry {
    // FROM NETWORK: Global sequence number of the message
    uint32_t g_idx;
};


// TODO Read requests
// TODO Tail requests
// TODO Trim requests

std::unique_ptr<struct ring_append_entry> create_ring_append_entry(uint32_t nonce, int64_t cid);
