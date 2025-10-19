#include "ring_headers.h"
#include <memory>


std::vector<std::string> get_vec_of_packet_types() {
    std::vector<std::string> pkt_types;
    switch (append)
    {
	case append:
	    pkt_types.append("append");
	case read:
	    pkt_types.append("read");
	case tail:
	    pkt_types.append("tail");
    }
    return pkt_types;
}

size_t get_size_of_hdr(PacketType pkt_type) {
    if (pkt_type == PacketType::append) {
        return sizeof(struct ring_append_entry);
    }
    return 0;
}

int get_eth_type(PacketType pkt_type) {
    if (pkt_type == PacketType::append) {
        return ETH_APPEND_REQ;
    }
    return -1; // No ethernet type found
}

std::unique_ptr<struct ring_append_entry> create_ring_append_entry(int64_t nonce, int64_t cid) {
    std::unique_ptr<struct ring_append_entry> app_entry_hdr = std::make_unique<struct ring_append_entry>();
    app_entry_hdr.get()->nonce = nonce;
    app_entry_hdr.get()->cid = cid;
    return app_entry_hdr;
}

/*std::unique_ptr<struct ring_append_success> create_ring_append_reply(int64_t nonce, int64_t cid) {
    std::unique_ptr<struct ring_append_success> app_reply_hdr = std::make_unique<struct ring_append_success>();
    app_reply_hdr.get()->nonce = nonce;
    app_reply_hdr.get()->cid = cid;
    return app_reply_hdr;
}*/
