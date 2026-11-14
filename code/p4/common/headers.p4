/*******************************************************************************
 * P4_16: Ringworld headers helper file
 * Credit: 
 *  - Lots of inspiration from the p4_16 examples
 * What's in this file (sections):
 * 1. Custom variable types
 * 2. Custom constants
 * 3. Custom headers
 * 4. Metadata struct
 * 5. Final header struct that will be parsed
 ******************************************************************************/

#ifndef _HEADERS_
#define _HEADERS_

// Custom variable types
typedef bit<48> mac_addr_t;
typedef bit<32> ipv4_addr_t;
typedef bit<128> ipv6_addr_t;
typedef bit<12> vlan_id_t;
typedef bit<16> ether_type_t;
typedef bit<8> ip_protocol_t;
typedef bit<9>  egress_spec_t;
typedef bit<48> time_t;
typedef bit<8> pkt_type_t;


// Default header types
const ether_type_t ETHERTYPE_IPV4 = 16w0x0800;
const ether_type_t ETHERTYPE_ARP = 16w0x0806;
const ether_type_t ETHERTYPE_IPV6 = 16w0x86dd;
const ether_type_t ETHERTYPE_VLAN = 16w0x8100;
const ip_protocol_t IP_PROTOCOLS_ICMP = 1;
const ip_protocol_t IP_PROTOCOLS_TCP = 6;
const ip_protocol_t IP_PROTOCOLS_UDP = 17;

// Custom Ringworld constants
const bit<16> TYPE_CONTROL = 0x0820; // only for the switches
const bit<16> TYPE_APPEND = 0x0860;
const bit<16> TYPE_APPEND_WAIT = 0x0862;
const bit<16> TYPE_APPEND_RESP = 0x0861;
const bit<16> TYPE_READ = 0x0870;
const bit<16> TYPE_READ_RESP = 0x0871;
const bit<16> TYPE_TAIL = 0x0840;
const bit<16> TYPE_SUB = 0x0850;
const bit<16> TYPE_SUB_RESP = 0x0851;
const bit<16> TYPE_VIEW_INDUCE = 0x0890;
const bit<16> TYPE_VIEW_TRIGGER = 0x0891;
const bit<16> TYPE_VIEW_CHANGE = 0x0900;
const bit<16> TYPE_VIEW_ACK = 0x0910;

const bit<32> MAX_INFLIGHT_ACKS = 65536;
const bit<32> MAX_GSN_MAP_SIZE = 65535;
const bit<32> MAX_GSN_MAP_IDX_VAL = 65534;
const bit<8> CANARY_VALUE = 165; //0xA5;

const pkt_type_t PKT_TYPE_NORMAL = 1;
const pkt_type_t PKT_TYPE_MIRROR = 2;


#if __TARGET_TOFINO__ == 1
typedef bit<3> mirror_type_t;
#else
typedef bit<4> mirror_type_t;
#endif
const mirror_type_t MIRROR_TYPE_I2E = 1;

// Default headers used in our protocols (ethernet, ipv4, and udp)
header ethernet_t {
    mac_addr_t dst;
    mac_addr_t src;
    bit<16> type;
}

header ipv4_t {
    bit<4> version;
    bit<4> ihl;
    bit<8> tos;
    bit<16> len;
    bit<16> id;
    bit<3> flags;
    bit<13> frag;
    bit<8> ttl;
    bit<8> proto;
    bit<16> chksum;
    ipv4_addr_t src;
    ipv4_addr_t dst;
}

header udp_t {
    bit<16> src_port;
    bit<16> dst_port;
    bit<16> len;
    bit<16> chksum;
}

/******* Custom Ringworld Headers ********/
/*
 * Note:
 *  Numbers which need to be read as integers or take part in arithmetic operations are ints.
 *  Numbers that are merely for identification are bit arrays.
 */

// Size: 8 bits
header mirror_h {
    bit<8> pkt_type;
}

// Control Packet Header
// Size: 64
header control_pkt_t {
   // Current global sequence number.
   bit<32> global_seq_no;

   // Current view of the ring
   bit<16> ring_view;
}

// Ring type header - holds the type of the packet
// Size: 96 bits
header ring_type_t {
    // Type to filter packets
    bit<16> type;

    // Counting the number of entries batched in the packet
    bit<16> num_entries;

    // Shard ID - to be filled in by switch
    bit<16> shard_id;

    // Switch ID - to be filled in by switch
    bit<8> switch_to_process;

    bit<48> start_ts;
    bit<48> end_ts;

    bit<16> raw_elapsed_time;

    bit<32> pipe_ts;
}

// AppendEntry header
// Size: 320
header append_entry_t {
    /** Part of header: Set by client **/
    // Unique nonce used to detect duplicates of the message
    bit<16> nonce;
    // Bytes in each paylod
    bit<16> payload_size;
    // Stream ID
    bit<32> stream_id;
    
    /** Part of header: Set by switches **/
    
    // Global sequence number of message
    bit<32> g_idx; // TODO: NEEDS TO BECOME 64 BIT NUMBER
    
    // Metadata: The number of times the control packet had been seen when entry is first received
    bit<16> cntrl_pkt_it;

    bit<16> recv_port;
    bit<16> exp_type;
    bit<48> start_ts;
}

// ReadEntry header
// Size: 368
header read_entry_t {
    /** Part of header: Set by client **/

    // Unique nonce used to detect duplicates of the message
    bit<16> nonce;
    // Bytes in each paylod
    bit<32> payload_size;

    // Stream ID
    bit<32> stream_id;

    // Global sequence number of message
    bit<32> g_idx; // TODO 64 BITS
    
    /** Part of header: Set by switches **/
    
    // View number of switch forwarding entry
    bit<16> ring_view;

    bit<16> recv_port;
}

// Header for requesting the current tail
// Size: 128
header tail_req_t {
    /** Filled in by client **/

    // Unique nonce used to detect duplicates of the message
    bit<16> nonce;
    
       
    /*Altered by switches*/

    // Keeps track of how many nodes in the ring the tail request has been passed to
    bit<8> hops;

    bit<8> start_switch_id;

    /** Filled in by switches **/

    // Sequence number that is the current tail - filled in by the switches
    bit<32> tail_seq_no; // TODO: 64 BITS
    bit<16> recv_port;
}

// Subscribe header
// Size: 160
header subscribe_req_t {
    bit<32> g_idx;
    // Stream ID
    bit<32> stream_id;
    bit<16> recv_port;
    bit<16> subscribe_port;
}


/***** View Change Headers ******/
struct digest_timeout_t {
    bit<8> cntrl_packet_not_seen;
}

header induce_view_change_t {
    bit<8> drop_cntrl;
}

// Phase 1: Trigger View Change
// Initially sent by any ring member whose timeout has expired
// Needs quorum to advance
header trigger_view_change_t {
    // Set by the view leader
    bit<8> leader_switch_id;
    bit<16> current_ring_view;
    bit<8> view_member_switch_id;

    // Filled in by view member switch
    bit<32> highest_seen_seq_no;
    bit<8> hops;
}

// Start View Change
// Initially sent by view change leader
// Needs quorum to advance
header start_view_change_t {
    // Set by the view leader
    bit<8> leader_switch_id;
    bit<16> current_ring_view;

    // Filled in by view member switch
    bit<8> view_member_switch_id;
    bit<32> highest_seen_seq_no;
}

// Finalize View Change
// Initially sent by view change leader
// Needs quorum to advance
header finalize_view_change_t {
    bit<8> leader_switch_id;
    bit<8> view_member_switch_id;

    bit<16> current_ring_view;
    bit<8> next_switch_id;
    bit<32> highest_quorum_seq_no;
    bit<8> hops;
}

header canary_t {
    bit<8> magic;
}

/****************** Metadata structure **************/
struct metadata {
    bit<2> recirc_port_type;
    bit<1> route_to_shard;
    bit<1> ready_for_ack;

    bit<16> ring_process;
    bit<8> ack_ready;
    bit<8> num_recirc_ports;
    bit<8> num_wait_ports;
    bit<8> port_idx;
    bit<8> switch_id;
    bit<8> leader_switch_id;
    bit<8> accept_finalize_view;

    bit<8> view_change_active;
    bit<16> accept_view_change;
    bit<8> view_hops;

    bit<16> time_since_last_cntrl;
    bit<8> next_hop;
    bit<8> update_iteration;
    bit<8> append_g_zero;
    
    bit<16> cntrl_pkt_it;
    bit<16> cntrl_pkt_it_idx;
    bit<16> wait_time;
    bit<16> nonce;
    bit<16> ring_view;
    bit<16> num_shards;

    bit<32> dst_addr;
    bit<32> cnt;
    bit<32> assign_sn;
    bit<32> local_seq_no;
    bit<32> global_seq_no;
    bit<32> batch_size; // For keeping tabs on the GSN table size
    bit<32> queue_congest;
    bit<32> shard_size;
    bit<32> duration;
    int<32> overflow;
    bit<32> output;
    bit<32> ack_threshold;
    bit<32> last_time;
    bit<32> current_time;

    // For mirroring
    bit<1> do_ing_mirroring; // Enable ingress mirroring
    bit<1> do_egr_mirroring; // Enable egress mirroring
    MirrorId_t ing_mir_ses; // Ingress mirror session ID
    MirrorId_t egr_mir_ses; // Egress mirror session ID
    pkt_type_t pkt_type;

    bit<48> start_ts;
    bit<48> end_ts;
    bit<32> elapse_time;
}


/****** FULL Headers structure **********/
struct headers {
    pktgen_timer_header_t   timer;
    ethernet_t              ethernet;
    control_pkt_t           cntrl;
    ipv4_t                  ipv4;
    udp_t		    udp; // 64 bits
    induce_view_change_t    induce_view;
    trigger_view_change_t   trigger_view;
    start_view_change_t     start_view;
    finalize_view_change_t  finalize_view;
    ring_type_t             ring_type;
    append_entry_t          append;
    read_entry_t            read;
    tail_req_t              tail;
    subscribe_req_t         sub;
    canary_t		    canary;
}



#endif /* _HEADERS_ */
