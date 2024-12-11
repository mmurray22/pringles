/* -*- P4_16 -*- */
// Credit: Base code come from p4 tutorial exercises
#include <core.p4>
#include <v1model.p4>

const bit<16> TYPE_TUNNEL = 0x1212;
const bit<16> TYPE_IPV4  = 0x0800;
const bit<16> TYPE_CONTROL = 0x0820;
const bit<16> TYPE_CLI_SEQ = 0x1414;
const bit<16> TYPE_APPEND = 0x0860;
const bit<16> TYPE_READ = 0x0870;
const bit<16> TYPE_TAIL = 0x0840;
#define STATIC_SHARD_NUM 100
#define IDX_SET_SIZE 100
#define NUM_BLOOM_HASH 4

#define RACK_STORAGE_SERVERS 1
#define QUORUM_SIZE 3 // f+1
#define MAX_OUTSTANDING_APPENDS 10
#define MAX_HOPS 10
#define MAX_PORTS 8
#define NUM_SWITCHES 4
#define CPU_PORT 510

/*************************************************************************
*********************** H E A D E R S  ***********************************
*************************************************************************/

typedef bit<9>  egressSpec_t;
typedef bit<48> macAddr_t;
typedef bit<32> ip4Addr_t;

typedef bit<48> time_t;

header ethernet_t {
    macAddr_t dstAddr;
    macAddr_t srcAddr;
    bit<16>   etherType;
}

header ipv4_t {
    bit<4>    version;
    bit<4>    ihl;
    bit<8>    diffserv;
    bit<16>   totalLen;
    bit<16>   identification;
    bit<3>    flags;
    bit<13>   fragOffset;
    bit<8>    ttl;
    bit<8>    protocol;
    bit<16>   hdrChecksum;
    ip4Addr_t srcAddr;
    ip4Addr_t dstAddr;
}



/******* Headers ********/

// Header for control packet
header control_pkt_t {
   // This is the current global sequence number.
   bit<32> global_seq_no;

   // Current view of the ring
   bit<32> ring_view;
}

// Header for heartbeats 
header heartbeat_t {
    // Current switch ID
    bit<32> sw_id;
}

// AppendEntry header
header append_entry_t {
    /** Part of header: Set by client **/

    // ID of the sender client
    bit<32> cid;
    // Unique nonce used to detect duplicates of the message
    bit<32> nonce;
    
    /** Part of header: Set by switches **/
    
    // Global sequence number of message
    bit<32> g_idx;
    // ID of the shard where the message must be written to
    bit<32> shard_id;
    // View number of switch forwarding entry
    bit<32> ring_view;
    // Status bits to indicate what "stage" of processing message is in
    // 1: New packet 
    // 2: Waiting for sequence number
    // 3: Waiting for shard id
    // 4: 
    bit<32> status;

    // Metadata: The number of times the control packet had been seen when entry is first received
    bit<32> cntrl_pkt_it;
}

// AppendEntrySuccess header
header append_entry_success_t {
    // ID of the sender client
    bit<32> cid;
    // Unique nonce used to detect duplicates of the message
    bit<32> nonce;
    // Global sequence number of message
    bit<32> g_idx;
}

// ReadEntry header
header read_entry_t {
    // ID of the sender client
    bit<32> cid;
    // Unique nonce used to detect duplicates of the message
    bit<32> nonce;
    // Global sequence number of message
    bit<32> g_idx;
    // ID of the shard storing the sequence number. Filled in by switches.
    int<32> shard_id;
}

// ReadEntry success header
header read_entry_success_t {
    // ID of the sender client
    bit<32> cid;
    // Unique nonce used to detect duplicates of the message
    bit<32> nonce;
    // Global sequence number of message
    bit<32> g_idx;
    // Log entry returned by storage server.
    bit<32> entry;
}

// Header for requesting the current tail
header tail_req_t {
    // ID of the sender client
    bit<32> cid;
    // Unique nonce used to detect duplicates of the message
    bit<32> nonce;
}

// Header for tail reply 
header update_tail_t {
    // Tail sequence number to return to the client
    bit<32> tail_no;
    // Unique nonce used to detect duplicates of the message
    bit<32> nonce;
}

// Header for requesting the largest written sequence number
// TODO is this necessary?
header GetLargestWrittenIdx {
    // ID of switch  
    bit<32> sw_id;
}

// Header for returning particular sequence number
header LargestShardIdx {
    // Largest global index from a shard
    bit<32> g_idx;
}


// Header for tunnelling 
header myTunnel_t {
    bit<16> proto_id;
    bit<16> dst_id;
}

struct metadata {
}

struct headers {
    ethernet_t              ethernet;
    control_pkt_t           cntrl;
    append_entry_t          append;
    myTunnel_t              myTunnel;
    ipv4_t                  ipv4;
}

/*************************************************************************
*********************** P A R S E R  ***********************************
*************************************************************************/

parser MyParser(packet_in packet,
                out headers hdr,
                inout metadata meta,
                inout standard_metadata_t standard_metadata) {

    state start {
        transition parse_ethernet;
    }

    state parse_ethernet {
        packet.extract(hdr.ethernet);
        transition select(hdr.ethernet.etherType) {
            TYPE_CONTROL: parse_control;
            TYPE_CLI_SEQ: parse_client_seq;
            TYPE_TUNNEL: parse_tunnel;
            TYPE_IPV4: parse_ipv4;
            TYPE_APPEND: parse_append;
	    default: accept;
        }
    }

    state parse_control {
        packet.extract(hdr.cntrl);
        transition parse_tunnel;
    }
     
    state parse_append {
        packet.extract(hdr.append);
        transition parse_tunnel;
    }
    
    state parse_tunnel {
        packet.extract(hdr.myTunnel);
        transition select(hdr.myTunnel.proto_id) {
            TYPE_IPV4: parse_ipv4;
            default: accept;
        }
    }

    state parse_client_seq {
        packet.extract(hdr.client_req);
        transition parse_ipv4;
    }

    state parse_ipv4 {
        packet.extract(hdr.ipv4);
        transition accept;
    }
}

/*************************************************************************
************   C H E C K S U M    V E R I F I C A T I O N   *************
*************************************************************************/

control MyVerifyChecksum(inout headers hdr, inout metadata meta) {
    apply {  }
}


/*************************************************************************
**************  I N G R E S S   P R O C E S S I N G   *******************
*************************************************************************/

control MyIngress(inout headers hdr,
                  inout metadata meta,
		  inout standard_metadata_t standard_metadata) {
   
    /** Registers **/

    /// Ring State
    register<bit<32>>(1) pred_switch_id; // ID of the switch immediately before this switch in the ring
    register<bit<32>>(1) succ_switch_id; // ID of the switch immediately following this switch in the ring
    register<bit<32>>(1) ring_view; // Current ring view number
    register<bit<32>>(1) iteration; // Used to track stale packets

    /// Local Switch State
    register<bit<32>>(1) local_seq_cntr; // Rack local sequence number. Reset each time it's added to global counter
    register<bit<32>>(1) global_offset; // Latest global sequence number known to the switch.  TODO confusing names :/ 
    register<bit<32>>(1) known_global_seq_no; // Latest global sequence number known to the ring. Updated every time control packet is received
    register<bit<1>>(IDX_SET_SIZE) idx_buf; // Tracks every index which is being actively processed by the switch's storage shard NO REMOVE FUNCTION TODO
    register<bit<32>>(NUM_BLOOM_HASH) k_bloom_pos;
    register<bit<1>>(1) value_not_found;
    register<bit<32>>(1) num_shards; // Total number of storage shards, actual shard IDs are stored in multicast table
    register<bit<32>>(1) cntrl_pkt_it; // Number of total times the switch has seen the control packet TODO potential to overflow?
    
    action drop() {
        mark_to_drop(standard_metadata);
    }

    action ipv4_forward(macAddr_t dstAddr, egressSpec_t port) {
        standard_metadata.egress_spec = port;
        hdr.ethernet.srcAddr = hdr.ethernet.dstAddr;
        hdr.ethernet.dstAddr = dstAddr;
        hdr.ipv4.ttl = hdr.ipv4.ttl - 1;
    }

    table ipv4_lpm {
        key = {
            hdr.ipv4.dstAddr: lpm;
        }
        actions = {
            ipv4_forward;
            drop;
            NoAction;
        }
        size = 1024;
        default_action = NoAction(); //drop();
    }
   
    action myTunnel_forward(egressSpec_t port) {
	    standard_metadata.egress_spec = port;
    }

    table myTunnel_exact {
        key = {
           hdr.myTunnel.dst_id: exact;
        }
        actions = {
           myTunnel_forward;
           drop;
        }
        size = 1024;
        default_action = drop();
    }

    /* Checks if sequence number is in bloom filter */
    action bf_check_bit(seq_no, salt) {
        // Execute k hash functions to see if a single one is not 0
        hash(k_bloom_pos[0], HashAlgorithm.crc16, (bit<32>)0, seq_no, (bit<32>)BLOOM_FILTER_ENTRIES);
        hash(k_bloom_pos[1], HashAlgorithm.crc32, (bit<32>)0, seq_no, (bit<32>)BLOOM_FILTER_ENTRIES);
        hash(k_bloom_pos[2], HashAlgorithm.ones_complement16, (bit<32>)0, seq_no, (bit<32>)BLOOM_FILTER_ENTRIES);
        hash(k_bloom_pos[3], HashAlgorithm.identity, (bit<32>)0, seq_no, (bit<32>)BLOOM_FILTER_ENTRIES);

        if (idx_buf[k_bloom_pos[0]] != 1 || idx_buf[k_bloom_pos[1]] != 1 || idx_buf[k_bloom_pos[2]] != 1 || idx_buf[k_bloom_pos[3]] != 1) {
            value_not_found.write(0, 1);
        }
    }

    /* Sets bits in bloom filter */
    action bf_set_bit(seq_no, salt) {
        hash(k_bloom_pos[0], HashAlgorithm.crc16, (bit<32>)0, seq_no, (bit<32>)BLOOM_FILTER_ENTRIES);
        hash(k_bloom_pos[1], HashAlgorithm.crc32, (bit<32>)0, seq_no, (bit<32>)BLOOM_FILTER_ENTRIES);
        hash(k_bloom_pos[2], HashAlgorithm.ones_complement16, (bit<32>)0, seq_no, (bit<32>)BLOOM_FILTER_ENTRIES);
        hash(k_bloom_pos[3], HashAlgorithm.identity, (bit<32>)0, seq_no, (bit<32>)BLOOM_FILTER_ENTRIES);

        idx_buf[k_bloom_pos[0]] = 1;
        idx_buf[k_bloom_pos[1]] = 1;
        idx_buf[k_bloom_pos[2]] = 1;
        idx_buf[k_bloom_pos[3]] = 1;
    }

    action get_shard_id() { // TODO
        int<32> num_shards_reg;
        num_shards_reg.read(0, num_shards);
        int<32> shard_idx = hdr.append.g_idx % (num_shards_reg + 1);
        hdr.append.shard_id = shard_idx;
    }

    action route_append_pkt() { 
        int<32> pred_sw_id_reg;
        pred_switch_id.read(0, pred_sw_id_reg);
        int<32> succ_sw_id_reg;
        succ_switch_id.read(0, succ_sw_id_reg);

        if (hdr.append.shard_id > pred_sw_id_reg && hdr.append.shard_id < succ_sw_id_reg && hdr.append.shard_id < switch_id_reg) {
            standard_metadata.mcast_grp = 1;
            hdr.append.status += 1;
        } else if (hdr.append.shard_id < pred_sw_id_reg) {
            standard_metadata.mcast_grp = 2; // TODO do I need a multicast group here? I don't think so
        } else if (hdr.append.shard_id > succ_sw_id_reg) {
            standard_metadata.mcast_grp = 3;
        }
    }

    action route_read_pkt() { 
        int<32> pred_sw_id_reg;
        pred_switch_id.read(0, pred_sw_id_reg);
        int<32> succ_sw_id_reg;
        succ_switch_id.read(0, succ_sw_id_reg);

        if (hdr.read.shard_id > pred_sw_id_reg && hdr.read.shard_id < succ_sw_id_reg && hdr.read.shard_id < switch_id_reg) {
            standard_metadata.mcast_grp = 1;
            hdr.read.status += 1;
        } else if (hdr.read.shard_id < pred_sw_id_reg) {
            standard_metadata.mcast_grp = 2; // TODO do I need a multicast group here? I don't think so
        } else if (hdr.read.shard_id > succ_sw_id_reg) {
            standard_metadata.mcast_grp = 3;
        }
    }

    apply {

        /** Process Control packets **/
        if (hdr.cntrl.isValid()) {
            bit<32> ring_view_reg;
            ring_view.read(ring_view_reg, 0);
            
            // If the control packet's view is outdated
            if (hdr.cntrl.ring_view < ring_view_reg) {
                drop();
                return;
            }

            // Record most recent global seq number
            known_global_seq_no.write(0, hdr.cntrl.global_seq_no);
            
            // Update control packet global seq number
            bit<32> local_seq_cntr_reg;
            local_seq_cntr.read(local_seq_cntr_reg, 0);
            hdr.cntrl.global_seq_no = hdr.cntrl.global_seq_no + local_seq_cntr_reg;
          
            // Update global offset on the switch
            global_offset.write(0, hdr.cntrl.global_seq_no);

            // Update the local seq no register
            local_seq_cntr.write(0, 0);

            // Set which switch/group to forward message to
            standard_metadata.mcast_grp = 1;
	    }

        /** Process AppendEntry packets **/
        if (hdr.append.isValid()) {
            // Change processing of append based on the status of the packet
            // Status 1: First time the packet has been seen
            if (hdr.append.status == 1) {

                // Assign local seq no
                bit<32> local_seq_cntr_reg;
                local_seq_cntr.read(local_seq_cntr_reg, 0);
                local_seq_cntr_reg = local_seq_cntr_reg + 1;
                local_seq_cntr.write(0, local_seq_cntr_reg);

                // Update packet header variables
                // Set status to 2 (pending sequence number)
                hdr.append.g_idx = local_seq_cntr_reg;
                hdr.append.status = 2;

                // Set multicast group
                standard_metadata.mcast_grp = 1; // TODO: What multicast group??
            } else if (hdr.append.status == 2) {
                bit<32> cntrl_pkt_it_reg;
                cntrl_pkt_it.read(cntrl_pkt_it_reg, 0);
                if (hdr.append.cntrl_pkt_it == cntrl_pkt_it_reg) {
                    return; // A new control packet has not been received TODO should you do this?
                }
                // Status 2: Packet is waiting for a global seq no assignment
                
                // Assign global seq no
                bit<32> g_seq_no;
                known_global_seq_on.read(g_seq_no, 0);
                hdr.append.g_idx = hdr.append.g_idx + g_seq_no;
                hdr.append.status = 3;
                // Set multicast group
                standard_metadata.mcast_grp = 1; // TODO: What multicast group??
            } else if (hdr.append.status == 3) {
                bit<32> g_offset_reg;
                global_offset.read(0, g_offset_reg);
                if (hdr.append.g_idx > g_offset_reg) {
                    get_shard_id();
                    hdr.append.status = 4;
                }
                // Set multicast group
                standard_metadata.mcast_grp = 1;
            } else if (hdr.append.status == 4) {
                route_append_pkt();
            } else if (hdr.append.status == 5) {
                // Now, the append entry is at the switch which manages the shard it needs to store entries at

                // Check if seq no is already being processed
                bit<32> salt = 0; // TODO
                bf_check_bit(hdr.append.g_idx, salt);
                bit<32> value_not_found_reg;
                value_not_found.read(value_not_found_reg, 0); // TODO is the read semantics correct
                if (value_not_found_reg == 1) {
                    bf_set_bit(hdr.append.g_idx, salt);
                    standard_metadata.mcast_grp = 4 + hdr.append.shard_id;
                    hdr.append.status = 6;
                } else {
                    // Send failure message TODO
                    send_failure();
                }
            }
        }

        /* Process read entry headers */
        if (hdr.read.status == 1) {
            get_shard_id();
            hdr.read.status = 2;
        } else if (hdr.read.status == 2) {
            route_read_pkt();
        } else if (hdr.read.status == 3) {
            
        }

        /* Process IP headers */
        if (hdr.ipv4.isValid()) {
            ipv4_lpm.apply();
        }

        /* Process tunnel headers */
        if (hdr.myTunnel.isValid()) {
            myTunnel_exact.apply();
        } 
    }
}

/*************************************************************************
****************  E G R E S S   P R O C E S S I N G   ********************
*************************************************************************/

control MyEgress(inout headers hdr,
                 inout metadata meta,
                 inout standard_metadata_t standard_metadata) {
	apply { }
}

/*************************************************************************
*************   C H E C K S U M    C O M P U T A T I O N   ***************
*************************************************************************/

control MyComputeChecksum(inout headers  hdr, inout metadata meta) {
     apply {
        update_checksum(
            hdr.ipv4.isValid(),
            { hdr.ipv4.version,
              hdr.ipv4.ihl,
              hdr.ipv4.diffserv,
              hdr.ipv4.totalLen,
              hdr.ipv4.identification,
              hdr.ipv4.flags,
              hdr.ipv4.fragOffset,
              hdr.ipv4.ttl,
              hdr.ipv4.protocol,
              hdr.ipv4.srcAddr,
              hdr.ipv4.dstAddr },
            hdr.ipv4.hdrChecksum,
            HashAlgorithm.csum16);
    }
}

/*************************************************************************
***********************  D E P A R S E R  *******************************
*************************************************************************/

control MyDeparser(packet_out packet, in headers hdr) {
    apply {
        packet.emit(hdr.ethernet);
	packet.emit(hdr.cntrl);
	packet.emit(hdr.client_req);
	packet.emit(hdr.myTunnel);
        packet.emit(hdr.ipv4);
    }
}

/*************************************************************************
***********************  S W I T C H  *******************************
*************************************************************************/

V1Switch(
MyParser(),
MyVerifyChecksum(),
MyIngress(),
MyEgress(),
MyComputeChecksum(),
MyDeparser()
) main;
