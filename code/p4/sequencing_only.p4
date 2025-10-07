/* -*- P4_16: Bare Minimum Sequencing Program -*- */
// Credit: 
// - Base code come from p4 tutorial exercises
// - CPU port code inspiration comes from https://github.com/nsg-ethz/p4-learning/tree/master/examples/copy_to_cpu
#include <core.p4>
#include <v1model.p4>

const bit<16> TYPE_IPV4  = 0x0800;
const bit<16> TYPE_CONTROL = 0x0820; // only for the switches
const bit<16> TYPE_CONTROL_CHECK = 0x0880; // only for the switches
const bit<16> TYPE_APPEND = 0x0860;
const bit<16> TYPE_TAIL = 0x0840;

#define STATIC_SHARD_NUM 100
#define IDX_SET_SIZE 100
#define NUM_BLOOM_HASH 4

#define RACK_STORAGE_SERVERS 1
#define QUORUM_SIZE 3 // f+1
#define STORAGE_SHARD_SIZE 5
#define MAX_OUTSTANDING_APPENDS 10
#define MAX_HOPS 10
#define MAX_PORTS 8
#define NUM_SWITCHES 4
#define CPU_PORT 510
#define NUM_SHARDS 3

/*************************************************************************
*********************** H E A D E R S  ***********************************
*************************************************************************/

typedef bit<9>  egressSpec_t;
typedef bit<48> macAddr_t;
typedef bit<32> ip4Addr_t;
typedef bit<48> time_t;


/******* Default Headers ********/

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



/******* Custom Pringles Headers ********/
/*
 * Note:
 *  Numbers which need to be read as integers or take part in arithmetic operations are ints.
 *  Numbers that are merely for identification are bit arrays.
*/



// Control Packet Header
header control_pkt_t {
   // Current global sequence number.
   int<32> global_seq_no;

   // Current view of the ring
   bit<32> ring_view;

   // ID of last sending switch
   bit<32> id;
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
    int<32> g_idx;
    // Number of payloads in batch 
    int<32> batch_size;
    // ID of the shard where the message must be written to
    bit<32> shard_id; // TODO: Shard ID type should be the same across headers, fix
    // View number of switch forwarding entry
    bit<32> ring_view;
    // Status bits to indicate what "stage" of processing message is in
    // 1: New packet 
    // 2: Waiting for sequence number
    // 3: Waiting for shard id
    // 4: TODO
    bit<32> status;

    // Metadata: The number of times the control packet had been seen when entry is first received
    int<32> cntrl_pkt_it;
}

// Header for requesting the current tail
header tail_req_t {
    /** Filled in by client **/

    // ID of the sender client
    bit<32> cid;
    // Unique nonce used to detect duplicates of the message
    bit<32> nonce;
   
    /*Altered by switches*/

    // Keeps track of how many nodes in the ring the tail request has been passed to
    int<32> hops;

    /** Filled in by switches **/

    // Sequence number that is the current tail - filled in by the switches
    int<32> tail_seq_no;
}

/******* Custom Test Headers ********/
// Check control packet variables TODO
header control_pkt_checker_t {
   // Read switch's current global sequence number.
   bit<32> switch_global_seq_no;
}

struct metadata {
    bit<32> highest_contiguous_seq_no;
    bit<32> default_bit;
    bit<32> circulate;
    bit<32> switch_id;
}

struct headers {
    ethernet_t              ethernet;
    ipv4_t                  ipv4;
    control_pkt_t           cntrl;
    control_pkt_checker_t   cntrl_check;
    append_entry_t          append;
    tail_req_t              tail;
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
        transition parse_ipv4;
        
    }

    state parse_control {
        packet.extract(hdr.cntrl);
        transition accept;
    }
    
    state parse_control_check {
        packet.extract(hdr.cntrl_check);
        transition accept;
    }

    state parse_append {
        packet.extract(hdr.append);
        transition accept;
    }
    
    state parse_tail {
        packet.extract(hdr.tail);
        transition accept;
    }

    state parse_ipv4 {
        packet.extract(hdr.ipv4);
	    transition select(hdr.ethernet.etherType) {
            TYPE_CONTROL: parse_control;
            TYPE_CONTROL_CHECK: parse_control_check;
            TYPE_APPEND: parse_append;
            TYPE_TAIL: parse_tail;
	        default: accept;
        }
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
    register<bit<32>>(1) ring_view; // Current ring view number TODO will need to be written to from the control plane

    /// Sequencing state
    register<bit<32>>(1) highest_seen_seq_no; // Largest global sequence number known to the switch Updated every time control packet is received.
    register<bit<32>>(1) local_seq_no; // Local seq no batch counter 
    register<bit<32>>(1) cntrl_pkt_it; // Number of total times the switch has seen the control packet TODO potential to overflow?

    /// Replication state
    register<bit<32>>(1) highest_replicated_seq_no; // Largest global sequence number corresponding to a successfully replicated entry.
    
    /** ACTIONS **/

    /* Standard IPv4 routing */
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
        default_action = NoAction();
    }

    /* Control Packet */
    action cntrl_forward(egressSpec_t port, bit<32> id) {
        standard_metadata.egress_spec = port;
        hdr.cntrl.id = id;
    }

    table cntrl_id_to_ip {
        key = {
            hdr.cntrl.id: exact;
        }
        actions = {
            cntrl_forward;
            drop;
            NoAction;
        }
        size = 1024;
        default_action = NoAction();
    }


    /* Read */
    action multicast_append(bit<16> mcast_grp_num) {
        standard_metadata.mcast_grp = mcast_grp_num;
    }
    
    table get_append_shard_id {
        key = { 
            hdr.append.shard_id: exact;
        }
        actions = {
            multicast_append;
            drop;
        }
        size = 1024;
        default_action = drop;
    }
    
    /* Tail */
    action update_tail(egressSpec_t port) {
        bit<32> hr_seq_no;
        highest_replicated_seq_no.read(hr_seq_no, 0);
        if (hdr.tail.tail_seq_no < (int<32>)hr_seq_no) {
            hdr.tail.tail_seq_no = (int<32>)hr_seq_no;
        }
        standard_metadata.egress_spec = port;
    }
    
    action return_tail(macAddr_t dstAddr, egressSpec_t port) {
        standard_metadata.egress_spec = port;
        hdr.ethernet.srcAddr = hdr.ethernet.dstAddr;
        hdr.ethernet.dstAddr = dstAddr;
        hdr.ipv4.ttl = hdr.ipv4.ttl - 1;
    }
    
    table process_tail {
        key = { 
            hdr.tail.hops: range;
        }
        actions = {
            update_tail;
            return_tail;
            drop;
        }
        size = 1024;
        default_action = drop;
    }

    action circulate_port(egressSpec_t port) {
        standard_metadata.egress_spec = port;
    }

    table circulate_table {
        key = {
            meta.default_bit: exact;
        }
        actions = {
            circulate_port;
            NoAction;
        }
        default_action = NoAction;
    }

    apply { // Parcel all header state changes into actions TODO 
        meta.circulate = 0;
        bit<32> base = 0; // Used to calculate the shard ID from the sequence number

        /* 
         * Process Control packets 
         * 
         * Number of read actions done: 3
         * Number of write actions done: 3
         */
        if (hdr.cntrl.isValid()) {
            bit<32> ring_view_reg;
            bit<32> local_seq_no_reg;
            int<32> highest_seen_seq_no_reg;
            bit<32> cntrl_pkt_it_reg;
            
            // Step 0: Check if the control packet's view is outdated
            ring_view.read(ring_view_reg, 0);
            if (hdr.cntrl.ring_view < ring_view_reg) {
                drop();
                return;
            }
        
            // Step 1: Update control packet global sequence number
            local_seq_no.read(local_seq_no_reg, 0);
            hdr.cntrl.global_seq_no = hdr.cntrl.global_seq_no + (int<32>)local_seq_no_reg;

            // Step 2: Update control packet + local highest seen seq no
            highest_seen_seq_no.write(0, (bit<32>)(hdr.cntrl.global_seq_no));

            // Step 3: Update local sequence counter
            local_seq_no.write(0, 0);

            // Step 4: Control packet iteration increase
            cntrl_pkt_it.read(cntrl_pkt_it_reg, 0);
            cntrl_pkt_it.write(0, (bit<32>)((int<32>)cntrl_pkt_it_reg + 1));
            
            // Step 5: Send to next switch
            cntrl_id_to_ip.apply();
            clone(CloneType.I2E,100);
        /** Process Control Check packets**/
        } else if (hdr.cntrl_check.isValid()) {
            bit<32> highest_seq_no_reg;
            highest_seen_seq_no.read(highest_seq_no_reg, 0);
            hdr.cntrl_check.switch_global_seq_no = highest_seq_no_reg;
        /** Process Append packets **/
        } else if (hdr.append.isValid()) { /** Process AppendEntry packets **/
            // Change processing of append based on the status of the packet
            // Status 1: First time the packet has been seen
            if (hdr.append.status == 1) {

                // 1) Assign local seq no TODO check how you initialize variables???
                bit<32> local_seq_no_reg;
                local_seq_no.read(local_seq_no_reg, 0);
                local_seq_no_reg = (bit<32>)((int<32>)local_seq_no_reg + hdr.append.batch_size);
                local_seq_no.write(0, local_seq_no_reg);

                // Update packet header variables
                // Set status to 2 (pending sequence number)
                hdr.append.g_idx = (int<32>)local_seq_no_reg;
                hdr.append.status = 2;
            } else if (hdr.append.status == 2) {
                bit<32> cntrl_pkt_it_reg;
                cntrl_pkt_it.read(cntrl_pkt_it_reg, 0);
                if (hdr.append.cntrl_pkt_it == (int<32>)cntrl_pkt_it_reg) {
                } else {
                    // Status 2: Packet is waiting for a global seq no assignment
                    
                    // Assign global seq no
                    bit<32> h_seen_seq_no_reg;
                    highest_seen_seq_no.read(h_seen_seq_no_reg, 0);
                    hdr.append.g_idx = hdr.append.g_idx + (int<32>)h_seen_seq_no_reg;
                    hdr.append.status = 3;
                }
            }
            bit<32> h_seen_seq_no_reg;
            highest_seen_seq_no.read(h_seen_seq_no_reg, 0);
            if (hdr.append.status == 3 && hdr.append.g_idx > (int<32>)h_seen_seq_no_reg) {
		        hdr.append.status = 4;
                hash(hdr.append.shard_id, HashAlgorithm.identity, base, {hdr.append.g_idx}, (bit<32>)NUM_SHARDS);
                get_append_shard_id.apply();
            } else {
                meta.circulate = 1;
		    }
        } else if (hdr.tail.isValid()) {
            process_tail.apply();
        }

        if (meta.circulate == 1) {
            circulate_table.apply();
        }

        /* Process IP hdr */
        if (hdr.ipv4.isValid()) {
            ipv4_lpm.apply();
        }
    }
}

/*************************************************************************
****************  E G R E S S   P R O C E S S I N G   ********************
*************************************************************************/

control MyEgress(inout headers hdr,
                 inout metadata meta,
                 inout standard_metadata_t standard_metadata) {
	
    /*action return_read(macAddr_t dstAddr, egressSpec_t port) {
        standard_metadata.egress_spec = port;
        hdr.ethernet.srcAddr = hdr.ethernet.dstAddr;
        hdr.ethernet.dstAddr = dstAddr;
        hdr.ipv4.ttl = hdr.ipv4.ttl - 1;
    }

    table check_unwritten {
        key = {
            hdr.read.unwritten: exact;
        }
        actions = {
            return_read;
            NoAction;
        }
        size = 1024;
        default_action = NoAction(); //drop();
    }*/

    apply { 
        if (hdr.cntrl.isValid() && standard_metadata.instance_type == 1) {
            hdr.ethernet.setInvalid();
            hdr.ipv4.setInvalid();
            hdr.cntrl.setInvalid();
        }
    }
}

/*************************************************************************
*************   C H E C K S U M    C O M P U T A T I O N   ***************
*************************************************************************/

control MyComputeChecksum(inout headers hdr, inout metadata meta) {
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
        packet.emit(hdr.ipv4);
	    packet.emit(hdr.cntrl);
        packet.emit(hdr.append);
	    packet.emit(hdr.tail);
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
