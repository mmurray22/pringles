/* -*- P4_16: Bare Minimum Sequencing Program -*- */
// Credit: 
// - Base code come from p4 tutorial exercises
// - CPU port code inspiration comes from https://github.com/nsg-ethz/p4-learning/tree/master/examples/copy_to_cpu
#include <core.p4>
#if __TARGET_TOFINO__ == 2
#include <t2na.p4>
#else
#include <tna.p4>
#endif

#include "common/headers.p4"
#include "common/util.p4"

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
   bit<32> pkt_id;
}

// Ring type header - holds the type of the packet
header ring_type_t {
    // Type to filter packets
    bit<16> type;
    // Counting the number of entries batched in the packet
    bit<16> num_entries;
}

// AppendEntry header
header append_entry_t {
    /** Part of header: Set by client **/

    // ID of the sender client
    bit<32> cid;
    // Unique nonce used to detect duplicates of the message
    bit<32> nonce;
    // Bytes in each paylod
    bit<32> payload_size;
    // Number of payload entries
    bit<32> num_entries;
    
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

    bit<32> thread_id;
    bit<32> recv_port;
    bit<32> cli_idx;
    bit<64> timestamp;
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
    control_pkt_t           cntrl;
    ipv4_t                  ipv4;
    udp_h		    udp;
    ring_type_t             ring_type;
    control_pkt_checker_t   cntrl_check;
    append_entry_t          append;
    tail_req_t              tail;
}

/*************************************************************************
*********************** P A R S E R  ***********************************
*************************************************************************/

parser MyParser(packet_in packet,
                out headers hdr,
                out metadata meta,
                out ingress_intrinsic_metadata_t standard_metadata) {
    
    TofinoIngressParser() tofino_parser;

    state start {
        tofino_parser.apply(packet, standard_metadata);
        transition parse_ethernet;
    }

    state parse_ethernet {
        packet.extract(hdr.ethernet);
	transition select(hdr.ethernet.etherType) {
            TYPE_CONTROL: parse_control;
	    default: parse_ipv4;
        }
    }
    
    state parse_ipv4 {
        packet.extract(hdr.ipv4);
        transition parse_udp;
    }

    state parse_udp {
        packet.extract(hdr.udp);
	transition parse_ring_type;
    }

    state parse_ring_type {
	packet.extract(hdr.ring_type);
        transition select(hdr.ring_type.type) {
            TYPE_CONTROL_CHECK: parse_control_check;
            TYPE_APPEND: parse_append;
            TYPE_TAIL: parse_tail;
	    default: accept;
        }
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
		  in ingress_intrinsic_metadata_t standard_metadata,
		  in ingress_intrinsic_metadata_from_parser_t ig_prsr_md,
		  inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
		  inout ingress_intrinsic_metadata_for_tm_t ig_tm_md) {
   
    /** Registers **/
    
    /// Ring State ///

    /*DirectRegister<bit<32>>(0) ring_view; // Current ring view number TODO will need to be written to from the control plane
    DirectRegisterAction<bit<32>, bit<32>>(ring_view) view_update = {
        void apply(inout bit<32> view) {
            view = view + 1;
        }
    }; // TODO: Update view
    
    DirectRegisterAction<bit<32>, bit<32>>(ring_view) get_curr_view = {
        void apply(inout bit<32> view, out bit<32> read_view) {
            read_view = view;
        }
    };*/

    /// Sequencing state ///

    // Largest global sequence number known to the switch Updated every time control packet is received.
    Register<bit<32>, bit<32>>(1, 0) highest_seen_seq_no;
    
    RegisterAction<bit<32>, bit<32>, bit<32>>(highest_seen_seq_no) write_seen_seq_no = {
        void apply(inout bit<32> cur_seen_seq_no) {
            cur_seen_seq_no = (bit<32>)hdr.cntrl.global_seq_no;
        }
    };
    
    RegisterAction<bit<32>, bit<32>, bit<32>>(highest_seen_seq_no) read_seen_seq_no = {
        void apply(inout bit<32> new_seen_seq_no, out bit<32> old_seen_seq_no) { // inout = register, out = output 
            old_seen_seq_no = new_seen_seq_no;
        }
    };

    // Local seq no batch counter 
    Register<bit<32>, bit<1>>(1) local_seq_no;
    
    RegisterAction<bit<32>, bit<1>, bit<32>>(local_seq_no) write_local_seq_no = {
        void apply(inout bit<32> cur_local_seq_no, out bit<32> pkt_local_seq_no) {
            cur_local_seq_no = cur_local_seq_no + 1;
	    pkt_local_seq_no = cur_local_seq_no;
        }
    };
    
    RegisterAction<bit<32>, bit<1>, bit<32>>(local_seq_no) read_local_seq_no = {
        void apply(inout bit<32> cur_local_seq_no, out bit<32> final_local_seq_no) {
            final_local_seq_no = cur_local_seq_no;
	    cur_local_seq_no = 0;
        }
    };
    
    // Largest global sequence number that is known to be a replicated sequence number
    DirectRegister<bit<32>>() highest_replicated_seq_no;
    /*DirectRegisterAction<bit<32>, bit<32>, bit<32>>(highest_replicated.seq_no) write_replicated_seq_no = {
        void apply(inout bit<32> cur_replicated_seq_no) {
            cur_replicated_seq_no = (bit<32>)hdr.cntrl.global_seq_no;
        }
    };*/
    DirectRegisterAction<bit<32>, bit<32>>(highest_replicated_seq_no) read_replicated_seq_no = {
        void apply(inout bit<32> new_replicated_seq_no, out bit<32> old_replicated_seq_no) { // inout = register, out = output 
            old_replicated_seq_no = new_replicated_seq_no;
        }
    };


    // Number of total times the switch has seen the control packet TODO potential to overflow?
    DirectRegister<bit<32>>() cntrl_pkt_it; 
    DirectRegisterAction<bit<32>, bit<32>>(cntrl_pkt_it) update_cntrl_pkt_it = {
        void apply(inout bit<32> cur_cntrl_pkt_it, out bit<32> output_cntrl_pkt_it) {
	    if (hdr.cntrl.isValid()) {
                cur_cntrl_pkt_it = cur_cntrl_pkt_it + 1;
	    }
	    output_cntrl_pkt_it = cur_cntrl_pkt_it;
	}
    };

    /** MATCH-ACTION TABLES **/

    /* Standard IPv4 routing */
    action drop() {
        ig_dprsr_md.drop_ctl = 1;
    }

    /*action ipv4_forward(macAddr_t dstAddr, egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
        hdr.ethernet.srcAddr = hdr.ethernet.dstAddr;
        hdr.ethernet.dstAddr = dstAddr;
        hdr.ipv4.ttl = hdr.ipv4.ttl - 1;
    }*/

    action ipv4_forward(macAddr_t dstAddr, egressSpec_t port, ipv4_addr_t dst_ip) {
        ig_tm_md.ucast_egress_port = port;
        hdr.ethernet.srcAddr = hdr.ethernet.dstAddr;
        hdr.ethernet.dstAddr = dstAddr;
	hdr.ipv4.dstAddr = dst_ip;
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

    action udp_forward(bit<16> dst_port) {
        hdr.udp.dst_port = dst_port;
    }
    
    table udp_exact {
        key = {
            hdr.ring_type.type: exact;
        }
        actions = {
            udp_forward;
            drop;
            NoAction;
        }
        size = 1024;
        default_action = NoAction();
    }
    
    /* Circulate port */
    action circulate_port(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
    }

    table circulate_table {
        key = {
	    meta.circulate: exact;
	}
	actions = {
            circulate_port;
	    drop;
        }
        size = 1024;
        default_action = drop();
    }


    /* Control Packet */
    action cntrl_forward(egressSpec_t port, bit<32> pkt_id) {
        ig_tm_md.ucast_egress_port = port;
        hdr.cntrl.pkt_id = pkt_id;
    }

    table cntrl_id_to_ip {
        key = {
            hdr.cntrl.pkt_id: exact;
        }
        actions = {
            cntrl_forward;
            drop;
            NoAction;
        }
        size = 1024;
        default_action = drop();
    }
    
    /* Ring View */
    table check_view {
        key = {
            hdr.cntrl.ring_view: exact;
        }
        actions = {
            drop;
            NoAction;
        }
        size = 1024;
        default_action = NoAction();
    }

    /* Get tail logic*/
    /*action forward_tail(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
        hdr.tail.hops = hdr.tail.hops + 1;
	bit<32> rep_seq = highest_replicated_seq_no.apply();
	if (hdr.tail.tail_seq_no < rep_seq) {
	    hdr.tail.tail_seq_no = req_seq;
	}
    }*/

    action finish_circ() {
	meta.circulate = 0;
    }

    /*table process_tail {
        key = {
            hdr.tail.hops: exact;
        }
        actions = {
            process_tail;
            forward_tail;
            drop;
            NoAction;
        }
        size = 1024;
        default_action = drop();
    }*/

    apply {
	meta.circulate = 0;
        bit<32> cntrl_pkt_it_reg = update_cntrl_pkt_it.execute();
	// check_view.apply(); <-- what do views look like?

	if (hdr.cntrl.isValid()) {
            // Step 0: Check if the control packet's view is outdated TODO
            bit<32> local_seq_no_reg = read_local_seq_no.execute(0);
            hdr.cntrl.global_seq_no = hdr.cntrl.global_seq_no + (int<32>)local_seq_no_reg;
	    write_seen_seq_no.execute(0);
            cntrl_id_to_ip.apply();
        } else if (hdr.append.isValid()) {
            if (hdr.append.status == 1) {
                hdr.append.g_idx = (int<32>)write_local_seq_no.execute(0);
                hdr.append.status = 2;
		hdr.append.cntrl_pkt_it = (int<32>)cntrl_pkt_it_reg;
            	meta.circulate = 1;
            } else if (hdr.append.status == 2) {
                if (hdr.append.cntrl_pkt_it != (int<32>)cntrl_pkt_it_reg) {
                    int<32> h_seen_seq_no_reg = (int<32>)read_seen_seq_no.execute(0);
                    hdr.append.g_idx = hdr.append.g_idx + h_seen_seq_no_reg;
                    hdr.append.status = 3;
		    meta.circulate = 0;
                } else {
		    meta.circulate = 1;
		}
            }/*else if (hdr.append.status == 3) {
                hash(hdr.append.shard_id, HashAlgorithm.identity, base, {hdr.append.g_idx}, (bit<32>)NUM_SHARDS);
                get_append_shard_id.apply(); // <-- meta.circulate = 0
	    }*/
        } /*else if (hdr.tail.isValid()) {
	    process_tail.apply();
	}*/

	if (meta.circulate == 1) {
           circulate_table.apply();
	} else if (!hdr.cntrl.isValid() && hdr.ipv4.isValid()) {
            ipv4_lpm.apply();
        }

	if (hdr.udp.isValid()) {
	    udp_exact.apply();
	}	
    }
}

control MyIngressDeparser(
 packet_out packet,
 inout headers hdr,
 in metadata ig_md,
 in ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md)
{
	apply {
	        packet.emit(hdr);
 	}
}

/*************************************************************************
****************  E G R E S S   P R O C E S S I N G   ********************
*************************************************************************/
parser MyEgressParser(packet_in packet,
 		      out headers hdr,
 		      out metadata eg_md,
 		      out egress_intrinsic_metadata_t eg_intr_md) {
 	
	TofinoEgressParser() tofino_parser;
   	state start {
            tofino_parser.apply(packet, eg_intr_md);
            transition parse_ethernet;
        }

        state parse_ethernet {
            packet.extract(hdr.ethernet);
            transition select(hdr.ethernet.etherType) {
                TYPE_CONTROL: parse_control;
                default: parse_ipv4;
            }
        }
        
        state parse_ipv4 {
            packet.extract(hdr.ipv4);
            transition parse_udp;
        }

        state parse_udp {
            packet.extract(hdr.udp);
            transition parse_ring_type;
        }

        state parse_ring_type {
            packet.extract(hdr.ring_type);
            transition select(hdr.ring_type.type) {
                TYPE_CONTROL_CHECK: parse_control_check;
                TYPE_APPEND: parse_append;
                TYPE_TAIL: parse_tail;
                default: parse_append;
            }
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
}

control MyEgress(inout headers hdr,
                 inout metadata meta,
                 in egress_intrinsic_metadata_t standard_metadata,
		 in egress_intrinsic_metadata_from_parser_t eg_prsr_md,
		 inout egress_intrinsic_metadata_for_deparser_t eg_dprsr_md,
		 inout egress_intrinsic_metadata_for_output_port_t eg_oport_md) {
    apply { 
    }
}

/*************************************************************************
*************   C H E C K S U M    C O M P U T A T I O N   ***************
*************************************************************************/

/*control MyComputeChecksum(inout headers hdr, inout metadata meta) {
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
}*/

/*************************************************************************
***********************  D E P A R S E R  *******************************
*************************************************************************/
control MyDeparser(packet_out packet, 
		   inout headers hdr,
		   in metadata meta,
		   in egress_intrinsic_metadata_for_deparser_t eg_dprsr_md) {
    apply {
        packet.emit(hdr);
    }
}

/*************************************************************************
***********************  S W I T C H  *******************************
*************************************************************************/

Pipeline(
	MyParser(),
	MyIngress(),
	MyIngressDeparser(),
	MyEgressParser(),
	MyEgress(),
	MyDeparser()
) pipe;

Switch(pipe) main;
