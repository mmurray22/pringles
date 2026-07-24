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

#include "../common/headers.p4"
#include "../common/util.p4"


const bit<16> TYPE_IPV4  = 0x0800;
const bit<16> TYPE_CONTROL = 0x0820; // only for the switches
const bit<16> TYPE_CONTROL_CHECK = 0x0880; // only for the switches
const bit<16> TYPE_MULTICAST = 0x0890; // only for the switches
const bit<16> TYPE_APPEND = 0x0860;
const bit<16> TYPE_APPEND_RESP = 0x0861;
const bit<16> TYPE_READ = 0x0870;
const bit<16> TYPE_READ_RESP = 0x0871;
const bit<16> TYPE_TAIL = 0x0840;
const bit<16> TYPE_SUB = 0x0850;
const bit<16> TYPE_SUB_RESP = 0x0851;
const bit<16> TYPE_VIEW = 0x0830;
const bit<32> MAX_INFLIGHT_ACKS = 65536;
const bit<32> MAX_GSN_MAP_SIZE = 65535; //65536; //131072; //65536;


/*************************************************************************
*********************** H E A D E R S  ***********************************
*************************************************************************/


typedef bit<9>  egressSpec_t;
typedef bit<48> macAddr_t;
typedef bit<32> ip4Addr_t;
typedef bit<48> time_t;
typedef bit<8> pkt_type_t;
const pkt_type_t PKT_TYPE_MIRROR = 2;
const pkt_type_t PKT_TYPE_NORMAL = 1;

#if __TARGET_TOFINO__ == 1
typedef bit<3> mirror_type_t;
#else
typedef bit<4> mirror_type_t;
#endif
const mirror_type_t MIRROR_TYPE_I2E = 1;

/******* Default Headers ********/
// Size: 112 bits
header ethernet_t {
    macAddr_t dstAddr;
    macAddr_t srcAddr;
    bit<16>   etherType;
}

// Size: 160 bits
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

   // ID of last sending switch
   bit<16> pkt_id;

   bit<48> start_ts;
}

// Ring type header - holds the type of the packet
// Size: 96 bits
header ring_type_t {
    // Type to filter packets
    bit<16> type;
    // Counting the number of entries batched in the packet
    bit<16> num_entries;
    // Shard ID - to be filled in by switch
    bit<32> shard_id;
    // Switch ID - to be filled in by switch
    bit<32> switch_to_process;
}

// AppendEntry header
// Size: 320
header append_entry_t {
    /** Part of header: Set by client **/
    // Unique nonce used to detect duplicates of the message
    bit<32> nonce;
    // Bytes in each paylod
    bit<16> payload_size;
    // Stream ID
    bit<32> stream_id;
    
    /** Part of header: Set by switches **/
    
    // Global sequence number of message
    bit<32> g_idx; // TODO: NEEDS TO BECOME 64 BIT NUMBER
    
    // Metadata: The number of times the control packet had been seen when entry is first received
    bit<16> cntrl_pkt_it; // TODO: OVERFLOW?

    bit<32> client_ip;
    bit<16> recv_port;
    bit<48> start_ts;
}

/******* Custom Test Headers ********/
struct digest_append_t {
    bit<48> start_ts;
    bit<48> end_ts;
    bit<16> nonce;
    bit<32> seq_no;
}

struct metadata {
    bit<1> circulate;
    bit<8> num_recirc_ports;
    bit<8> port_idx;
    bit<1> append_process;
    bit<1> read_process;
    bit<1> route_to_shard;
    bit<1> is_cntrl;
    bit<1> send_acks;
    bit<1> route_to_client;

    // For keepign tabs on the GSN table size
    bit<32> batch_size;

    // For switch ID
    bit<16> switch_id;
    bit<16> ring_view;
   
    // For shards
    bit<32> num_shards;
    bit<32> shard_size;
 
    // For acks
    bit<32> ack_threshold;

    // For mirroring
    bit<1> do_ing_mirroring; // Enable ingress mirroring
    bit<1> do_egr_mirroring; // Enable egress mirroring
    MirrorId_t ing_mir_ses; // Ingress mirror session ID
    MirrorId_t egr_mir_ses; // Egress mirror session ID
    pkt_type_t pkt_type;

    // For packet generation
    bit<8> get_pkt_gen;
    bit<48> start_ts;
    bit<48> end_ts;
    bit<32> duration;
    bit<32> overflow;
    bit<16> nonce;
    bit<32> seq_no;
}

struct headers {
    pktgen_timer_header_t   timer;
    ethernet_t              ethernet;
    ipv4_t                  ipv4;
    udp_h		    udp; // 64 bits
    ring_type_t             ring_type;
    append_entry_t          append;
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
	transition select(standard_metadata.ingress_port) {
            default : parse_pktgen_timer;      // Normal clients
    	}
    }
    
    state parse_pktgen_timer {
	packet.extract(hdr.timer);
	transition parse_ethernet;
    }

    state parse_ethernet {
        packet.extract(hdr.ethernet);
	transition select(hdr.ethernet.etherType) {
	    default: parse_ipv4;
        }
    }
    
    state parse_ipv4 {
        packet.extract(hdr.ipv4);
        packet.extract(hdr.udp);
	transition parse_ring_type;
    }

    state parse_ring_type {
	packet.extract(hdr.ring_type);
        transition select(hdr.ring_type.type) {
            TYPE_APPEND: parse_append;
	    default: accept;
        }
    }

    state parse_append {
        packet.extract(hdr.append);
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
   
    /******** Registers *********/
    /*
     * List of registers:
     * - highest_seen_seq_no
     * - local_seq_no
     * - highest_replicated_seq_no
     * - cntrl_pkt_it
     */
    action drop() {
        ig_dprsr_md.drop_ctl = 1;
    }

    action output_port(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
    }

    @pragma ternary 1 
    table output_table {
	actions = {
            output_port;
        }
        size = 64;
        default_action = output_port(192);
    }
   
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) packet_cntr; 
    Register<bit<32>, bit<1>>(1, 1) cntr_lower; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(cntr_lower) update_low_cntr = {
        void apply(inout bit<32> new_lat_cntr, out bit<32> overflow) {
	    new_lat_cntr = new_lat_cntr + 1;
	    if (new_lat_cntr == 0) {
		overflow = 1;
	    } else {
		overflow = 0;
	    }
	}
    };
    Register<bit<32>, bit<1>>(1, 0) cntr_higher; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(cntr_higher) update_high_cntr = {
        void apply(inout bit<32> new_lat_cntr) {
	    new_lat_cntr = new_lat_cntr + 1; //meta.overflow;
	}
    };

    /**** DONE WITH MATCH ACTION TABLES *****/
    /* Start of Ingress Pipeline */
    apply {
	if (hdr.append.isValid()) {
	    packet_cntr.count(0);
	    bit<32> overflow = update_low_cntr.execute(0);
	    if (overflow != 0) {
	        update_high_cntr.execute(0);
	    }
	    drop();
	    //output_table.apply();
	}
    }
}

control MyIngressDeparser(
 packet_out packet,
 inout headers hdr,
 in metadata ig_md,
 in ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md)
{
    Mirror() mirror;
    Digest<digest_append_t>() digest_append; 

    apply {
	if (ig_dprsr_md.digest_type == 1) {
	    digest_append.pack({ig_md.start_ts, ig_md.end_ts, ig_md.nonce, ig_md.seq_no});
	}
	if (ig_dprsr_md.mirror_type == MIRROR_TYPE_I2E) {
	    mirror.emit<mirror_h>(ig_md.ing_mir_ses, {ig_md.pkt_type});
	}
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
            transition parse_pktgen_timer;      // Normal clients
        }
        
        state parse_pktgen_timer {
            packet.extract(hdr.timer);
            transition parse_ethernet;
        }

        state parse_ethernet {
            packet.extract(hdr.ethernet);
            transition select(hdr.ethernet.etherType) {
                default: parse_ipv4;
            }
        }
        
        state parse_ipv4 {
            packet.extract(hdr.ipv4);
            packet.extract(hdr.udp);
            transition parse_ring_type;
        }

        state parse_ring_type {
            packet.extract(hdr.ring_type);
            transition select(hdr.ring_type.type) {
                TYPE_APPEND: parse_append;
                default: accept;
            }
        }

        state parse_append {
            packet.extract(hdr.append);
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
	if (eg_dprsr_md.mirror_type != 0) {
            hdr.ring_type.type = TYPE_SUB_RESP;
	}
    }
}

/*************************************************************************
***********************  D E P A R S E R  *******************************
*************************************************************************/
control MyDeparser(packet_out packet, 
		   inout headers hdr,
		   in metadata meta,
		   in egress_intrinsic_metadata_for_deparser_t eg_dprsr_md) {
    apply {
        packet.emit(hdr.timer);
        packet.emit(hdr.ethernet);
        packet.emit(hdr.ipv4);
        packet.emit(hdr.udp);
        packet.emit(hdr.ring_type);
        packet.emit(hdr.append);
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
