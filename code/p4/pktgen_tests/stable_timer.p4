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


const bit<16> TYPE_TIMER = 0x0660;


const bit<16> TYPE_IPV4  = 0x0800;
const bit<16> TYPE_CONTROL = 0x0820; // only for the switches
const bit<16> TYPE_CONTROL_CHECK = 0x0880; // only for the switches
const bit<16> TYPE_MULTICAST = 0x0890; // only for the switches
const bit<16> TYPE_APPEND = 0x0860;
const bit<16> TYPE_APPEND_WAIT = 0x0862;
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

// Ring type header - holds the type of the packet
// Size: 96 bits
header ring_type_t {
    bit<16> type;
    bit<16> exp_type;
    bit<48> start_ts;
    bit<48> end_ts;
    bit<32> g_idx;
    bit<16> raw_elapsed_time;
    bit<32> raw_duration;
}

struct metadata {
    bit<1> circulate;
    bit<1> wait;
    
    bit<8> num_recirc_ports;
    bit<8> num_wait_ports;
    bit<8> port_idx;

    // For switch ID
    bit<16> switch_id;
    bit<16> wait_time;

    bit<32> queue_congest;
    bit<32> duration;
    bit<32> overflow;
    bit<32> seq_no;
    bit<32> ack_threshold;

    // For mirroring
    bit<1> do_ing_mirroring; // Enable ingress mirroring
    bit<1> do_egr_mirroring; // Enable egress mirroring
    MirrorId_t ing_mir_ses; // Ingress mirror session ID
    MirrorId_t egr_mir_ses; // Egress mirror session ID
    pkt_type_t pkt_type;

    bit<48> start_ts;
    bit<48> end_ts;
}

struct headers {
    pktgen_timer_header_t   timer;
    ethernet_t              ethernet;
    ipv4_t                  ipv4;
    udp_h		    udp; // 64 bits
    ring_type_t             ring_type;
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
            12      : parse_pktgen_timer; // Adjust port # for your Pipe
            20      : parse_pktgen_timer; // Adjust port # for your Pipe
            56      : parse_pktgen_timer; // Adjust port # for your Pipe
            68      : parse_pktgen_timer; // Adjust port # for your Pipe
            69      : parse_pktgen_timer; // Adjust port # for your Pipe
            70      : parse_pktgen_timer; // Adjust port # for your Pipe
            71      : parse_pktgen_timer; // Adjust port # for your Pipe
            140      : parse_pktgen_timer; // Recirculated generated packet!
            168      : parse_pktgen_timer; // Recirculated generated packet!
            172      : parse_pktgen_timer; // Recirculated generated packet!
            196      : parse_pktgen_timer; // Recirculated generated packet!
            197      : parse_pktgen_timer; // Recirculated generated packet!
            198      : parse_pktgen_timer; // Recirculated generated packet!
            199      : parse_pktgen_timer; // Recirculated generated packet!
            default : parse_ethernet;      // Normal clients
    	}
    }
    
    state parse_pktgen_timer {
	packet.extract(hdr.timer);
	transition parse_ethernet;
    }

    state parse_ethernet {
        packet.extract(hdr.ethernet);
	transition parse_ipv4;
    }
    
    state parse_ipv4 {
        packet.extract(hdr.ipv4);
        packet.extract(hdr.udp);
	transition parse_ring_type;
    }

    state parse_ring_type {
	packet.extract(hdr.ring_type);
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
    
    //////// Ports for recirculation of data /////////
    Register<bit<8>, bit<1>>(1, 0) recirc_port_idx;
    RegisterAction<bit<8>, bit<1>, bit<8>>(recirc_port_idx) get_recirc_idx = {
        void apply(inout bit<8> curr_recirc_idx, out bit<8> recirc_idx) {
	    if ((curr_recirc_idx + 1) == meta.num_recirc_ports) {
		curr_recirc_idx = 0;
		recirc_idx = 0;
	    } else {
	        curr_recirc_idx = curr_recirc_idx + 1;
	    	recirc_idx = curr_recirc_idx;
	    }
	}
    };

    Register<bit<8>, bit<1>>(1, 0) wait_port_idx;
    RegisterAction<bit<8>, bit<1>, bit<8>>(wait_port_idx) get_wait_idx = {
        void apply(inout bit<8> curr_wait_idx, out bit<8> wait_idx) {
	    if ((curr_wait_idx + 1) == meta.num_wait_ports) {
		curr_wait_idx = 0;
		wait_idx = 0;
	    } else {
	        curr_wait_idx = curr_wait_idx + 1;
	    	wait_idx = curr_wait_idx;
	    }
	}
    };


    //////// Performance Statistics (Packet Gen) /////////
    const bit<32> LATENCY_INIT_VAL = 2147483647; // This only stores SIGNED 32 bit integers...even though it's an unsigned uint32
    Register<bit<32>, bit<1>>(1, 0) latency_lower; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(latency_lower) update_low_lat_cntr = {
        void apply(inout bit<32> new_lat_cntr, out bit<32> overflow) {
	    if (new_lat_cntr > (LATENCY_INIT_VAL - meta.duration)) {
		overflow = 1;
	    } else {
		overflow = 0;
	    }
	    new_lat_cntr = new_lat_cntr + meta.duration;
	}
    };
    Register<bit<32>, bit<1>>(1, 0) latency_higher; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(latency_higher) update_high_lat_cntr = {
        void apply(inout bit<32> new_lat_cntr) {
	    new_lat_cntr = new_lat_cntr + 1; //meta.overflow;
	}
    };
    Register<bit<32>, bit<1>>(1, 0) raw_duration; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(raw_duration) update_raw_duration = {
        void apply(inout bit<32> raw_cntr) {
	    raw_cntr = meta.duration;
	}
    };
    Register<bit<16>, bit<1>>(1, 0) raw_elapsed_time; 
    RegisterAction<bit<16>, bit<1>, bit<16>>(raw_elapsed_time) update_raw_elapsed_time = {
        void apply(inout bit<16> raw_cntr) {
	    raw_cntr = hdr.ring_type.raw_elapsed_time;
	}
    };


    
    /****************** MATCH-ACTION TABLES ******************/
    /* List of MAU tables:
     * - ipv4_lpm
     * - circulate_table
     * - shard_id
     * - check_switch_routing
     * - cntrl_id_to_ip
     * - check_cntrl_view
     * - process_tail
     */    
     action drop() {
        ig_dprsr_md.drop_ctl = 1;
    }

    /* Circulate port */
    action circulate_port(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
    }

    @pragma ternary 1 
    table circulate_table {
        key = {
	    meta.port_idx: exact;
	}
	actions = {
            circulate_port;
	    drop;
        }
        size = 64;
        default_action = drop();
    }

    /* Wait port */
    action wait_port(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
    }

    @pragma ternary 1 
    table wait_table {
        key = {
	    meta.port_idx: exact;
	}
	actions = {
            wait_port;
	    drop;
        }
        size = 64;
        default_action = drop();
    }

    
    /* Recirculation ports - for logic */
    action get_num_recirc(bit<8> num_recirc_ports) {
        meta.num_recirc_ports = num_recirc_ports;
    }

    @pragma ternary 1 
    table num_recirc_port_table {
	actions = {
            get_num_recirc;
        }
        size = 64;
        default_action = get_num_recirc(1);
    }

    /* Recirculation ports - for arbitrary waiting */
    action get_num_wait(bit<8> num_wait_ports) {
        meta.num_wait_ports = num_wait_ports;
    }

    @pragma ternary 1 
    table num_wait_port_table {
	actions = {
            get_num_wait;
        }
        size = 64;
        default_action = get_num_wait(1);
    }

    /**** DONE WITH MATCH ACTION TABLES *****/
     
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) tot_packet_counter; 

    /* Start of Ingress Pipeline */
    apply {
	meta.circulate = 0;
	meta.wait = 0;
	meta.end_ts = 0;
	meta.start_ts = 0;

        if (hdr.ring_type.type == TYPE_APPEND) {
	    if (hdr.ring_type.g_idx == 0) {
		//hdr.ring_type.start_ts = standard_metadata.ingress_mac_tstamp;
	        hdr.ring_type.g_idx = 1;	    
		meta.circulate = 1;
	    } else if (hdr.ring_type.exp_type == 1) {
		// Get latency timestamps
		tot_packet_counter.count(0);
		drop();
	    } else {
		hdr.ring_type.start_ts = standard_metadata.ingress_mac_tstamp;
		hdr.ring_type.type = TYPE_APPEND_WAIT;
	    	hdr.ring_type.end_ts = standard_metadata.ingress_mac_tstamp;
		meta.wait = 1;
	    }
        } else if (hdr.ring_type.type == TYPE_APPEND_WAIT) {
	    hdr.ring_type.end_ts = standard_metadata.ingress_mac_tstamp;
	    meta.wait = 1;
	} else if (hdr.ring_type.type == TYPE_APPEND_RESP) {
            bit<32> ready_for_ack = 1;
	    if (ready_for_ack == 1) {
	        tot_packet_counter.count(0);
	    	meta.duration = (bit<32>)(standard_metadata.ingress_mac_tstamp - hdr.ring_type.start_ts);
	        bit<32> overflow = update_low_lat_cntr.execute(0);
		if (overflow != 0) {
		    update_high_lat_cntr.execute(0);
		}

		update_raw_duration.execute(0);
		update_raw_elapsed_time.execute(0);
	        drop();
	    }
	}


	/********* ROUTING LOGIC ***********/
	if (meta.circulate == 1) {
	    num_recirc_port_table.apply();
	    meta.port_idx = get_recirc_idx.execute(0);
            circulate_table.apply();
	} else if (meta.wait == 1) {
	    num_wait_port_table.apply();
	    meta.port_idx = get_wait_idx.execute(0);
            wait_table.apply();
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

    apply {
	/*if (ig_dprsr_md.digest_type == 1) {
	    digest_ring_type.pack({ig_md.start_ts, ig_md.end_ts, ig_md.nonce, ig_md.seq_no});
	}*/
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
            tofino_parser.apply(packet, eg_intr_md);
	    mirror_h mirror_md = packet.lookahead<mirror_h>();
	    transition select(mirror_md.pkt_type) {
		PKT_TYPE_MIRROR: parse_mirror;
		default: parse_pktgen_timer;
	    }
	}

	state parse_mirror {
	    mirror_h mirror_md;
	    packet.extract(mirror_md);
	    transition parse_pktgen_timer;
	}

        state parse_pktgen_timer {
            packet.extract(hdr.timer);
            transition parse_ethernet;
        }

        state parse_ethernet {
            packet.extract(hdr.ethernet);
	    transition parse_ipv4;
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
	    transition accept;
        }
}

control MyEgress(inout headers hdr,
                 inout metadata meta,
                 in egress_intrinsic_metadata_t standard_metadata,
		 in egress_intrinsic_metadata_from_parser_t eg_prsr_md,
		 inout egress_intrinsic_metadata_for_deparser_t eg_dprsr_md,
		 inout egress_intrinsic_metadata_for_output_port_t eg_oport_md) {

    Register<bit<32>, bit<1>>(1, 0) queue_cnt; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(queue_cnt) curr_queue_cnt = {
        void apply(inout bit<32> new_queue_cnt) {
	    new_queue_cnt = meta.queue_congest;
	}
    };
    
    Register<bit<32>, bit<1>>(1, 0) start_ts; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(start_ts) update_start_ts = {
        void apply(inout bit<32> new_start) {
	    new_start = (bit<32>)hdr.ring_type.start_ts;
	}
    };

    Register<bit<32>, bit<1>>(1, 0) end_ts; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(end_ts) update_end_ts = {
        void apply(inout bit<32> new_end) {
	    new_end = (bit<32>)hdr.ring_type.end_ts;
	}
    };

    action update_type() {
	hdr.ring_type.type = TYPE_APPEND_RESP;
    }
    
    table check_duration {
	key = {
            hdr.ring_type.raw_elapsed_time: range;
	}
	actions = {
	    update_type;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }

    apply { 
	if (eg_dprsr_md.mirror_type != 0) {
            hdr.ring_type.type = TYPE_SUB_RESP;
	}
	if (hdr.timer.isValid()) {
            meta.queue_congest = (bit<32>)(standard_metadata.deq_qdepth);
            curr_queue_cnt.execute(0);
	}
	if (hdr.ring_type.type == TYPE_APPEND_WAIT) {
	    //hdr.ring_type.raw_duration = (bit<32>)(hdr.ring_type.end_ts - hdr.ring_type.start_ts);
	    hdr.ring_type.raw_elapsed_time = (bit<16>)(hdr.ring_type.end_ts - hdr.ring_type.start_ts);
	    update_start_ts.execute(0);
	    update_end_ts.execute(0);
            check_duration.apply();
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
