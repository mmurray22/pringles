/* -*- P4_16: Bare Minimum Sequencing Program -*- */
// Credit: 
// - Base code come from p4 tutorial exercises
// - CPU port code inspiration comes from https://github.com/nsg-ethz/p4-learning/tree/master/examples/copy_to_cpu
#include <core.p4>
#include <tna.p4>

#include "../../common/headers.p4"
#include "../../common/util.p4"


const bit<16> TYPE_IPV4  = 0x0800;
const bit<16> TYPE_LINK_TEST  = 0x0711;


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

// Size: 8 bits
header mirror_h {
    bit<8> pkt_type;
}

// Link test header
header link_test_t {
    bit<48> start_ts;
    bit<48> end_ts;
    bit<8> hop;
}

struct metadata {
    bit<1> do_ing_mirroring; // Enable ingress mirroring
    bit<1> do_egr_mirroring; // Enable egress mirroring
    MirrorId_t ing_mir_ses; // Ingress mirror session ID
    MirrorId_t egr_mir_ses; // Egress mirror session ID
    pkt_type_t pkt_type;

    bit<32> queue_congest;
}

struct headers {
    pktgen_timer_header_t   timer;
    ethernet_t              ethernet;
    link_test_t		    link_test;
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
            196      : parse_pktgen_timer; 
            197      : parse_pktgen_timer; 
            198      : parse_pktgen_timer; 
            199      : parse_pktgen_timer; 
            default : parse_ethernet;      // Normal clients
    	}
    }
    
    state parse_pktgen_timer {
	packet.extract(hdr.timer);
	transition parse_ethernet;
    }

    state parse_ethernet {
        packet.extract(hdr.ethernet);
	transition parse_link_test;
    }
    
    state parse_link_test {
	packet.extract(hdr.link_test);
	transition accept;
    }
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
    
     action drop() {
        ig_dprsr_md.drop_ctl = 1;
    }

    /* First hop */
    action first_hop(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
	hdr.link_test.start_ts = standard_metadata.ingress_mac_tstamp;
	hdr.link_test.hop = hdr.link_test.hop + 1;
    }

    /* Send to next hop */
    action forward_pkt(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
	hdr.link_test.hop = hdr.link_test.hop + 1;
    }

    /* Final hop - send packet back to start */
    action final_hop(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
	hdr.link_test.hop = hdr.link_test.hop + 1;
    }

    /* Final hop - send packet back to start */
    action send_to_control(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
	hdr.link_test.end_ts = standard_metadata.ingress_mac_tstamp;
    }

    table link_hop { 
        key = {
	    hdr.link_test.hop: exact;
	}
	actions = {
            first_hop;
            forward_pkt;
            final_hop;
	    send_to_control;
	    drop;
        }
        size = 64;
        default_action = drop();
    }

        
    /* Start of Ingress Pipeline */
    apply {
	link_hop.apply();
	hdr.timer.setInvalid();
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
		default: parse_ethernet;
	    }
	}

	state parse_mirror {
	    mirror_h mirror_md;
	    packet.extract(mirror_md);
	    transition parse_ethernet;
	}

        state parse_ethernet {
            packet.extract(hdr.ethernet);
	    transition parse_link_test;
        }
        
        state parse_link_test {
            packet.extract(hdr.link_test);
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
    
    apply { 
	
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
