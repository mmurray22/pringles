/* -*- P4_16 -*- */
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

// Currently unused ethernet types 
const bit<16> TYPE_HELLO = 0x0888;
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
/*
 * Numbers which need to be read as integers or take part in arithmetic operations are ints.
 * Numbers that are merely for identification are bit arrays.
*/

header hello_world_t {
    bit<32> hello;
}

struct metadata {
}

struct headers {
    ethernet_t              ethernet;
    ipv4_t                  ipv4;
    hello_world_t	    hello;
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

    state parse_hello {
        packet.extract(hdr.hello);
        transition accept;
    }

    state parse_ethernet {
	packet.extract(hdr.ethernet);
	transition parse_ipv4;
    }

    state parse_ipv4 {
        packet.extract(hdr.ipv4);
	transition select(hdr.ethernet.etherType) {
	    TYPE_HELLO: parse_hello;
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
		  in ingress_intrinsic_metadata_t standard_metadata,
		  in ingress_intrinsic_metadata_from_parser_t ig_prsr_md,
		  inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
		  inout ingress_intrinsic_metadata_for_tm_t ig_tm_md) {
   
    /** Registers **/
    const bit<32> max_counter_sz = 1 << 10;
    Register<bit<32>, bit<32>>(max_counter_sz, 0) counters;
    // A simple one-bit register action that returns the inverse of the value
    // stored in the register table.
    RegisterAction<bit<32>, bit<32>, bit<32>>(counters) counter_action = {
        void apply(inout bit<32> val, out bit<32> rv) {
            val = val + 1;
	    rv = val;
        }
    };

    /* Standard IPv4 routing */
    action drop() {
        ig_dprsr_md.drop_ctl = 1;
    }

    action ipv4_forward(macAddr_t dstAddr, egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
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
        default_action = drop();
    }

    /* Hello world */
    action increment() {
	hdr.hello.hello = hdr.hello.hello + 1;
    }
    
    table trigger_counter {
        key = {
            hdr.hello.hello: exact;
        }
        actions = {
            increment;
            drop;
            NoAction;
        }
        size = 1024;
        default_action = drop();
    }
    
    apply { // Parcel all header state changes into actions TODO 
        /** Process Hello World packet **/
        if (hdr.hello.isValid()) {
	    //trigger_counter.apply();
	    bit<32> idx_ = 1;
	    //hdr.hello.hello = increment_counter.execute(idx_);
            // Purposely assigning bypass_egress field like this so that the
            // compiler generates a match table internally for this register
            // table. (Note that this internally generated table is not
            // published in bf-rt.json but is only published in context.json)
            hdr.hello.hello = counter_action.execute(idx_);
        }

        /* Process IP hdr */
        if (hdr.ipv4.isValid()) {
            ipv4_lpm.apply();
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
        packet.emit(hdr.ethernet);
        packet.emit(hdr.ipv4);
        packet.emit(hdr.hello);
 }
}

/*************************************************************************
****************  E G R E S S   P R O C E S S I N G   ********************
*************************************************************************/
parser MyEgressParser(
 packet_in packet,
 out headers hdr,
 out metadata eg_md,
 out egress_intrinsic_metadata_t eg_intr_md)
{
 	TofinoEgressParser() tofino_parser;
 	state start {
        	tofino_parser.apply(packet, eg_intr_md);
        	transition parse_ethernet;
    	}
	state parse_hello {
        	packet.extract(hdr.hello);
        	transition accept;
  	}

  	state parse_ethernet {
  		packet.extract(hdr.ethernet);
  	    	transition parse_ipv4;
  	}

  	state parse_ipv4 {
  	      packet.extract(hdr.ipv4);
  	      transition select(hdr.ethernet.etherType) {
  	          TYPE_HELLO: parse_hello;
  	          default: accept;
  	      }
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
        packet.emit(hdr.ethernet);
	packet.emit(hdr.ipv4);
	packet.emit(hdr.hello);
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
