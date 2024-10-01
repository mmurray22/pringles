/* -*- P4_16 -*- */
// Credit: Base code come from p4 tutorial exercises
#include <core.p4>
#include <v1model.p4>

const bit<16> TYPE_TUNNEL = 0x1212;
const bit<16> TYPE_IPV4  = 0x0800;
const bit<16> TYPE_CONTROL = 0x0820;
const bit<16> TYPE_TAIL = 0x0840;
const bit<16> TYPE_CLI_SEQ = 0x1414;
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

// TODO: All these headers are very redundant :(
// Header for indicating acking entry
header ack_entry_t {
    // Log index which is being acked
    bit<32> log_idx;
}

// Header for requesting entry from storage server 
header store_entry_t {
    // Log index which is being stored
    bit<32> log_idx;
}

// Header for replying to Read request
header send_entry_t {
    // Log index of entry storage server is sending to client 
    bit<32> log_idx;
}

// Header for Read request
header get_entry_t {
    // Log index which client wants to read
    bit<32> log_idx;
}

// Header for requesting tail
header tail_req_t {
    // Value which should be filled in by the switch with the latest tail sequence number
    bit<32> tail_no;
}

// Header for updating the tail in the switch 
header update_tail_t {
    bit<32> tail_no;
}

// Header for sequence number requests
header sequence_no_request_t {
    // Value which should be filled in by the switch with the next local sequence counter value
    bit<32> local_sequence_no;
}

// Header for control packet
// Currently, control packet is sometimes forwarded to the clients
header control_pkt_t {
   // Value of the global sequence number before the latest switch's local sequence cntr update
   // Meant to be read by clients
   bit<32> last_global_offset;
   
   // This is the current global sequence number.
   bit<32> global_seq_no;
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
    sequence_no_request_t   client_req;
    //update_tail_t           update_tail;
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
	    default: accept;
        }
    }

    state parse_control {
	packet.extract(hdr.cntrl);
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
    register<bit<32>>(1) local_seq_cntr_reg; // Rack local sequence number. Reset each time it's added to global counter
    register<bit<32>>(1) last_seen_global_seq_no_reg; // Global sequence number. Updated every time control packet is received
    register<bit<32>>(1) tail; // Tail of the log, latest committed index
    register<bit<32>>(1) epoch; // Keeps track of the number of rounds the control packet has made
    register<bit<32>>(1) view_number; // Keeps track of the view (i.e. current configuration of the system)
    register<bit<32>>(1) cntrl_id; // Control packet ID

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

    apply {

	// Control packet: Sequence number update
        if (hdr.cntrl.isValid()) {
	    // Read the registers
	    bit<32> global_seq_no;
	    last_seen_global_seq_no_reg.read(global_seq_no, 0);
	    bit<32> local_cntr;
	    local_seq_cntr_reg.read(local_cntr, 0);

	    // Updating the global sequence counter and the last global offset
	    hdr.cntrl.last_global_offset = hdr.cntrl.global_seq_no;
	    global_seq_no = local_cntr  + hdr.cntrl.global_seq_no;
	    hdr.cntrl.global_seq_no = global_seq_no;

	    // Update the registers
	    last_seen_global_seq_no_reg.write(0, global_seq_no);
	    local_seq_cntr_reg.write(0, 0);

	    standard_metadata.mcast_grp = 1;
	}

	if (hdr.client_req.isValid()) {
	    // Updating the local sequence counter
	    bit<32> local_cntr; 
	    local_seq_cntr_reg.read(local_cntr, 0);
	    local_cntr = local_cntr + 1;
	    hdr.client_req.local_sequence_no = local_cntr;
	    local_seq_cntr_reg.write(0, local_cntr);
	}

	if (hdr.ipv4.isValid()) {
            ipv4_lpm.apply();
        }

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
