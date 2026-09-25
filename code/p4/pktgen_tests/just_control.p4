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
const bit<8> CANARY_VALUE = 0xA5; //65536; //131072; //65536;


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
    bit<48> start_ts;
    bit<48> end_ts;
    bit<16> raw_elapsed_time;

}

header latency_bridge {
    bit<1> has_timer;
    bit<7> _pad;
    bit<32> ig_ts;
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
    bit<16> cntrl_pkt_it; // TODO: OVERFLOW?

    bit<32> client_ip;
    bit<16> recv_port;
    bit<48> start_ts;
    bit<16> exp_type;
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
    bit<32> client_ip;
    bit<48> start_ts;
}

// Header for requesting the current tail
// Size: 128
header tail_req_t {
    /** Filled in by client **/

    // Unique nonce used to detect duplicates of the message
    bit<16> nonce;
    
       
    /*Altered by switches*/

    // Keeps track of how many nodes in the ring the tail request has been passed to
    bit<16> hops;

    /** Filled in by switches **/

    // Sequence number that is the current tail - filled in by the switches
    bit<32> tail_seq_no; // TODO: 64 BITS
    bit<32> client_ip;
    bit<16> recv_port;
}

// Subscribe header
// Size: 160
header subscribe_req_t {
    bit<32> g_idx;
    // Stream ID
    bit<32> stream_id;

    bit<32> subscribe; // TODO??

    bit<32> client_ip;
    bit<16> recv_port;
    bit<16> subscribe_port;
}


/***** View Change Headers ******/
header start_view_change_t {
    bit<16> current_ring_view;
    bit<16> old_ring_view;
    bit<16> switch_id;
    bit<16> sender_switch_id;
    bit<32> highest_seen_seq_no;
}

header canary_t {
    bit<8> magic;
}

struct metadata {
    bit<1> circulate;
    bit<1> wait;
    bit<1> append_process;
    bit<1> read_process;
    bit<1> route_to_shard;
    bit<1> is_cntrl;
    bit<1> route_to_client;
   
    bit<8> canary_ok; 
    
    bit<8> num_recirc_ports;
    bit<8> num_wait_ports;
    bit<8> port_idx;

    // For switch ID
    bit<16> switch_id;
    bit<16> wait_time;
    bit<16> nonce;
    bit<16> ring_view;
    bit<16> num_shards;

    bit<32> batch_size; // For keeping tabs on the GSN table size
    bit<32> queue_congest;
    bit<32> shard_size;
    bit<32> duration;
    int<32> overflow;
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
    bit<32> elapse_time;
}

struct headers {
    ethernet_t              ethernet;
    control_pkt_t           cntrl;
    canary_t		    canary;
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
            default: accept;
        }
    }
    
    state parse_control {
        packet.extract(hdr.cntrl);
        transition parse_canary;
    }
    
    state parse_canary {
	packet.extract(hdr.canary);
	transition accept;
    }

}

/*************************************************************************
**************  I N G R E S S   P R O C E S S I N G   *******************
*************************************************************************/
control MyIngress0(inout headers hdr,
                  inout metadata meta,
		  in ingress_intrinsic_metadata_t standard_metadata,
		  in ingress_intrinsic_metadata_from_parser_t ig_prsr_md,
		  inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
		  inout ingress_intrinsic_metadata_for_tm_t ig_tm_md) {
 

    /******** Registers *********/
    action drop() {
        ig_dprsr_md.drop_ctl = 1;
    }

    /////// Sequencing state /////////
    /// Local seq no batch counter ///
    Register<bit<32>, bit<1>>(1) local_seq_no;
    RegisterAction<bit<32>, bit<1>, bit<32>>(local_seq_no) read_local_seq_no = {
        void apply(inout bit<32> cur_local_seq_no, out bit<32> final_local_seq_no) {
            final_local_seq_no = cur_local_seq_no;
	    cur_local_seq_no = 0;
        }
    };
    
    // Largest global sequence number known to the switch Updated every time control packet is received.
    Register<bit<32>, bit<16>>(MAX_GSN_MAP_SIZE, 0) map_from_gsn; // TODO: How to increment/track this sequence number value?
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_gsn) add_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = hdr.cntrl.global_seq_no;
        }
    };
  
    Register<bit<16>, bit<1>>(1, 0) active_batches;
    RegisterAction<bit<16>, bit<1>, bit<16>>(active_batches) add_batch = {
        void apply(inout bit<16> num_batches) { // inout = register, out = output 
	    num_batches = num_batches + 1;
        }
    };

    Register<bit<32>, bit<16>>(MAX_GSN_MAP_SIZE, 0) map_from_batch_sz; // TODO: How to increment/track this sequence number value?
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_batch_sz) set_batch_sz = {
        void apply(inout bit<32> batch_slot) { // inout = register, out = output 
	    batch_slot = meta.batch_size;
        }
    };


    /////// Control Packet Iteration //////
    // Number of total times the switch has seen the control packet TODO potential to overflow?
    Register<bit<16>, bit<1>>(1, 0) cntrl_pkt_it; 
    RegisterAction<bit<16>, bit<1>, bit<16>>(cntrl_pkt_it) update_cntrl_pkt_it = {
        void apply(inout bit<16> cur_cntrl_pkt_it, out bit<16> output_cntrl_pkt_it) {
	    output_cntrl_pkt_it = cur_cntrl_pkt_it;
	    if (cur_cntrl_pkt_it == (bit<16>)MAX_GSN_MAP_SIZE) {
                cur_cntrl_pkt_it = 0;
	    } else {
                cur_cntrl_pkt_it = cur_cntrl_pkt_it + 1;
	    }
	}
    };
    RegisterAction<bit<16>, bit<1>, bit<16>>(cntrl_pkt_it) get_cntrl_pkt_it = {
        void apply(inout bit<16> cur_cntrl_pkt_it, out bit<16> output_cntrl_pkt_it) {
	    output_cntrl_pkt_it = cur_cntrl_pkt_it;
	}
    };

    /**** CONTROL PACKET *****/
    action cntrl_forward(egressSpec_t port, bit<16> pkt_id) {
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
    
    /**** DONE WITH MATCH ACTION TABLES *****/
    
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) cntrl_packet_counter;  // Tracks when the control packet actually increments
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) cntrl_counter; 
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) canary_no_match_counter_ig0; 

    

    /* Start of Ingress Pipeline */
    apply {
	meta.circulate = 0;
	meta.wait = 0;
	meta.is_cntrl = 0;
	meta.route_to_shard = 0;
	meta.route_to_client = 0;
	meta.read_process = 1;
	meta.append_process = 1;
        meta.pkt_type = PKT_TYPE_NORMAL;

	
	bit<32> local_seq_no_reg = 0;
	if (hdr.cntrl.isValid()) {
		local_seq_no_reg = read_local_seq_no.execute(0);
	}
            
	if (hdr.cntrl.isValid()) {
            // Step 0: Check if the control packet's view is outdated TODO
	    //check_cntrl_view.apply();
	    if (local_seq_no_reg != 0) {
                bit<16> cntrl_pkt_it_reg = update_cntrl_pkt_it.execute(0);
	        add_gsn.execute(cntrl_pkt_it_reg);
                hdr.cntrl.global_seq_no = hdr.cntrl.global_seq_no + local_seq_no_reg;

	        cntrl_packet_counter.count(0);

		meta.batch_size = local_seq_no_reg;
		add_batch.execute(0);
		set_batch_sz.execute(cntrl_pkt_it_reg);
	    } else {
                bit<16> cntrl_pkt_it_reg = get_cntrl_pkt_it.execute(0);
	        add_gsn.execute(cntrl_pkt_it_reg);
	    }
	    cntrl_counter.count(0);

            cntrl_id_to_ip.apply();

	    if (hdr.canary.magic != CANARY_VALUE) {
	        canary_no_match_counter_ig0.count(0);
	        drop();
            }

        }
     }
}

control MyIngress1(inout headers hdr,
                  inout metadata meta,
		  in ingress_intrinsic_metadata_t standard_metadata,
		  in ingress_intrinsic_metadata_from_parser_t ig_prsr_md,
		  inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
		  inout ingress_intrinsic_metadata_for_tm_t ig_tm_md) {
 
    /******** Registers *********/
    action drop() {
        ig_dprsr_md.drop_ctl = 1;
    }

    /////// Sequencing state /////////
    /// Local seq no batch counter ///
    Register<bit<32>, bit<1>>(1) local_seq_no;
    RegisterAction<bit<32>, bit<1>, bit<32>>(local_seq_no) read_local_seq_no = {
        void apply(inout bit<32> cur_local_seq_no, out bit<32> final_local_seq_no) {
            final_local_seq_no = cur_local_seq_no;
	    cur_local_seq_no = 0;
        }
    };
    
    // Largest global sequence number known to the switch Updated every time control packet is received.
    Register<bit<32>, bit<16>>(MAX_GSN_MAP_SIZE, 0) map_from_gsn; // TODO: How to increment/track this sequence number value?
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_gsn) add_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = hdr.cntrl.global_seq_no;
        }
    };
  
    Register<bit<16>, bit<1>>(1, 0) active_batches;
    RegisterAction<bit<16>, bit<1>, bit<16>>(active_batches) add_batch = {
        void apply(inout bit<16> num_batches) { // inout = register, out = output 
	    num_batches = num_batches + 1;
        }
    };

    Register<bit<32>, bit<16>>(MAX_GSN_MAP_SIZE, 0) map_from_batch_sz; // TODO: How to increment/track this sequence number value?
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_batch_sz) set_batch_sz = {
        void apply(inout bit<32> batch_slot) { // inout = register, out = output 
	    batch_slot = meta.batch_size;
        }
    };


    /////// Control Packet Iteration //////
    // Number of total times the switch has seen the control packet TODO potential to overflow?
    Register<bit<16>, bit<1>>(1, 0) cntrl_pkt_it; 
    RegisterAction<bit<16>, bit<1>, bit<16>>(cntrl_pkt_it) update_cntrl_pkt_it = {
        void apply(inout bit<16> cur_cntrl_pkt_it, out bit<16> output_cntrl_pkt_it) {
	    output_cntrl_pkt_it = cur_cntrl_pkt_it;
	    if (cur_cntrl_pkt_it == (bit<16>)MAX_GSN_MAP_SIZE) {
                cur_cntrl_pkt_it = 0;
	    } else {
                cur_cntrl_pkt_it = cur_cntrl_pkt_it + 1;
	    }
	}
    };
    RegisterAction<bit<16>, bit<1>, bit<16>>(cntrl_pkt_it) get_cntrl_pkt_it = {
        void apply(inout bit<16> cur_cntrl_pkt_it, out bit<16> output_cntrl_pkt_it) {
	    output_cntrl_pkt_it = cur_cntrl_pkt_it;
	}
    };

    /**** CONTROL PACKET *****/
    action cntrl_forward(egressSpec_t port, bit<16> pkt_id) {
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
    
    /**** DONE WITH MATCH ACTION TABLES *****/
    
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) cntrl_packet_counter;  // Tracks when the control packet actually increments
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) cntrl_counter; 
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) canary_no_match_counter_ig1; \

    Register<bit<8>, bit<1>>(1, 1) true_magic_val;
    RegisterAction<bit<8>, bit<1>, bit<8>>(true_magic_val) write_magic = {
        void apply(inout bit<8> magic_val) {
            magic_val = hdr.canary.magic;
        }
    };

    Register<bit<16>, bit<1>>(1, 1) pesky_ether_type;
    RegisterAction<bit<16>, bit<1>, bit<16>>(pesky_ether_type) read_ether_type = {
        void apply(inout bit<16> etherType) {
	    if (hdr.ethernet.isValid()) {
                etherType = hdr.ethernet.etherType;
	    } else {
	    	etherType = 0;
	    }
        }
    };
    
    apply {
	meta.circulate = 0;
	meta.wait = 0;
	meta.is_cntrl = 0;
	meta.route_to_shard = 0;
	meta.route_to_client = 0;
	meta.read_process = 1;
	meta.append_process = 1;
        meta.pkt_type = PKT_TYPE_NORMAL;

	
	bit<32> local_seq_no_reg = 0;
	if (hdr.cntrl.isValid()) {
		local_seq_no_reg = read_local_seq_no.execute(0);
	}
            
	if (hdr.cntrl.isValid()) {
            // Step 0: Check if the control packet's view is outdated TODO
	    if (local_seq_no_reg != 0) {
                bit<16> cntrl_pkt_it_reg = update_cntrl_pkt_it.execute(0);
	        add_gsn.execute(cntrl_pkt_it_reg);
                hdr.cntrl.global_seq_no = hdr.cntrl.global_seq_no + local_seq_no_reg;

	        cntrl_packet_counter.count(0);

		meta.batch_size = local_seq_no_reg;
		add_batch.execute(0);
		set_batch_sz.execute(cntrl_pkt_it_reg);
	    } else {
                bit<16> cntrl_pkt_it_reg = get_cntrl_pkt_it.execute(0);
	        add_gsn.execute(cntrl_pkt_it_reg);
	    }
	    if (hdr.ethernet.isValid()) {
		cntrl_counter.count(0);
	    }

            cntrl_id_to_ip.apply();

	    if (hdr.canary.magic != CANARY_VALUE) {
	        canary_no_match_counter_ig1.count(0);
	 	write_magic.execute(0);
		read_ether_type.execute(0);
	        drop();
            }

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
 		      out metadata meta,
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
                default: accept;
            }
        }
        
        state parse_control {
            packet.extract(hdr.cntrl);
            transition parse_canary;
        }
        
        state parse_canary {
            packet.extract(hdr.canary);
            transition accept;
        }
}

control MyEgress0(inout headers hdr,
                 inout metadata meta,
                 in egress_intrinsic_metadata_t standard_metadata,
		 in egress_intrinsic_metadata_from_parser_t eg_prsr_md,
		 inout egress_intrinsic_metadata_for_deparser_t eg_dprsr_md,
		 inout egress_intrinsic_metadata_for_output_port_t eg_oport_md) {

    action drop() {
        eg_dprsr_md.drop_ctl = 1;
    }


    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) canary_no_match_counter_eg0; 

    apply { 
	if (hdr.canary.magic != CANARY_VALUE) {
	    canary_no_match_counter_eg0.count(0);
	    drop();
	}
    }
}

control MyEgress1(inout headers hdr,
                 inout metadata meta,
                 in egress_intrinsic_metadata_t standard_metadata,
		 in egress_intrinsic_metadata_from_parser_t eg_prsr_md,
		 inout egress_intrinsic_metadata_for_deparser_t eg_dprsr_md,
		 inout egress_intrinsic_metadata_for_output_port_t eg_oport_md) {

    action drop() {
        eg_dprsr_md.drop_ctl = 1;
    }

    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) canary_no_match_counter_eg1; 

    apply { 
	if (hdr.canary.magic != CANARY_VALUE) {
	    canary_no_match_counter_eg1.count(0);
	    drop();
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
        packet.emit(hdr);
    }
}

/*************************************************************************
***********************  S W I T C H  *******************************
*************************************************************************/

Pipeline(
	MyParser(),
	MyIngress0(),
	MyIngressDeparser(),
	MyEgressParser(),
	MyEgress0(),
	MyDeparser()
) pipe0;

Pipeline(
	MyParser(),
	MyIngress1(),
	MyIngressDeparser(),
	MyEgressParser(),
	MyEgress1(),
	MyDeparser()
) pipe1;

Switch(pipe0, pipe1) main;
