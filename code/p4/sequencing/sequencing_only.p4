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

// ReadEntry header
// Size: 368
header read_entry_t {
    /** Part of header: Set by client **/

    // Unique nonce used to detect duplicates of the message
    bit<32> nonce;
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
    bit<32> nonce;
    
       
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

/******* Custom Test Headers ********/
// Check control packet variables
header control_pkt_checker_t {
   // Read switch's current global sequence number.
   bit<32> switch_global_seq_no;
}

struct digest_append_t {
    bit<48> start_ts;
    bit<48> end_ts;
    bit<16> nonce;
    bit<32> seq_no;
}

struct metadata {
    bit<1> circulate;
    bit<1> append_process;
    bit<1> read_process;
    bit<1> route_to_shard;
    bit<1> is_cntrl;
    bit<1> send_acks;
    bit<1> route_to_client;

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
    bit<16> nonce;
    bit<32> seq_no;
}

struct headers {
    pktgen_timer_header_t   timer;
    ethernet_t              ethernet;
    control_pkt_t           cntrl;
    ipv4_t                  ipv4;
    udp_h		    udp; // 64 bits
    start_view_change_t     start_view; // 64 bits
    ring_type_t             ring_type;
    append_entry_t          append;
    read_entry_t            read;
    tail_req_t              tail;
    subscribe_req_t         sub;
    control_pkt_checker_t   cntrl_check;
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
            68      : parse_pktgen_timer; // Adjust port # for your Pipe
            168      : parse_pktgen_timer; // Recirculated generated packet!
            196      : parse_pktgen_timer; // Recirculated generated packet!
            default : parse_ethernet;      // Normal clients
    	}
    }
    
    state parse_pktgen_timer {
	packet.extract(hdr.timer);
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
        packet.extract(hdr.udp);
	transition parse_ring_type;
    }

    state parse_ring_type {
	packet.extract(hdr.ring_type);
        transition select(hdr.ring_type.type) {
            TYPE_CONTROL_CHECK: parse_control_check;
            TYPE_APPEND: parse_append;
            TYPE_APPEND_RESP: parse_append;
            TYPE_READ: parse_read;
            TYPE_READ_RESP: parse_read;
            TYPE_TAIL: parse_tail;
            TYPE_SUB: parse_sub;
	    TYPE_VIEW: parse_view;
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

    state parse_read {
        packet.extract(hdr.read);
        transition accept;
    }

    
    state parse_tail {
        packet.extract(hdr.tail);
        transition accept;
    }

    state parse_sub {
        packet.extract(hdr.sub);
        transition accept;
    }

    state parse_view {
        packet.extract(hdr.start_view);
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
    
    /////// Sequencing state /////////
    /// Local seq no batch counter ///
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
    
    // Largest global sequence number that is known to be a replicated sequence number ///
    Register<bit<32>, bit<1>>(1) highest_replicated_seq_no;
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_replicated_seq_no) write_replicated_seq_no = {
        void apply(inout bit<32> cur_replicated_seq_no) {
	    if (hdr.append.g_idx > cur_replicated_seq_no) {
	         cur_replicated_seq_no = hdr.append.g_idx;
	    }
        }
    };

    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_replicated_seq_no) get_tail = {
        void apply(inout bit<32> curr_replicated_seq_no, out bit<32> old_replicated_seq_no) { // inout = register, out = output 
	    if (hdr.tail.tail_seq_no < curr_replicated_seq_no) {
	       old_replicated_seq_no = curr_replicated_seq_no;
	    } else {
                old_replicated_seq_no = 0;
	    }
        }
    };

    // Largest global sequence number known to the switch Updated every time control packet is received.
    Register<bit<32>, bit<16>>(MAX_GSN_MAP_SIZE, 0) map_from_gsn; // TODO: How to increment/track this sequence number value?
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_gsn) get_gsn = {
        void apply(inout bit<32> gsn_at_idx, out bit<32> gsn) { // inout = register, out = output 
	    gsn = gsn_at_idx;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_gsn) add_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = hdr.cntrl.global_seq_no;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_gsn) zero_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = 0;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_gsn) compare_gsn = {
        void apply(inout bit<32> gsn_slot, out bit<32> higher) { // inout = register, out = output 
	    if (hdr.append.g_idx > gsn_slot) {
	        higher = 0;
	    } else {
	        higher = 1;
	    }
        }
    };

    /////// Acknowledgement Seq //////
    Register<bit<32>, bit<32>>(MAX_INFLIGHT_ACKS, 0) ack_array;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ack_array) check_ack = {
        void apply(inout bit<32> curr_ack_cnt, out bit<32> ack_count) {
	    bit<32> val = curr_ack_cnt;
	    curr_ack_cnt = val + 1;
	    ack_count = 1;
	    /*if (curr_ack_cnt == meta.ack_threshold) {
		curr_ack_cnt = 0;
		ack_count = 1;
	    } else {
		ack_count = 0;
	    }*/
        }
    };

    Register<bit<32>, bit<1>>(1) ack_idx; // TODO: This is a heuristic, 100 is arbitrary
    RegisterAction<bit<32>, bit<1>, bit<32>>(ack_idx) get_ack_idx = { // TODO: HASHING!
        void apply(inout bit<32> curr_ack_idx, out bit<32> ret_ack_idx) {
	    ret_ack_idx = hdr.append.g_idx & (MAX_INFLIGHT_ACKS - 1);
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
    RegisterAction<bit<16>, bit<1>, bit<16>>(cntrl_pkt_it) latest_complete_cntrl_pkt_it = {
        void apply(inout bit<16> cur_cntrl_pkt_it, out bit<16> output_cntrl_pkt_it) {
	    output_cntrl_pkt_it = cur_cntrl_pkt_it - 1;
	}
    };

    //////// Performance Statistics (Packet Gen) /////////
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) tot_packet_counter; 
    Register<bit<32>, bit<1>>(1, 0) latency; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(latency) update_lat_cntr = {
        void apply(inout bit<32> new_lat_cntr) {
	    new_lat_cntr = new_lat_cntr + meta.duration;
	}
    };

    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) recirc_packet_counter; 
    Register<bit<32>, bit<1>>(1, 0) recirc_time; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(recirc_time) update_recirc_cntr = {
        void apply(inout bit<32> new_lat_cntr) {
	    new_lat_cntr = meta.duration;
	}
    };


    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) cntrl_packet_counter; 
    Register<bit<32>, bit<1>>(1, 0) ring_trip_time; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(ring_trip_time) update_ring_trip_cntr = {
        void apply(inout bit<32> new_lat_cntr) {
	    new_lat_cntr = new_lat_cntr + meta.duration;
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

    /**** ROUTING *****/
    
    action switch_id(bit<16> id) {
	meta.switch_id = id;	
    }
   
    @pragma ternary 1 
    table get_switch_id {
	actions = {
	    switch_id;
	}
	size = 64;
	default_action = switch_id(1);
    }

    action shard_size(bit<32> size) {
	meta.shard_size = size;	
    }
   
    @pragma ternary 1 
    table get_shard_size {
	actions = {
	    shard_size;
	}
	size = 64;
	default_action = shard_size(1);
    }

    action num_shards(bit<32> shards) {
	meta.num_shards = 0; //shards - 1;	
    }
   
    @pragma ternary 1 
    table get_num_shards {
	actions = {
	    num_shards;
	}
	size = 64;
	default_action = num_shards(1);
    }

    action ack_threshold(bit<32> threshold) {
	meta.ack_threshold = threshold;	
    }
   
    @pragma ternary 1 
    table get_ack_threshold {
	actions = {
	    ack_threshold;
	}
	size = 64;
	default_action = ack_threshold(1);
    }

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
        default_action = NoAction();
    }

    /* Circulate port */
    action circulate_port(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
    }

    @pragma ternary 1 
    table circulate_table {
        key = {
	    meta.circulate: exact;
	}
	actions = {
            circulate_port;
	    drop;
        }
        size = 64;
        default_action = drop();
    }

    /* Shard routing */
    action shard_port(bit<16> group_id) {
        ig_tm_md.mcast_grp_a = group_id;
    }

    table get_shard_port {
        key = {
	    hdr.ring_type.shard_id: exact;
	}
	actions = {
	    shard_port;
	    NoAction;
	}
	size = 1024;
	default_action = NoAction();
    }
    
    // Send to the next switch
    action route_next_switch(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
	meta.append_process = 0;
	meta.read_process = 0;
    }

    table check_switch_routing {
        key = {
            hdr.ring_type.switch_to_process: exact; // This is being set at the endpoints
        }
        actions = {
	    route_next_switch;
            NoAction;
        }
        size = 1024;
        default_action = NoAction; //route_next_switch(68);
    }

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
    
    /**** RING VIEW *****/
    action update_cntrl_view(bit<16> view) {
        hdr.cntrl.ring_view = view;
    }

    table check_cntrl_view {
        key = {
            hdr.cntrl.ring_view: range;
        }
        actions = {
	    update_cntrl_view;
            NoAction;
        }
        size = 1024;
        default_action = NoAction();
    }

    action ring_view(bit<16> view) {
        meta.ring_view = view;
    }

    table get_ring_view {
        actions = {
	    ring_view;
        }
        size = 64;
        default_action = ring_view(1);
    }


    /**** TAIL *****/
    action forward_tail(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
        hdr.tail.hops = hdr.tail.hops + 1;
    }

    action return_tail() {
        meta.route_to_client = 1;
    }

    table process_tail {
        key = {
            hdr.tail.hops: range;
        }
        actions = {
            forward_tail;
            return_tail;
        }
        size = 1024;
        default_action = return_tail();
    }

    /****** SUBSCRIPTION ********/
    action send_subscription_to_all(bit<16> group_id) {
        ig_tm_md.mcast_grp_a = group_id;
    }

    action send_subscription_to_self(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
    }

    table submit_subscription {
	key = {
	    hdr.sub.subscribe: exact; // This could be much smaller bit-wise TODO
	}
	actions = {
	    send_subscription_to_all;
	    send_subscription_to_self;
	    NoAction;
	}
	size = 1024;
	default_action = NoAction;
    }

    /****** ACKNOWLEDGEMENTS **********/
    action forward_to_subscribers(MirrorId_t mirror_session_id) {
	meta.do_ing_mirroring = 1;
	meta.ing_mir_ses = mirror_session_id;
	meta.pkt_type = PKT_TYPE_MIRROR;
	ig_dprsr_md.mirror_type = MIRROR_TYPE_I2E;
    }
    
    table route_subscriber_acks {
	key = {
            hdr.append.stream_id: exact;
	}
	actions = {
	    forward_to_subscribers;
	    NoAction;
	}
	size = 1024;
	default_action = NoAction;
    }


    /**** DONE WITH MATCH ACTION TABLES *****/


    /* Start of Ingress Pipeline */
    apply {
	meta.circulate = 0;
	meta.is_cntrl = 0;
	meta.route_to_shard = 0;
	meta.send_acks = 0;
	meta.route_to_client = 0;
	meta.get_pkt_gen = 0;
	meta.read_process = 1;
	meta.append_process = 1;
        meta.pkt_type = PKT_TYPE_NORMAL;
	meta.ack_threshold = 1; //get_ack_threshold.apply();
	meta.num_shards = 1; //get_num_shards.apply();
	meta.shard_size = 1; //get_shard_size.apply();

	/*if (hdr.ring_type.isValid() && !hdr.timer.isValid()) {
	    check_switch_routing.apply();
	}*/

	bit<32> local_seq_no_reg = 0;
	if (hdr.cntrl.isValid()) {
		local_seq_no_reg = read_local_seq_no.execute(0);
	} else if (hdr.append.isValid() && hdr.append.g_idx == 0) {
		local_seq_no_reg = write_local_seq_no.execute(0); 
	}
	if (hdr.cntrl.isValid()) {
            // Step 0: Check if the control packet's view is outdated TODO
	    //check_cntrl_view.apply();
	    if (local_seq_no_reg != 0) {
                bit<16> cntrl_pkt_it_reg = update_cntrl_pkt_it.execute(0);
	        add_gsn.execute(cntrl_pkt_it_reg);
                hdr.cntrl.global_seq_no = hdr.cntrl.global_seq_no + local_seq_no_reg;
	        cntrl_packet_counter.count(0);
	    } else {
                bit<16> cntrl_pkt_it_reg = get_cntrl_pkt_it.execute(0);
	        add_gsn.execute(cntrl_pkt_it_reg);
	    }

	    meta.start_ts = hdr.cntrl.start_ts;
	    meta.end_ts = standard_metadata.ingress_mac_tstamp;
	    meta.duration = (bit<32>)(meta.end_ts - meta.start_ts);

	    update_ring_trip_cntr.execute(0);
	    hdr.cntrl.start_ts = standard_metadata.ingress_mac_tstamp;

	    meta.is_cntrl = 1;
        } else if (hdr.ring_type.type == TYPE_APPEND && meta.append_process == 1) {
	    bit<32> assign_sn = 0;
            bit<16> cntrl_pkt_it_reg = get_cntrl_pkt_it.execute(0);
	    if (hdr.append.g_idx == 0) {
		hdr.append.cntrl_pkt_it = cntrl_pkt_it_reg;
		hdr.append.start_ts = standard_metadata.ingress_mac_tstamp;
		assign_sn = local_seq_no_reg; //write_local_seq_no.execute(0); // Get batchOffset
	        hdr.append.g_idx = assign_sn;	    
		zero_gsn.execute(cntrl_pkt_it_reg); // Get Global sequence number

	    } else {
		assign_sn = get_gsn.execute(hdr.append.cntrl_pkt_it); // Get Global sequence number
	    	hdr.append.g_idx = hdr.append.g_idx + assign_sn;	    
	    }

	    // Routing determination
	    if (hdr.append.cntrl_pkt_it == cntrl_pkt_it_reg) {
		meta.circulate = 1;
	    } else if (hdr.timer.isValid()) {
		meta.start_ts = hdr.append.start_ts;
	        meta.end_ts = standard_metadata.ingress_mac_tstamp;
	        meta.duration = (bit<32>)(meta.end_ts - meta.start_ts);

		meta.circulate = 1;
		hdr.ring_type.type = TYPE_APPEND_RESP;

	        recirc_packet_counter.count(0);
	        update_recirc_cntr.execute(0);



	    } else {
		meta.route_to_shard = 1;
		if (hdr.append.stream_id == 0) {
		    hdr.ring_type.shard_id = hdr.append.g_idx;
		} else {
		    hdr.ring_type.shard_id = hdr.append.stream_id;
		}
	    }
        } else if ((hdr.ring_type.type == TYPE_APPEND_RESP && hdr.append.isValid()) || (hdr.ring_type.type == TYPE_READ && meta.read_process == 1)) {
            bit<32> ready_for_ack = 1;
	    if (hdr.ring_type.type == TYPE_APPEND_RESP) {
                bit<16> cntrl_pkt_it_reg = get_cntrl_pkt_it.execute(0);
		ready_for_ack = compare_gsn.execute(cntrl_pkt_it_reg);
	        //tot_packet_counter.count(0);
	    }
	    if (ready_for_ack == 1) {


		bit<32> slot = get_ack_idx.execute(0);
		bit<32> ack_ready = check_ack.execute(slot);
		if (ack_ready == 1) {
		    if (hdr.timer.isValid()) {

	                meta.start_ts = hdr.append.start_ts;

	                meta.end_ts = standard_metadata.ingress_mac_tstamp;
			meta.duration = (bit<32>)(meta.end_ts - meta.start_ts);


	        	tot_packet_counter.count(0);
	        	update_lat_cntr.execute(0);
		        meta.route_to_client = 1;
		    } else if (hdr.append.isValid() && !hdr.timer.isValid()) {
		        meta.send_acks = 1;
		        meta.route_to_client = 1;
		        write_replicated_seq_no.execute(0);
		    } else if (hdr.read.isValid()) {
		        meta.route_to_shard = 1;
			hdr.ring_type.shard_id = hdr.read.g_idx;
		    }
		} else {
		    drop();
		}
	    } else {
		meta.circulate = 1; // TODO: READ???
	    }
	} else if (hdr.ring_type.type == TYPE_READ_RESP) {
	    meta.route_to_client = 1; // NOTE: Currently not waiting for read quorum!
	} else if (hdr.tail.isValid()) {
	    bit<32> tail_seq = hdr.tail.tail_seq_no;
	    hdr.tail.tail_seq_no = get_tail.execute(0);
	    if (hdr.tail.tail_seq_no == 0) {
		hdr.tail.tail_seq_no = tail_seq;
	    }
	    process_tail.apply();
	} else if (hdr.start_view.isValid()) {
            bit<16> cntrl_pkt_it_reg = latest_complete_cntrl_pkt_it.execute(0);
	    hdr.start_view.highest_seen_seq_no = get_gsn.execute(cntrl_pkt_it_reg); 
	    get_switch_id.apply();
	    hdr.start_view.switch_id = meta.switch_id;
	    get_ring_view.apply();
	    hdr.start_view.old_ring_view = meta.ring_view;
	}


	/********* ROUTING LOGIC ***********/
	/*if (hdr.timer.isValid() && meta.get_pkt_gen == 1) { // All generated packet are appends
	    meta.start_ts = hdr.append.start_ts;
	    meta.end_ts = standard_metadata.ingress_mac_tstamp;
	    meta.duration = (bit<32>)(meta.end_ts - meta.start_ts);
	    meta.nonce = hdr.append.cntrl_pkt_it; //g_idx;
	    meta.seq_no = hdr.append.g_idx;
	    ig_dprsr_md.digest_type = 1;
	}*/

	if (meta.route_to_client == 1) {
            ipv4_lpm.apply();
        }
	
	if (meta.is_cntrl == 1) {
            cntrl_id_to_ip.apply();
	} else if (meta.circulate == 1) {
            circulate_table.apply();
	} else if (meta.route_to_shard == 1) {
	    //meta.ack_threshold = hdr.ring_type.shard_id & meta.num_shards;
	    get_shard_port.apply();
	} else if (hdr.sub.isValid()) {  /// Subscribe header
	    if (hdr.sub.subscribe == 0) {
		hdr.sub.subscribe = 1;
	    } else if (hdr.sub.subscribe == 1) {
		hdr.sub.subscribe = 0;
	    }
	    submit_subscription.apply();
	} else if (meta.send_acks == 1) {
	    route_subscriber_acks.apply();
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
            tofino_parser.apply(packet, eg_intr_md);
	    mirror_h mirror_md = packet.lookahead<mirror_h>();
	    transition select(mirror_md.pkt_type) {
		PKT_TYPE_MIRROR: parse_mirror;
		default: parse_maybe_pktgen;
	    }
	}

	state parse_mirror {
	    mirror_h mirror_md;
	    packet.extract(mirror_md);
	    transition parse_maybe_pktgen;
	}

   	state parse_maybe_pktgen {
	    pktgen_timer_header_t pkt_timer = packet.lookahead<pktgen_timer_header_t>();
	    transition select(pkt_timer.app_id) {
		1: parse_pktgen_timer;
		default: parse_ethernet;
	    }
	}

        state parse_pktgen_timer {
            packet.extract(hdr.timer);
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
                TYPE_APPEND_RESP: parse_append;
                TYPE_READ: parse_read;
                TYPE_READ_RESP: parse_read;
                TYPE_TAIL: parse_tail;
                TYPE_SUB: parse_sub;
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
        
	state parse_read {
            packet.extract(hdr.read);
            transition accept;
        }

    
        state parse_tail {
            packet.extract(hdr.tail);
            transition accept;
        }

	state parse_sub {
            packet.extract(hdr.sub);
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
        packet.emit(hdr.cntrl);
        packet.emit(hdr.ipv4);
        packet.emit(hdr.udp);
        packet.emit(hdr.ring_type);
        packet.emit(hdr.append);
        packet.emit(hdr.read);
        packet.emit(hdr.tail);
        packet.emit(hdr.sub);
        packet.emit(hdr.cntrl_check);
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
