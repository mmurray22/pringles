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

#include "../../common/headers.p4"
#include "../../common/util.p4"


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
const bit<16> TYPE_VIEW_CHANGE = 0x0900;
const bit<16> TYPE_VIEW_ACK = 0x0910;

const bit<32> MAX_INFLIGHT_ACKS = 65536;
const bit<32> MAX_GSN_MAP_SIZE = 65535;
const bit<8> CANARY_VALUE = 165; //0xA5;


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

   // ID of switch to send to
   bit<8> pkt_id;
}

// Ring type header - holds the type of the packet
// Size: 96 bits
header ring_type_t {
    // Type to filter packets
    bit<16> type;

    // Counting the number of entries batched in the packet
    bit<16> num_entries;

    // Shard ID - to be filled in by switch
    bit<16> shard_id;

    // Switch ID - to be filled in by switch
    bit<32> switch_to_process;

    bit<48> start_ts;
    bit<48> end_ts;

    bit<16> raw_elapsed_time;

    bit<32> pipe_ts;
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
    bit<16> cntrl_pkt_it;

    bit<32> client_ip;
    bit<16> recv_port;
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
}

// Header for requesting the current tail
// Size: 128
header tail_req_t {
    /** Filled in by client **/

    // Unique nonce used to detect duplicates of the message
    bit<16> nonce;
    
       
    /*Altered by switches*/

    // Keeps track of how many nodes in the ring the tail request has been passed to
    bit<8> hops;

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
    bit<8> switch_id;
    bit<8> sender_switch_id;
    bit<32> highest_seen_seq_no;
}

header finalize_view_change_t {
    bit<16> dst_id;
    bit<16> current_ring_view;
    bit<8> next_switch_id;
    bit<32> highest_quorum_seq_no;
}

header canary_t {
    bit<8> magic;
}

struct metadata {
    bit<2> recirc_port_type;
    bit<1> route_to_shard;

    bit<1> ready_for_ack;
    bit<1> ack_ready;
    bit<1> cntrl_to_process;
    bit<1> route_to_client;

    bit<8> ring_process; // MAKE 8 bits
    bit<8> read_process;
    bit<8> subscribe;
    bit<8> canary_ok; 
    bit<8> num_recirc_ports;
    bit<8> num_wait_ports;
    bit<8> port_idx;
    bit<8> hops;
    bit<8> switch_id;
    bit<8> view_change_active;
    bit<8> view_change_prev;
    bit<8> next_hop;
    
    bit<16> cntrl_pkt_it;
    bit<16> wait_time;
    bit<16> nonce;
    bit<16> ring_view;
    bit<16> num_shards;

    bit<32> stream_id;
    bit<32> cnt;
    bit<32> assign_sn;
    bit<32> local_seq_no;
    bit<32> global_seq_no;
    bit<32> view_change_start_time;
    bit<32> batch_size; // For keeping tabs on the GSN table size
    bit<32> queue_congest;
    bit<32> shard_size;
    bit<32> duration;
    int<32> overflow;
    bit<32> output;
    bit<32> ack_threshold;

    // For mirroring
    bit<1> do_ing_mirroring; // Enable ingress mirroring
    bit<1> do_egr_mirroring; // Enable egress mirroring
    MirrorId_t ing_mir_ses; // Ingress mirror session ID
    MirrorId_t egr_mir_ses; // Egress mirror session ID
    pkt_type_t pkt_type;

    bit<48> start_ts;
    bit<48> end_ts;
    bit<32> egress_end_time;
    bit<32> elapse_time;
}

struct headers {
    pktgen_timer_header_t   timer;
    ethernet_t              ethernet;
    control_pkt_t           cntrl;
    ipv4_t                  ipv4;
    udp_h		    udp; // 64 bits
    start_view_change_t     start_view;
    finalize_view_change_t  finalize_view;
    ring_type_t             ring_type;
    append_entry_t          append;
    read_entry_t            read;
    tail_req_t              tail;
    subscribe_req_t         sub;
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
	meta.read_process = 0;
	meta.ring_process = 0;
	meta.cntrl_to_process = 0;
	meta.subscribe = 0;
	meta.ready_for_ack = 1; // DEFAULT
	meta.cnt = 0;
	meta.hops = 0;
	meta.ack_ready = 0;
	meta.assign_sn = 0;

   	meta.route_to_shard = 0;
   	meta.route_to_client = 0;
	meta.cntrl_pkt_it = 0;
	meta.local_seq_no = 0;

   	meta.canary_ok = 0;
   	meta.num_recirc_ports = 0;
   	meta.num_wait_ports = 0;
   	meta.port_idx = 0;
   	meta.stream_id = 0;
	meta.next_hop = 0;

   	meta.switch_id = 0;
   	meta.wait_time = 0;
   	meta.nonce = 0;
   	meta.ring_view = 0;
   	meta.num_shards = 0;

   	meta.batch_size = 0;
   	meta.queue_congest = 0;
   	meta.shard_size = 0;
   	meta.duration = 0;
   	meta.overflow = 0;
   	meta.output = 0;
   	meta.ack_threshold = 0;

        meta.do_ing_mirroring = 0;
        meta.do_egr_mirroring = 0;
        meta.ing_mir_ses = 0;
        meta.egr_mir_ses = 0;
        meta.pkt_type = PKT_TYPE_NORMAL;

    	meta.start_ts = 0;
    	meta.end_ts = 0;
    	meta.elapse_time = 0;

	transition select(standard_metadata.ingress_port) {
            196      : parse_pktgen_timer; // Generated packet!
            197      : parse_pktgen_timer; // Generated packet!
            198      : parse_pktgen_timer; // Generated packet!
            199      : parse_pktgen_timer; // Generated packet!
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
            TYPE_VIEW_CHANGE: parse_start_view;
            TYPE_VIEW_ACK: parse_finalize_view;
            TYPE_IPV4: parse_ipv4;
	    default: accept;
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
            TYPE_APPEND_WAIT: parse_append;
            TYPE_APPEND_RESP: parse_append;
            TYPE_READ: parse_read;
            TYPE_READ_RESP: parse_read;
            TYPE_TAIL: parse_tail;
            TYPE_SUB: parse_sub;
	    default: parse_canary;
        }
    }

    state parse_control {
        packet.extract(hdr.cntrl);
        transition parse_canary;
    }
    
    state parse_append {
        packet.extract(hdr.append);
        transition parse_canary;
    }

    state parse_read {
        packet.extract(hdr.read);
        transition parse_canary;
    }

    state parse_tail {
        packet.extract(hdr.tail);
        transition parse_canary;
    }

    state parse_sub {
        packet.extract(hdr.sub);
        transition parse_canary;
    }

    state parse_start_view {
        packet.extract(hdr.start_view);
        transition parse_canary;
    } 

    state parse_finalize_view {
        packet.extract(hdr.finalize_view);
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
control MyIngress1(inout headers hdr,
                  inout metadata meta,
		  in ingress_intrinsic_metadata_t standard_metadata,
		  in ingress_intrinsic_metadata_from_parser_t ig_prsr_md,
		  inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
		  inout ingress_intrinsic_metadata_for_tm_t ig_tm_md) {
 

    /********* Sequencing state *********/
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
                old_replicated_seq_no = hdr.tail.tail_seq_no;
	    }
        }
    };

    // Largest global sequence number known to the switch Updated every time control packet is received.
    Register<bit<32>, bit<16>>(MAX_GSN_MAP_SIZE, 0) map_from_gsn;
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_gsn) get_gsn = {
        void apply(inout bit<32> gsn_at_idx, out bit<32> gsn) { // inout = register, out = output 
	    gsn = gsn_at_idx;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_gsn) add_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = meta.global_seq_no;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_gsn) zero_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = 0;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_gsn) compare_gsn = {
        void apply(inout bit<32> gsn_slot, out bit<32> higher) { // inout = register, out = output 
	    if (hdr.append.g_idx > gsn_slot || hdr.append.g_idx == gsn_slot) {
	        higher = 1;
	    } else {
	        higher = 0;
	    }
        }
    };
  
    /**** SETTING UP VARIABLES ****/
    action switch_id(bit<8> id) {
	meta.switch_id = id;	
    }
    @pragma ternary 1 
    table get_switch_id {
	actions = {
	    switch_id;
	}
	size = 1;
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
	size = 1;
	default_action = shard_size(1);
    }

    action num_shards(bit<16> shards) {
	meta.num_shards = 0; //shards - 1;	
    }
    @pragma ternary 1 
    table get_num_shards {
	actions = {
	    num_shards;
	}
	size = 1;
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
	size = 1;
	default_action = ack_threshold(1);
    }

    // Get number of available recirculation ports for sequencing
    action get_num_recirc(bit<8> num_recirc_ports) {
        meta.num_recirc_ports = num_recirc_ports;
    }
    @pragma ternary 1 
    table num_recirc_port_table {
	actions = {
            get_num_recirc;
        }
        size = 1;
        default_action = get_num_recirc(1);
    }
    action get_num_wait(bit<8> num_wait_ports) {
        meta.num_wait_ports = num_wait_ports;
    }
    @pragma ternary 1 
    table num_wait_port_table {
	actions = {
            get_num_wait;
        }
        size = 1;
        default_action = get_num_wait(1);
    }


    /**** ROUTING *****/
    // Managing data recirculation
    Register<bit<8>, bit<1>>(2, 0) recirc_port_idx; // SeqRecirc @ index 0, StorageRecirc @ index 1
    RegisterAction<bit<8>, bit<1>, bit<8>>(recirc_port_idx) get_seqrecirc_idx = {
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
    RegisterAction<bit<8>, bit<1>, bit<8>>(recirc_port_idx) get_storagerecirc_idx = {
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

    action drop() {
        ig_dprsr_md.drop_ctl = 1;
    }

    // Send to the next switch
    action route_next_switch(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
	meta.ring_process = 0;
    }
    action set_ring_process(egressSpec_t port, bit<8> process_type) {
	meta.ring_process = 1;
	hdr.ring_type.pipe_ts = ig_prsr_md.global_tstamp[31:0];
    }
    table check_switch_routing {
        key = {
            hdr.ring_type.switch_to_process: exact; // This is being set at the endpoints
            hdr.ring_type.type: exact; // This is being set at the endpoints
        }
        actions = {
	    route_next_switch;
	    set_ring_process;
            NoAction;
        }
        size = 1024;
        default_action = NoAction; //route_next_switch(68);
    }

    /**** CONTROL PACKET *****/

    // Number of total times the switch has seen the control packet
    // NOTE: It is anticipated that this value will overflow
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

    
    Register<bit<8>, bit<1>>(1, 1) next_ring_hop_id;
    RegisterAction<bit<8>, bit<1>, bit<8>>(next_ring_hop_id) update_id = {
        void apply(inout bit<8> ring_hop_id) {
	    ring_hop_id = hdr.finalize_view.next_switch_id;
	}
    };
    RegisterAction<bit<8>, bit<1>, bit<8>>(next_ring_hop_id) get_id = {
        void apply(inout bit<8> ring_hop_id, out bit<8> output) {
	    output = ring_hop_id;
	}
    };

   
    /****** View and View Change Tables + Registers *******/ 
    Register<bit<16>, bit<1>>(1, 1) current_view;
    RegisterAction<bit<16>, bit<1>, bit<16>>(current_view) update_view = {
        void apply(inout bit<16> curr_view) {
	    curr_view = hdr.finalize_view.current_ring_view;
	}
    };
    RegisterAction<bit<16>, bit<1>, bit<16>>(current_view) get_view = {
        void apply(inout bit<16> curr_view, out bit<16> output) {
	    output = curr_view;
	}
    };

    action update_cntrl_view(bit<16> view) {
        hdr.cntrl.ring_view = view;
	meta.cntrl_to_process = 1;
    }
    table check_cntrl_view {
        key = {
            hdr.cntrl.ring_view: exact;
        }
        actions = {
	    update_cntrl_view;
            NoAction;
        }
        size = 1024;
        default_action = NoAction();
    }

    /**** TAIL *****/
    
    /****** SUBSCRIPTION ********/
    
    /****** ACKNOWLEDGEMENTS **********/
    Register<bit<32>, bit<32>>(MAX_INFLIGHT_ACKS, 0) ack_array;
    RegisterAction<bit<32>, bit<32>, bit<1>>(ack_array) check_ack = {
        void apply(inout bit<32> curr_ack_cnt, out bit<1> ack_count) {
	    if (curr_ack_cnt == meta.ack_threshold) {
		curr_ack_cnt = 0;
		ack_count = 1;
	    } else {
		ack_count = 0;
	    	curr_ack_cnt = curr_ack_cnt + 1;
	    }
        }
    };

    Register<bit<32>, bit<1>>(1) ack_idx;
    RegisterAction<bit<32>, bit<1>, bit<32>>(ack_idx) get_ack_idx = {
        void apply(inout bit<32> curr_ack_idx, out bit<32> ret_ack_idx) {
	    ret_ack_idx = hdr.append.g_idx & (MAX_INFLIGHT_ACKS - 1);
        }
    };
    

   /******* Telemetry + Performance Statistics (Packet Gen) ********/

    // Track number of sequencing batches that are currently in flight - being replicated, waiting for full sequencing //
    Register<bit<16>, bit<1>>(1, 0) active_batches;
    RegisterAction<bit<16>, bit<1>, bit<16>>(active_batches) add_batch = {
        void apply(inout bit<16> num_batches) { // inout = register, out = output 
	    num_batches = num_batches + 1;
        }
    };
    RegisterAction<bit<16>, bit<1>, bit<16>>(active_batches) sub_batch = {
        void apply(inout bit<16> num_batches) { // inout = register, out = output 
	    num_batches = num_batches - 1;
        }
    };

    // Track number of in flight messages per batch. Can track maximum of MAX_GSN_MAP_SIZE 
    Register<bit<32>, bit<16>>(MAX_GSN_MAP_SIZE, 0) map_from_batch_sz;
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_batch_sz) update_batch_sz = {
        void apply(inout bit<32> sz_at_idx, out bit<32> batch_sz) { // inout = register, out = output 
	    sz_at_idx = sz_at_idx - 1;
	    batch_sz = sz_at_idx;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_batch_sz) set_batch_sz = {
        void apply(inout bit<32> batch_slot) { // inout = register, out = output 
	    batch_slot = meta.batch_size;
        }
    };

    // Track e2e latency
    Register<int<32>, bit<1>>(1, 0) latency_lower; 
    MathUnit<bit<32>>(MathOp_t.MUL, 2) mult;
    RegisterAction<int<32>, bit<1>, int<32>>(latency_lower) update_low_lat_cntr = {
        void apply(inout int<32> new_lat_cntr, out int<32> overflow) {
	    overflow = new_lat_cntr;
	    if (new_lat_cntr < 0) {
	        new_lat_cntr = (int<32>)(mult.execute(meta.duration));
	    } else {
	        new_lat_cntr = new_lat_cntr + (int<32>)meta.duration;
	    }

	}
    };
    Register<bit<32>, bit<1>>(1, 0) latency_higher; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(latency_higher) update_high_lat_cntr = {
        void apply(inout bit<32> new_lat_cntr) {
	    if (meta.overflow < 0) {
	        new_lat_cntr = new_lat_cntr + 1;
	    }
	}
    };

    // Tracks how long packet was waiting to be allowed to advance to acknowledgements (part of waiting)
    Register<bit<16>, bit<1>>(1, 0) raw_elapsed_time; 
    RegisterAction<bit<16>, bit<1>, bit<16>>(raw_elapsed_time) update_raw_elapsed_time = {
        void apply(inout bit<16> raw_cntr) {
	    raw_cntr = hdr.ring_type.raw_elapsed_time;
	}
    };
    // Total packet counter 
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) seq_only_packet_counter; 
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) tot_packet_counter; 
    Register<bit<32>, bit<1>>(1, 1) total_pkt_cnt_lower; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(total_pkt_cnt_lower) update_low_pkt_cntr = {
        void apply(inout bit<32> new_pkt_cntr, out bit<32> overflow) {
	    overflow = new_pkt_cntr;
	    new_pkt_cntr = new_pkt_cntr + 1;
	}
    };

    Register<bit<32>, bit<1>>(1, 0) total_pkt_cnt_higher; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(total_pkt_cnt_higher) update_high_pkt_cntr = {
        void apply(inout bit<32> new_pkt_cntr) {
	    if (meta.output == 0) {
	        new_pkt_cntr = new_pkt_cntr + 1;
	    }
	}
    };

    // Recirc packet counter
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) recirc_packet_counter; 
    Register<bit<32>, bit<1>>(1, 1) recirc_pkt_cnt_lower; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(recirc_pkt_cnt_lower) update_low_recirc_cntr = {
        void apply(inout bit<32> new_pkt_cntr, out bit<32> overflow) {
	    overflow = new_pkt_cntr;
	    new_pkt_cntr = new_pkt_cntr + 1;
	}
    };

    Register<bit<32>, bit<1>>(1, 0) recirc_pkt_cnt_higher; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(recirc_pkt_cnt_higher) update_high_recirc_cntr = {
        void apply(inout bit<32> new_pkt_cntr) {
	    if (meta.output == 0) {
	        new_pkt_cntr = new_pkt_cntr + 1;
	    }
	}
    };


    /******** CANARY **********/
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) canary_no_match_counter_ig1; 
    Register<bit<8>, bit<1>>(1, 1) true_magic_val;
    RegisterAction<bit<8>, bit<1>, bit<8>>(true_magic_val) write_magic = {
        void apply(inout bit<8> magic_val) {
            magic_val = hdr.canary.magic;
        }
    };
    Register<bit<16>, bit<1>>(1, 0) pesky_ether_type;
    RegisterAction<bit<16>, bit<1>, bit<16>>(pesky_ether_type) read_ether_type = {
        void apply(inout bit<16> etherType) {
	    if (hdr.ethernet.isValid()) {
                etherType = hdr.ethernet.etherType;
	    } else {
	    	etherType = 0;
	    }
        }
    };

    action read_local() {
	meta.local_seq_no = read_local_seq_no.execute(0);
    }
    action write_local() {
        meta.local_seq_no = write_local_seq_no.execute(0); 
    }
    table cntrl_local_seq {
        key = {
            meta.ring_process: exact;
 	    meta.cntrl_to_process: exact;
        }
        actions = {
            read_local;
            write_local;
            NoAction;
        }
        size = 64;
        default_action = NoAction;
    }

    action get_it() {
        meta.cntrl_pkt_it = get_cntrl_pkt_it.execute(0);
    }
    action update_it() {
	meta.cntrl_pkt_it = update_cntrl_pkt_it.execute(0);
    }
    action get_complete_it() {
        meta.cntrl_pkt_it = latest_complete_cntrl_pkt_it.execute(0);
    }
    action get_append_it() {
        meta.cntrl_pkt_it = hdr.append.cntrl_pkt_it;
    }
    table fill_cntrl_pkt_it {
        key = {
            meta.cntrl_to_process: exact;
            meta.local_seq_no: exact;
	    meta.ring_process: exact; // ONLY get_it
        }
        actions = {
            get_it;
	    get_complete_it; // all values in key should be 0, this is for start_view
            update_it;
            get_append_it;
            NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }

    action update_local_seq() {
        hdr.cntrl.global_seq_no = hdr.cntrl.global_seq_no + meta.local_seq_no;
	meta.batch_size = meta.local_seq_no;
	add_batch.execute(0);
    }
    action update_batch() {
	sub_batch.execute(0);
    }
    table process_lsn {
        key = {
            meta.local_seq_no: exact;
            meta.cntrl_to_process: exact;
	    meta.ring_process: exact;
        }
        actions = {
            update_local_seq;
	    update_batch;
            NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }


    action set_batch_size() {
	set_batch_sz.execute(meta.cntrl_pkt_it);
    }
    action update_batch_size() {
        meta.cnt = update_batch_sz.execute(meta.cntrl_pkt_it);
    }
    table edit_batch_size{
        key = {
            meta.local_seq_no: exact;
            meta.cntrl_to_process: exact;
            meta.ring_process: exact;
        }
        actions = {
            set_batch_size;
            update_batch_size;
            NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }

    action get_local_idx() {
	hdr.append.cntrl_pkt_it = meta.cntrl_pkt_it;
	hdr.append.g_idx = meta.local_seq_no;	    
	meta.recirc_port_type = 1;
    }
    action wait_append() {
	hdr.ring_type.end_ts = standard_metadata.ingress_mac_tstamp;
	meta.recirc_port_type = 2;
    }
    action get_read_resp() {
        meta.route_to_client = 1; // NOTE: Currently not waiting for read quorum!
    }
    table append_first_tbl {
        key = {
            meta.ring_process: exact;
	    hdr.ring_type.type: exact;
	    hdr.append.g_idx: exact;
        }
        actions = {
            get_local_idx;
	    wait_append;

	    get_read_resp; // TYPE_READ_RESP
            NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }


    action update_gsn() {
	add_gsn.execute(meta.cntrl_pkt_it); // TODO: What is the order? Check this!
    }
    action get_ack_readiness() {
	meta.ready_for_ack = (bit<1>)compare_gsn.execute(meta.cntrl_pkt_it);
    }
    action make_gsn_zero() {
	zero_gsn.execute(meta.cntrl_pkt_it); // Get Global sequence number
    }
    action assign_gsn() {
        meta.assign_sn = get_gsn.execute(meta.cntrl_pkt_it); // Get Global sequence number
    }
    table append_first_tbl_gsn {
        key = {
            meta.ring_process: exact;
	    hdr.ring_type.type: exact;
	    hdr.append.g_idx: exact; // TODO
	    meta.local_seq_no: exact;
            meta.cntrl_to_process: exact;
        }
        actions = {
	    get_ack_readiness;
            make_gsn_zero;
	    assign_gsn;
            NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }


    action exp_type_one() {
	meta.recirc_port_type = 1;
	seq_only_packet_counter.count(0); // TODO
	drop();
    }
    action exp_type_two() {
	hdr.ring_type.start_ts = standard_metadata.ingress_mac_tstamp;
	hdr.ring_type.end_ts = standard_metadata.ingress_mac_tstamp;
	hdr.ring_type.type = TYPE_APPEND_WAIT;
	meta.recirc_port_type = 2;
    }
    action regular_stream() {
	meta.route_to_shard = 1;
	hdr.ring_type.shard_id = (bit<16>)hdr.append.stream_id;
    }
    action regular_idx() {
	meta.route_to_shard = 1;
	hdr.ring_type.shard_id = (bit<16>)hdr.append.g_idx;
    }
    table append_second_tbl {
        key = {
            meta.ring_process: exact;
	    hdr.ring_type.type: exact;
	    hdr.append.exp_type: exact;
        }
        actions = {
            exp_type_one;
	    exp_type_two;
            regular_stream;
            regular_idx;
	    NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }
    

    action emulate_storage_ports() {
	meta.port_idx = get_seqrecirc_idx.execute(1);
    }
    action await_sequencing_ports() {
	meta.port_idx = get_seqrecirc_idx.execute(0);
    }
    table get_port_idx {
	key = {
	   meta.recirc_port_type: exact; 
	}
	actions = {
	    emulate_storage_ports;
	    await_sequencing_ports;
	    NoAction;
	}	
    }


    action subtract_batch() {
	sub_batch.execute(0);
    }
    table adjust_batch_cnt {
	key = {
	    meta.cnt: exact;
	}
	actions = {
	    subtract_batch;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }


    action ack_append() {
	tot_packet_counter.count(0);
	//meta.output = update_low_pkt_cntr.execute(0);
	//update_high_pkt_cntr.execute(0);
        //update_raw_elapsed_time.execute(0); TODO: Make separate table
	meta.route_to_client = 1;
    }
    action ack_read() {
	meta.route_to_shard = 1;
	hdr.ring_type.shard_id = (bit<16>)hdr.append.g_idx;
    }
    table ack_action_tbl{
        key = {
            meta.ring_process: exact;
	    hdr.ring_type.type: exact;
	    meta.ack_ready: exact;
        }
        actions = {
	    ack_append;
	    ack_read;
	    NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }

    action write_rep_seq_no() {
	write_replicated_seq_no.execute(0);
    }
    action read_rep_seq_no() {
        get_tail.execute(0);
    }
    table update_replicated_seq_no {
	key = {
	    meta.ring_process: exact;
	    meta.ack_ready: exact;
	}
	actions = {
	    write_rep_seq_no;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }

    action low_latency() {
        meta.overflow = update_low_lat_cntr.execute(0);
    }
    table ack_get_lower_lat {
	key = {
            meta.ring_process: exact;
            meta.read_process: exact;
	    hdr.ring_type.type: exact;
	    meta.ack_ready: exact;
        }
        actions = {
	    low_latency;
	    NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }
    action upper_latency() {
	update_high_lat_cntr.execute(0);
    }
    table ack_get_upper_lat {
	key = {
            meta.ring_process: exact;
            meta.read_process: exact;
	    hdr.ring_type.type: exact;
	    meta.ack_ready: exact;
        }
        actions = {
	    upper_latency;
	    NoAction;
        }
        size = 1024;
        default_action = NoAction;

    }

    action forward_to_subscribers(MirrorId_t mirror_session_id, bit<16> group_id) {
	meta.do_ing_mirroring = 1;
	meta.ing_mir_ses = mirror_session_id;
	meta.pkt_type = PKT_TYPE_MIRROR;
	ig_dprsr_md.mirror_type = MIRROR_TYPE_I2E;
        ig_tm_md.mcast_grp_a = group_id;
    } 
    action receive_subscription(egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
    }
    action send_multicast(bit<16> group_id) { // This does: shard multicast, subscription multicast
        ig_tm_md.mcast_grp_a = group_id;
    }
    action eth_forward(macAddr_t dstAddr, egressSpec_t port) {
        ig_tm_md.ucast_egress_port = port;
        hdr.ethernet.srcAddr = hdr.ethernet.dstAddr;
        hdr.ethernet.dstAddr = dstAddr;
        hdr.ipv4.ttl = hdr.ipv4.ttl - 1;
    }
    action send_to(egressSpec_t port) { // This does: circulate, wait, cntrl, and subscribe to self
        ig_tm_md.ucast_egress_port = port;
    }
    table routing {
        key = {
	    meta.next_hop: exact; // encodes pkt_id
	    meta.route_to_client: exact;
            hdr.ethernet.dstAddr: lpm;
	    meta.recirc_port_type: exact;
	    meta.port_idx: exact;
	    meta.subscribe: exact;
	    meta.route_to_shard: exact; // encodes shard_id
	    meta.hops: exact; // send_to if more hops to go, eth_forward to send back to client
	}
	actions = {
	   send_to;
	   eth_forward; 
	   send_multicast;
	   forward_to_subscribers; 
	   receive_subscription;
	   drop;
	}
	size = 1024;
	default_action = drop();
    }

    action update_next_hop(macAddr_t dstAddr, egressSpec_t port) {
	update_id.execute(0); // Set control packet next member
    }
    action get_next_hop(egressSpec_t port) { // This does: circulate, wait, cntrl, and subscribe to self
	meta.next_hop = get_id.execute(0);
    }
    table next_hop_routing {
        key = {
	    meta.ring_process: exact; // encodes pkt_id
	    meta.cntrl_to_process: exact; // encodes pkt_id
	}
	actions = {
	   get_next_hop;
	   update_next_hop; 
	   drop;
	}
	size = 64;
	default_action = drop();
    }

    /* Start of Ingress Pipeline */
    apply {
	get_ack_threshold.apply();
	get_num_shards.apply();
	get_shard_size.apply();
	get_switch_id.apply();
	num_recirc_port_table.apply();
	num_wait_port_table.apply();
	meta.ring_view = get_view.execute(0);

	hdr.timer.setInvalid();

	// Determine if this packet needs to be routed to another switch
	if (hdr.ring_type.isValid()) { // TODO TIMER HEADER??
	    check_switch_routing.apply();
	} 

	if (hdr.cntrl.isValid()) {
	    if (meta.ring_view != hdr.cntrl.ring_view) {
		meta.cntrl_to_process = 0;
		drop();
	    } else {
		meta.cntrl_to_process = 1;
	    }
	}

	cntrl_local_seq.apply();
	fill_cntrl_pkt_it.apply();
	next_hop_routing.apply();

	append_first_tbl.apply(); // handles g_idx == 0 and TYPE_WAIT
	if (hdr.ring_type.type == TYPE_APPEND && hdr.append.cntrl_pkt_it != meta.cntrl_pkt_it) {
	    meta.ring_process = 2; // TODO: Arbitrary???
	}

	edit_batch_size.apply();
	process_lsn.apply();
	append_first_tbl_gsn.apply(); // handles TYPE_APPEND_RESP, handles GSN updates	

	if (hdr.ring_type.type == TYPE_APPEND && hdr.append.cntrl_pkt_it != meta.cntrl_pkt_it) {
	    hdr.append.g_idx = hdr.append.g_idx + meta.assign_sn;	    
	    append_second_tbl.apply(); // handles post-sequencing exp_type
	}

	if (meta.ready_for_ack == 1) {
	    bit<32> slot = get_ack_idx.execute(0);
	    meta.ack_ready = check_ack.execute(slot);
	    ack_action_tbl.apply();
	    ack_get_lower_lat.apply();
	    ack_get_upper_lat.apply();
	    if (hdr.append.isValid() && hdr.append.exp_type == 2) {
	        drop();
	    }
	} 

	update_replicated_seq_no.apply();
	if (hdr.tail.isValid()) {
	    hdr.tail.hops = hdr.tail.hops + 1;
	    meta.hops = hdr.tail.hops;
	} 

        if (hdr.start_view.isValid()) {
	    hdr.start_view.highest_seen_seq_no = meta.assign_sn;
	    hdr.start_view.old_ring_view = meta.ring_view;
	    hdr.start_view.switch_id = meta.switch_id;
	}

        if (hdr.finalize_view.isValid()) {
	    // Set highest sequence number
	    meta.global_seq_no = hdr.finalize_view.highest_quorum_seq_no;
	    update_view.execute(0);
	}

        routing.apply();
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
	    meta.ring_process = 0;
	    meta.read_process = 0;
	    meta.route_to_shard = 0;
	    meta.route_to_client = 0;
	    meta.subscribe = 0;
	    
	    meta.canary_ok = 0;
	    meta.num_recirc_ports = 0;
	    meta.num_wait_ports = 0;
	    meta.port_idx = 0;
	    
	    meta.switch_id = 0;
	    meta.wait_time = 0;
	    meta.nonce = 0;
	    meta.ring_view = 0;
	    meta.num_shards = 0;
	    
	    meta.batch_size = 0;
	    meta.queue_congest = 0;
	    meta.shard_size = 0;
	    meta.duration = 0;
	    meta.overflow = 0;
	    meta.output = 0;
	    meta.ack_threshold = 0;
	    
	    meta.do_ing_mirroring = 0;
	    meta.do_egr_mirroring = 0;
	    meta.ing_mir_ses = 0;
	    meta.egr_mir_ses = 0;
	    meta.pkt_type = 0;
	    
	    meta.start_ts = 0;
	    meta.end_ts = 0;
	    meta.elapse_time = 0;


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
            transition select(hdr.ethernet.etherType) {
                TYPE_CONTROL: parse_control;
		TYPE_VIEW_CHANGE: parse_start_view;
            	TYPE_VIEW_ACK: parse_finalize_view;
                TYPE_IPV4: parse_ipv4;
                default: accept;
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
                TYPE_APPEND: parse_append;
                TYPE_APPEND_WAIT: parse_append;
                TYPE_APPEND_RESP: parse_append;
                TYPE_READ: parse_read;
                TYPE_READ_RESP: parse_read;
                TYPE_TAIL: parse_tail;
                TYPE_SUB: parse_sub;
                default: parse_canary;
            }
        }

        state parse_control {
            packet.extract(hdr.cntrl);
            transition parse_canary;
        }

        state parse_start_view {
            packet.extract(hdr.start_view);
            transition parse_canary;
        } 

        state parse_finalize_view {
            packet.extract(hdr.finalize_view);
            transition parse_canary;
        } 
        
        state parse_append {
            packet.extract(hdr.append);
            transition parse_canary;
        }

        state parse_read {
            packet.extract(hdr.read);
            transition parse_canary;
        }

        state parse_tail {
            packet.extract(hdr.tail);
            transition parse_canary;
        }

        state parse_sub {
            packet.extract(hdr.sub);
            transition parse_canary;
        }

        state parse_view {
            packet.extract(hdr.start_view);
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

    Register<bit<32>, bit<1>>(1, 0) queue_cnt; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(queue_cnt) curr_queue_cnt = {
        void apply(inout bit<32> new_queue_cnt) {
	    new_queue_cnt = meta.queue_congest;
	}
    };

    Register<bit<8>, bit<1>>(1, 0) view_change_active; 
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_change_active) set_active_view_change = {
        void apply(inout bit<8> is_active, out bit<8> output) {
	    is_active = 1;
	    output = 1;
	}
    };
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_change_active) unset_active_view_change = {
        void apply(inout bit<8> is_active, out bit<8> output) {
	    output = is_active;
	    is_active = 0;
	}
    };
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_change_active) get_active_view_change = {
        void apply(inout bit<8> is_active, out bit<8> output) {
	    output = is_active;
	}
    };

    

    Register<bit<32>, bit<1>>(1, 0) view_change_start_time; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(view_change_start_time) log_view_change_start_time = {
        void apply(inout bit<32> start_time) {
	    start_time = eg_prsr_md.global_tstamp[31:0];
	}
    };
    RegisterAction<bit<32>, bit<1>, bit<32>>(view_change_start_time) update_view_change_time = {
        void apply(inout bit<32> start_time) {
	    start_time = meta.egress_end_time - start_time;
	}
    };

    Register<bit<32>, bit<1>>(1, 0) time_in_pipeline; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(time_in_pipeline) update_pipeline_time = {
        void apply(inout bit<32> time) {
	    time = meta.elapse_time;
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

    action drop() {
        eg_dprsr_md.drop_ctl = 1;
    }
    table check_view_change {
	key = {
            meta.view_change_active: exact;
	}
	actions = {
	    drop;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }


    action set_active_view() {
	meta.view_change_active = set_active_view_change.execute(0);
    }
    action unset_active_view() {
	meta.view_change_active = unset_active_view_change.execute(0);
    }
    action get_active_view() {
	meta.view_change_active = get_active_view_change.execute(0);
    }
    table update_active_view {
	key = {
	    hdr.ethernet.etherType: exact;
            hdr.ring_type.type: exact;
	}
	actions = {
	    set_active_view;
	    unset_active_view;
	    get_active_view;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }

    action get_ts() {
	update_view_change_time.execute(0);
    }
    action log_ts() {
	log_view_change_start_time.execute(0);
    }
    table view_logistics {
	key = {
	    meta.view_change_active: exact;
	}
	actions = {
	    log_ts;
	    get_ts;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }

    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) canary_no_match_counter_eg0; 

    apply { 
	if (eg_dprsr_md.mirror_type != 0) {
            hdr.ring_type.type = TYPE_SUB_RESP;
	}

	meta.egress_end_time = eg_prsr_md.global_tstamp[31:0];
	if (hdr.ring_type.isValid()) {
            meta.queue_congest = (bit<32>)(standard_metadata.deq_qdepth);
            curr_queue_cnt.execute(0);
	    meta.elapse_time = meta.egress_end_time - hdr.ring_type.pipe_ts;
	    update_pipeline_time.execute(0);
	}

	if (hdr.ring_type.type == TYPE_APPEND_WAIT) {
	    hdr.ring_type.raw_elapsed_time = (bit<16>)(hdr.ring_type.end_ts - hdr.ring_type.start_ts);
            check_duration.apply();
	} 

	update_active_view.apply();
	view_logistics.apply();
	check_view_change.apply();

	if ((hdr.ring_type.isValid() || hdr.cntrl.isValid()) && hdr.canary.magic != CANARY_VALUE) {
	    canary_no_match_counter_eg0.count(0);
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
	MyIngress1(),
	MyIngressDeparser(),
	MyEgressParser(),
	MyEgress0(),
	MyDeparser()
) pipe;

Switch(pipe) main;
