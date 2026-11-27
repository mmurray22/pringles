/*******************************************************************************
 * P4_16: Ringworld
 * Credit: 
 *  - Base code come from p4 tutorial exercises
 *  - CPU port code inspiration comes from https://github.com/nsg-ethz/p4-learning/tree/master/examples/copy_to_cpu
 *  - Lots of inspiration from the p4_16 examples
 ******************************************************************************/

#include <core.p4>
#if __TARGET_TOFINO__ == 2
#include <t2na.p4>
#else
#include <tna.p4>
#endif

#include "../../common/64_bit_headers.p4"
#include "../../common/util.p4"

struct seq_pair_t { bit<32> hi; bit<32> lo; }

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
	    68      : parse_pktgen_timer; // Generated packet!
            69      : parse_pktgen_timer; // Generated packet!
            70      : parse_pktgen_timer; // Generated packet!
            71      : parse_pktgen_timer; // Generated packet!
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
	transition select(hdr.ethernet.type) {
            TYPE_CONTROL: parse_control;
            TYPE_VIEW_INDUCE: parse_induce_view;
            TYPE_VIEW_CHANGE: parse_trigger_view;
            TYPE_VIEW_ACK: parse_finalize_view;
            ETHERTYPE_IPV4: parse_ipv4;
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
            TYPE_APPEND_PRE: parse_append;
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

    state parse_induce_view {
        packet.extract(hdr.induce_view);
        transition parse_canary;
    } 

    state parse_trigger_view {
        packet.extract(hdr.trigger_view);
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
control MyIngress0(inout headers hdr,
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
    Register<bit<32>, bit<1>>(1) highest_lower_replicated_seq_no; // TODO: 64 bit
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_lower_replicated_seq_no) write_lower_replicated_seq_no = {
        void apply(inout bit<32> cur_replicated_seq_no) {
	    if (meta.new_upper == 1 || meta.lower_global_seq_no > cur_replicated_seq_no) {
	         cur_replicated_seq_no = meta.lower_global_seq_no;
	    }
        }
    };
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_lower_replicated_seq_no) write_all_lower_replicated_seq_no = {
        void apply(inout bit<32> cur_replicated_seq_no) {
	     cur_replicated_seq_no = meta.lower_global_seq_no;
        }
    };
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_lower_replicated_seq_no) get_all_lower_tail = {
        void apply(inout bit<32> curr_replicated_seq_no, out bit<32> old_replicated_seq_no) { // inout = register, out = output 
	       old_replicated_seq_no = curr_replicated_seq_no;
        }
    };
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_lower_replicated_seq_no) get_lower_tail = {
        void apply(inout bit<32> curr_replicated_seq_no, out bit<32> old_replicated_seq_no) { // inout = register, out = output 
	    if (meta.lower_global_seq_no < curr_replicated_seq_no) {
	       old_replicated_seq_no = curr_replicated_seq_no;
	    } else {
                old_replicated_seq_no = 0;
	    }
        }
    };
    
    Register<seq_pair_t, bit<1>>(1) highest_replicated; // TODO TODO NEW
    RegisterAction<seq_pair_t, bit<1>, bit<1>>(highest_replicated) update_replicated = {
        void apply(inout seq_pair_t v) {
            if (meta.upper_global_seq_no > v.hi) {
                v.hi = meta.upper_global_seq_no;
                v.lo = meta.lower_global_seq_no;
            } else if (meta.upper_global_seq_no == v.hi
                       && meta.lower_global_seq_no > v.lo) {
                v.lo = meta.lower_global_seq_no;
            }
        }
    };

    Register<bit<32>, bit<1>>(1) highest_upper_replicated_seq_no; // TODO: 64 bit
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_upper_replicated_seq_no) write_upper_replicated_seq_no = {
        void apply(inout bit<32> cur_replicated_seq_no, out bit<32> status) {
	    if (meta.upper_global_seq_no > cur_replicated_seq_no) {
	         cur_replicated_seq_no = meta.upper_global_seq_no;
		 status = 2;
	    } else if (meta.upper_global_seq_no == cur_replicated_seq_no) {
		 status = 1;
	    } else {
		 status = 0;
	    }
        }
    };
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_upper_replicated_seq_no) get_upper_tail = {
        void apply(inout bit<32> curr_replicated_seq_no, out bit<32> old_replicated_seq_no) { // inout = register, out = output 
	    if (meta.upper_global_seq_no < curr_replicated_seq_no || meta.upper_global_seq_no == curr_replicated_seq_no) {
	       old_replicated_seq_no = curr_replicated_seq_no + 1;
	    } else {
                old_replicated_seq_no = 0;
	    }
        }
    };

    // Largest global sequence number known to the switch Updated every time control packet is received.
    Register<bit<32>, bit<16>>(MAX_GSN_MAP_SIZE, 0) map_from_lower_gsn; // TODO: 64 bit
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_lower_gsn) get_lower_gsn = {
        void apply(inout bit<32> gsn_at_idx, out bit<32> gsn) { // inout = register, out = output 
	    gsn = gsn_at_idx;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_lower_gsn) add_lower_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = meta.lower_global_seq_no;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_lower_gsn) zero_lower_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = 0;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_lower_gsn) compare_lower_gsn = {
        void apply(inout bit<32> gsn_slot, out bit<32> higher) { // inout = register, out = output 
	    if (meta.lower_global_seq_no < gsn_slot || meta.lower_global_seq_no == gsn_slot) { // hdr.append.g_idx
	        higher = 1;
	    } else {
	        higher = 0;
	    }
        }
    };
    Register<bit<32>, bit<16>>(MAX_GSN_MAP_SIZE, 0) map_from_upper_gsn; // TODO: 64 bit
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_upper_gsn) get_upper_gsn = {
        void apply(inout bit<32> gsn_at_idx, out bit<32> gsn) { // inout = register, out = output 
	    gsn = gsn_at_idx;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_upper_gsn) add_upper_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = meta.upper_global_seq_no;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_upper_gsn) zero_upper_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = 0;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_upper_gsn) compare_upper_gsn = {
        void apply(inout bit<32> gsn_slot, out bit<32> higher) { // inout = register, out = output 
	    if (meta.upper_global_seq_no < gsn_slot) {
	        higher = 2;
	    } else if (meta.upper_global_seq_no == gsn_slot) {
	        higher = 1;
	    }  else {
	        higher = 0;
	    }
        }
    };
  

    /**** SETTING UP VARIABLES ****/
    action constants(bit<8> switch_id, 
		     bit<32> shard_size,
		     bit<16> number_of_shards,
		     bit<32> ack_threshold,
		     bit<8> num_recirc_ports,
		     bit<8> num_wait_ports) {
	
	meta.switch_id = switch_id;
	meta.shard_size = shard_size;	
	meta.num_shards = number_of_shards;
	meta.ack_threshold = ack_threshold;	
        meta.num_recirc_ports = num_recirc_ports;
        meta.num_wait_ports = num_wait_ports;
    }
    @pragma ternary 1 
    table get_constants {
	actions = {
	    constants;
	}
	size = 1;
	default_action = constants(1, 0, 0, 1, 0, 0);
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

    /**** CONTROL PACKET *****/

    // Number of total times the switch has seen the control packet
    // NOTE: It is anticipated that this value will overflow
    Register<bit<16>, bit<1>>(1, 0) cntrl_pkt_it; 
    RegisterAction<bit<16>, bit<1>, bit<16>>(cntrl_pkt_it) update_cntrl_pkt_it = {
        void apply(inout bit<16> cur_cntrl_pkt_it, out bit<16> output_cntrl_pkt_it) {
	    output_cntrl_pkt_it = cur_cntrl_pkt_it;
	    if (cur_cntrl_pkt_it == (bit<16>)MAX_GSN_MAP_IDX_VAL) {
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
	    if (cur_cntrl_pkt_it == 0) {
	        output_cntrl_pkt_it = (bit<16>)MAX_GSN_MAP_IDX_VAL;
	    } else {
	        output_cntrl_pkt_it = cur_cntrl_pkt_it - 1;
	    }	
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
    
    

    /**** TAIL *****/
    
    /****** SUBSCRIPTION ********/
    
    /****** ACKNOWLEDGEMENTS **********/
    Register<bit<32>, bit<32>>(MAX_INFLIGHT_ACKS, 0) ack_array; // TODO: 64 bit 
    RegisterAction<bit<32>, bit<32>, bit<8>>(ack_array) check_ack = {
        void apply(inout bit<32> curr_ack_cnt, out bit<8> ack_count) {
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
	    ret_ack_idx = hdr.append.lower_g_idx & (MAX_INFLIGHT_ACKS - 1);
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

    
    // Total packet counter 
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) cntrl_counter_ig0; 
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) seq_only_packet_counter0; 
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) tot_packet_counter0; 
    Register<bit<32>, bit<1>>(1, 0) seq_wait_recirc_counter; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(seq_wait_recirc_counter) update_seq_recirc_cntr = {
        void apply(inout bit<32> new_pkt_cntr) {
	    new_pkt_cntr = new_pkt_cntr + (bit<32>)hdr.append.seq_recirc;
	}
    };


    /******** CANARY **********/
    Register<bit<8>, bit<1>>(1, 1) true_magic_val;
    RegisterAction<bit<8>, bit<1>, bit<8>>(true_magic_val) write_magic = {
        void apply(inout bit<8> magic_val) {
            magic_val = hdr.canary.magic;
        }
    };
    Register<bit<16>, bit<1>>(1, 0) pesky_type;
    RegisterAction<bit<16>, bit<1>, bit<16>>(pesky_type) read_type = {
        void apply(inout bit<16> type) {
	    if (hdr.ethernet.isValid()) {
                type = hdr.ethernet.type;
	    } else {
	    	type = 0;
	    }
        }
    };

    action read_local() { // Control packet
	meta.local_seq_no = read_local_seq_no.execute(0);
    }
    action write_local() { // First append packet
        meta.local_seq_no = write_local_seq_no.execute(0); 
    }
    table cntrl_local_seq {
        key = {
            meta.ring_process: exact;
	    meta.append_g_zero: exact;
 	    hdr.cntrl.isValid(): exact;
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
	    meta.ring_process: exact; // ONLY get_it
            hdr.cntrl.isValid(): exact;
            hdr.trigger_view.isValid(): exact;
            meta.update_iteration: exact;
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

    action get_local_idx() {
	hdr.append.cntrl_pkt_it = meta.cntrl_pkt_it;
	hdr.append.local_seq_cntr = meta.local_seq_no;	    
	meta.recirc_port_type = 1;
	hdr.append.seq_recirc = hdr.append.seq_recirc + 1;
    }
    action wait_append() {
	hdr.append.seq_recirc = hdr.append.seq_recirc + 1;
	meta.recirc_port_type = 2;
    }
    action wait_ack_append() {
	meta.recirc_port_type = 2;
	hdr.append.seq_recirc = hdr.append.seq_recirc + 1;
    }
    action wait_to_seq() {
	meta.recirc_port_type = 1;
	hdr.append.seq_recirc = hdr.append.seq_recirc + 1;
    }
    action get_read_resp() {
	meta.dst_addr = hdr.ipv4.dst;
    }
    table append_first_tbl {
        key = {
            meta.ring_process: exact;
	    meta.append_g_zero: exact;
        }
        actions = {
            get_local_idx;
	    wait_append;
	    wait_ack_append;
	    wait_to_seq;
	    get_read_resp; // TYPE_READ_RESP
            NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }

    action exp_type_one() {
	seq_only_packet_counter0.count(0);
	drop();
    }
    action exp_type_two() {
	hdr.ring_type.start_ts = meta.current_timestamp[15:0];

	meta.recirc_port_type = 2;
	hdr.append.seq_recirc = hdr.append.seq_recirc - 1;
    }
    action regular_stream() {
	meta.route_to_shard = 1;
	meta.recirc_port_type = 0;
	hdr.ring_type.shard_id = (bit<16>)hdr.append.stream_id;
    }
    action regular_idx() {
	meta.route_to_shard = 1;
	meta.recirc_port_type = 0;
	hdr.ring_type.shard_id = (bit<16>)hdr.append.local_seq_cntr;
    }
    table append_second_tbl {
        key = {
	    hdr.append.exp_type: exact;
        }
        actions = {
            exp_type_one;
	    exp_type_two;
            regular_stream;
            regular_idx;
	    NoAction;
        }
        size = 256;
        default_action = NoAction;
    }


    action lower_update_gsn() {
	add_lower_gsn.execute(meta.cntrl_pkt_it_idx); // TODO: What is the order? Check this!
    }
    action lower_get_ack_readiness() {
	meta.lower_ready_for_ack = (bit<2>)compare_lower_gsn.execute(meta.cntrl_pkt_it_idx);
    }
    action lower_make_gsn_zero() {
	zero_lower_gsn.execute(meta.cntrl_pkt_it_idx); // Get Global sequence number
    }
    action lower_assign_gsn() {
        meta.lower_assign_sn = get_lower_gsn.execute(meta.cntrl_pkt_it_idx); // Get Global sequence number
    }
    table lower_process_gsn { // TODO TODO NEED TO ALTER SO MANY REGISTERS!!!!!
        key = {
            meta.ring_process: exact;
	    meta.append_g_zero: exact;
            hdr.cntrl.isValid(): exact;
            hdr.trigger_view.isValid(): exact;
        }
        actions = {
	    lower_get_ack_readiness;
            lower_make_gsn_zero;
	    lower_assign_gsn;
	    lower_update_gsn;
            NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }

    action upper_update_gsn() {
	add_upper_gsn.execute(meta.cntrl_pkt_it_idx); // TODO: What is the order? Check this!
    }
    action upper_get_ack_readiness() {
	meta.upper_ready_for_ack = (bit<2>)compare_upper_gsn.execute(meta.cntrl_pkt_it_idx);
    }
    action upper_make_gsn_zero() {
	zero_upper_gsn.execute(meta.cntrl_pkt_it_idx); // Get Global sequence number
    }
    action upper_assign_gsn() {
        meta.upper_assign_sn = get_upper_gsn.execute(meta.cntrl_pkt_it_idx); // Get Global sequence number
    }
    table upper_process_gsn { // TODO TODO NEED TO ALTER SO MANY REGISTERS!!!!!
        key = {
            meta.ring_process: exact;
	    meta.append_g_zero: exact;
            hdr.cntrl.isValid(): exact;
            hdr.trigger_view.isValid(): exact;
        }
        actions = {
	    upper_get_ack_readiness;
            upper_make_gsn_zero;
	    upper_assign_gsn;
	    upper_update_gsn;
            NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }

    action emulate_storage_ports() {
	meta.port_idx = get_storagerecirc_idx.execute(1);
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
	size = 4;
	default_action = NoAction;
    }


    action ack_append() {
	meta.dst_addr = hdr.ipv4.dst;
    }
    action ack_read() {
	meta.dst_addr = hdr.ipv4.dst;
    }
    table ack_action_tbl{
        key = {
            meta.ring_process: exact;
	    meta.ack_ready: exact;
        }
        actions = {
	    ack_append;
	    ack_read;
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
	    meta.ack_ready: exact;
        }
        actions = {
	    low_latency;
	    NoAction;
        }
        size = 64;
        default_action = NoAction;
    }
    action upper_latency() {
	update_high_lat_cntr.execute(0);
    }
    table ack_get_upper_lat {
	key = {
            meta.ring_process: exact;
	    meta.ack_ready: exact;
        }
        actions = {
	    upper_latency;
	    NoAction;
        }
        size = 64;
        default_action = NoAction;

    }

    action forward_to_subscribers(MirrorId_t mirror_session_id, bit<16> group_id) {
	meta.do_ing_mirroring = 1;
	meta.ing_mir_ses = mirror_session_id;
	meta.pkt_type = PKT_TYPE_MIRROR;
	ig_dprsr_md.mirror_type = MIRROR_TYPE_I2E;
        ig_tm_md.mcast_grp_a = group_id;
    } 
    action receive_subscription(egress_spec_t port) {
        ig_tm_md.ucast_egress_port = port;
    }
    action send_multicast(bit<16> group_id) { // This does: shard multicast, subscription multicast
        ig_tm_md.mcast_grp_a = group_id;
    }
    action eth_forward(mac_addr_t dst_addr, egress_spec_t port) {
        ig_tm_md.ucast_egress_port = port;
        hdr.ethernet.src = hdr.ethernet.dst;
        hdr.ethernet.dst = dst_addr;
        hdr.ipv4.ttl = hdr.ipv4.ttl - 1;
    }
    action send_to(egress_spec_t port) { // This does: circulate, wait, cntrl, and subscribe to self
        ig_tm_md.ucast_egress_port = port;
    }
    table routing {
        key = {
	    meta.next_hop: exact; // encodes pkt_id
            meta.dst_addr: lpm;
	    meta.recirc_port_type: exact;
	    meta.port_idx: exact; 
	    meta.route_to_shard: exact; // encodes shard_id
	    meta.view_hops: exact; // multicast if hop = 0, send_to external switch if hop = 1, send to cpu port if hop 2 
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

    action update_next_hop() { // Triggered on a FinalizeView
	update_id.execute(0); // Set control packet next member
    }
    action get_next_hop() { // This does: circulate, wait, cntrl, and subscribe to self
	meta.next_hop = get_id.execute(0);

    }
    table next_hop_routing {
        key = {
	    hdr.tail.isValid(): exact;
	    hdr.finalize_view.isValid(): exact;
	    hdr.cntrl.isValid(): exact; // encodes pkt_id
	}
	actions = {
	   get_next_hop;
	   update_next_hop;
	   NoAction;
	}
	size = 8;
	default_action = NoAction;
    }

    

    
    Meter<bit<1>>(1, MeterType_t.PACKETS) seq_admission;  
    /* Start of Ingress Pipeline */
    apply {	
	ig_dprsr_md.drop_ctl = 0;
        meta.recirc_port_type = 0;
        meta.route_to_shard= 0;
        meta.lower_ready_for_ack = 0;
        meta.upper_ready_for_ack = 0;
        meta.ack_ready = 0;

        meta.port_idx = 0;

        meta.view_change_active = 0;
        meta.accept_view_change = 0;

        meta.time_since_last_cntrl = 0; 
        meta.next_hop = 0;
        meta.update_iteration = 0;

        
        meta.cntrl_pkt_it = 0;
        meta.wait_time = 0;
        meta.ring_view = 0;
        meta.num_shards = 0;

        meta.dst_addr = 0;
        meta.cnt = 0;
        meta.lower_assign_sn = 0;
        meta.upper_assign_sn = 0;
        meta.local_seq_no = 0;
        meta.lower_global_seq_no = 0;
        meta.upper_global_seq_no = 0;
        meta.batch_size = 0; // For keeping tabs on the GSN table size
        meta.queue_congest = 0;
        meta.duration = 0;
        meta.overflow = 0;
        meta.output = 0;
        meta.ack_threshold = 0;
        meta.last_time = 0;

	bit<8> color = 0;
	meta.current_timestamp = standard_metadata.ingress_mac_tstamp[31:0];
        meta.do_ing_mirroring = 0;
        meta.do_egr_mirroring = 0;
        meta.ing_mir_ses = 0; 
        meta.egr_mir_ses = 9; 
        //meta.pkt_type = PKT_NORMAL;

        meta.elapsed_time = 0;
	meta.append_g_zero = 0;

	get_constants.apply();

	if (hdr.cntrl.isValid()) {
	    meta.ring_process = 0;
	    cntrl_counter_ig0.count(0);

	    // CHANGE LOG: ORIGINAL:
	    // meta.global_seq_no = hdr.cntrl.global_seq_no; // 64 bit

	   // NEW:
	   meta.upper_global_seq_no = hdr.cntrl.upper_global_seq_no;
	   meta.lower_global_seq_no = hdr.cntrl.lower_global_seq_no;
	} else if (hdr.ring_type.isValid()) {
	    // Determine if this packet needs to be routed to another switch
	    if (hdr.append.isValid()) {
		if (hdr.ring_type.type == TYPE_APPEND_PRE) {
	    	    color = seq_admission.execute(0);
	    	    if (color == MeterColor_t.RED) {
	    	        meta.append_g_zero = 0;
	    	        ig_dprsr_md.drop_ctl = 1;
			meta.ring_process = 0;
	    	    } else {
	    	        meta.append_g_zero = 1;
	    	    	hdr.append.start_ts = meta.current_timestamp;
			hdr.ring_type.type = TYPE_APPEND;
			meta.ring_process = TYPE_APPEND;
	    	    }
		} else {
	            meta.ring_process = hdr.ring_type.type;
	    	    meta.append_g_zero = 0;
		}
		meta.local_seq_no = hdr.append.local_seq_cntr;
	        meta.upper_global_seq_no = hdr.append.upper_g_idx;
	        meta.lower_global_seq_no = hdr.append.lower_g_idx;
	    } else {
	        meta.ring_process = hdr.ring_type.type;
	    }
	    hdr.ring_type.pipe_ts = ig_prsr_md.global_tstamp[15:0];
	} else if (hdr.finalize_view.isValid()) {
	    meta.upper_global_seq_no = hdr.finalize_view.highest_quorum_upper_seq_no;  
	    meta.lower_global_seq_no = hdr.finalize_view.highest_quorum_lower_seq_no;  
	} else {
	    meta.ring_process = 0;
	}

	if (hdr.tail.isValid()) {
	    if (hdr.tail.start_switch_id == meta.switch_id && hdr.tail.hops != 0) {
		meta.dst_addr = hdr.ipv4.dst;
	    } else {
		hdr.tail.hops = hdr.tail.hops + 1;
	    }
	    meta.upper_global_seq_no = hdr.tail.upper_tail_seq_no;
	    meta.lower_global_seq_no = hdr.tail.lower_tail_seq_no;
	} 

	bit<1> trigger_dst = 0;
	if (hdr.trigger_view.isValid() && hdr.trigger_view.view_member_switch_id == meta.switch_id) {
	    trigger_dst = 1;
	}

	cntrl_local_seq.apply();

	if (hdr.cntrl.isValid()) { // Only update pkt it if cntrl packet
	    meta.update_iteration = 1;
	}

	fill_cntrl_pkt_it.apply();

	if (meta.ring_process == TYPE_APPEND && meta.append_g_zero == 0) {
	    meta.cntrl_pkt_it_idx = hdr.append.cntrl_pkt_it;
	} else {
	    meta.cntrl_pkt_it_idx = meta.cntrl_pkt_it;
	}

	append_first_tbl.apply(); // handles g_idx == 0 and TYPE_WAIT

	lower_process_gsn.apply(); // handles TYPE_APPEND_RESP, handles GSN updates
	upper_process_gsn.apply(); // handles TYPE_APPEND_RESP, handles GSN updates
	
	if (hdr.cntrl.isValid() && meta.local_seq_no != 0) {
            hdr.cntrl.temp_local_seq_no = meta.local_seq_no; // TODO TODO THIS NEEDS EGRESS ACTION
	    meta.batch_size = meta.local_seq_no;
	    set_batch_sz.execute(meta.cntrl_pkt_it);
	    add_batch.execute(0);
	} else if (meta.ring_process == TYPE_APPEND && hdr.append.cntrl_pkt_it != meta.cntrl_pkt_it) {
	    bit<32> cnt = update_batch_sz.execute(hdr.append.cntrl_pkt_it);
	    if (cnt == 0) {
	        sub_batch.execute(0);
	    }
	    hdr.ring_type.type = TYPE_APPEND_RESP;
	    hdr.append.lower_g_idx = meta.lower_assign_sn;
	    hdr.append.upper_g_idx = meta.upper_assign_sn;
	    append_second_tbl.apply(); // handles post-sequencing exp_type
	}

	get_port_idx.apply();

	//next_hop_routing.apply();
	if (hdr.cntrl.isValid()) {
	    meta.next_hop = get_id.execute(0);
	} else if (hdr.tail.isValid()) {
	    meta.next_hop = get_id.execute(0);
	} else if (hdr.finalize_view.isValid() && hdr.finalize_view.view_member_switch_id == meta.switch_id) {
	    update_id.execute(0); // Set control packet next member
	}


	if (trigger_dst == 1) {
	    hdr.trigger_view.highest_seen_upper_seq_no = meta.upper_assign_sn;
	    hdr.trigger_view.highest_seen_lower_seq_no = meta.lower_assign_sn;
	}

	if (hdr.trigger_view.isValid()) {
	    meta.view_hops = 1;
	    if (hdr.trigger_view.view_member_switch_id == meta.switch_id) {
		hdr.trigger_view.hops = 1;
		if (hdr.trigger_view.leader_switch_id != meta.switch_id) {
		    meta.next_hop = hdr.trigger_view.leader_switch_id; 
		} else {
		    meta.next_hop = meta.switch_id;
		}
		meta.next_hop = hdr.trigger_view.view_member_switch_id;
	    } else if (hdr.trigger_view.hops == 0) {
		meta.next_hop = hdr.trigger_view.view_member_switch_id; 
	    } else if (hdr.trigger_view.leader_switch_id != meta.switch_id) {
		meta.next_hop = hdr.trigger_view.leader_switch_id;
	    } else {
		meta.next_hop = meta.switch_id;
	    }
        } else if (hdr.finalize_view.isValid()) {
	    meta.view_hops = 1;
	    if (hdr.finalize_view.view_member_switch_id == meta.switch_id) {
		hdr.finalize_view.hops = 1;
		if (hdr.finalize_view.leader_switch_id != meta.switch_id) {
		    meta.next_hop = hdr.finalize_view.leader_switch_id; 
		} else {
		    meta.next_hop = meta.switch_id;
		}
		meta.next_hop = hdr.finalize_view.view_member_switch_id;
	    } else if (hdr.finalize_view.hops == 0) {
		meta.next_hop = hdr.finalize_view.view_member_switch_id; 
	    } else if (hdr.finalize_view.leader_switch_id != meta.switch_id) {
		meta.next_hop = hdr.finalize_view.leader_switch_id;
	    } else {
		meta.next_hop = meta.switch_id;
	    }
        } else {
	    meta.view_hops = 0;
	}

	if (meta.upper_ready_for_ack == 2 || (meta.lower_ready_for_ack == 1 && meta.upper_ready_for_ack == 1)) {
	    bit<32> slot = get_ack_idx.execute(0);
	    meta.ack_ready = check_ack.execute(slot);
	    if (meta.ring_process == TYPE_APPEND_RESP && meta.ack_ready == 1) {
		bit<32> status = write_upper_replicated_seq_no.execute(0); // TODO TODO
		if (status == 2) {
		    meta.new_upper = 1;
	        } else {
		    meta.new_upper = 0;
		}
		if (status != 0) {
		    write_lower_replicated_seq_no.execute(0);
		}

	        tot_packet_counter0.count(0);
	        update_seq_recirc_cntr.execute(0);
	        meta.duration = meta.current_timestamp - hdr.append.start_ts;
	        ack_get_lower_lat.apply();
	        ack_get_upper_lat.apply();
	    }

	    ack_action_tbl.apply(); // TODO redo
	    if (hdr.append.isValid() && hdr.append.exp_type == 2) {
	        drop(); // <-- TODO put in routing table?
	    }
	    meta.recirc_port_type = 0;
	    meta.port_idx = 0;
	} else {
	    if (meta.ring_process == TYPE_TAIL) {
		bit<32> upper_temp = get_upper_tail.execute(0); 
		if (upper_temp != 0) {
		    bit<32> lower_temp = 0;
		    upper_temp = upper_temp - 1;
		    if (meta.upper_global_seq_no != upper_temp) {
	                lower_temp = get_all_lower_tail.execute(0); 
		    } else {
	                lower_temp = get_lower_tail.execute(0); 
		    }
		    if (lower_temp != 0) {
			hdr.tail.lower_tail_seq_no = lower_temp;
			hdr.tail.upper_tail_seq_no = upper_temp;
		    }
		}
	    }
	}

        routing.apply();
	hdr.timer.setInvalid();
     }
}

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
    Register<bit<32>, bit<1>>(1) highest_lower_replicated_seq_no; // TODO: 64 bit
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_lower_replicated_seq_no) write_lower_replicated_seq_no = {
        void apply(inout bit<32> cur_replicated_seq_no) {
	    if (meta.new_upper == 1 || meta.lower_global_seq_no > cur_replicated_seq_no) {
	         cur_replicated_seq_no = meta.lower_global_seq_no;
	    }
        }
    };
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_lower_replicated_seq_no) write_all_lower_replicated_seq_no = {
        void apply(inout bit<32> cur_replicated_seq_no) {
	     cur_replicated_seq_no = meta.lower_global_seq_no;
        }
    };
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_lower_replicated_seq_no) get_all_lower_tail = {
        void apply(inout bit<32> curr_replicated_seq_no, out bit<32> old_replicated_seq_no) { // inout = register, out = output 
	       old_replicated_seq_no = curr_replicated_seq_no;
        }
    };
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_lower_replicated_seq_no) get_lower_tail = {
        void apply(inout bit<32> curr_replicated_seq_no, out bit<32> old_replicated_seq_no) { // inout = register, out = output 
	    if (meta.lower_global_seq_no < curr_replicated_seq_no) {
	       old_replicated_seq_no = curr_replicated_seq_no;
	    } else {
                old_replicated_seq_no = 0;
	    }
        }
    };
    
    Register<seq_pair_t, bit<1>>(1) highest_replicated; // TODO TODO NEW
    RegisterAction<seq_pair_t, bit<1>, bit<1>>(highest_replicated) update_replicated = {
        void apply(inout seq_pair_t v) {
            if (meta.upper_global_seq_no > v.hi) {
                v.hi = meta.upper_global_seq_no;
                v.lo = meta.lower_global_seq_no;
            } else if (meta.upper_global_seq_no == v.hi
                       && meta.lower_global_seq_no > v.lo) {
                v.lo = meta.lower_global_seq_no;
            }
        }
    };

    Register<bit<32>, bit<1>>(1) highest_upper_replicated_seq_no; // TODO: 64 bit
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_upper_replicated_seq_no) write_upper_replicated_seq_no = {
        void apply(inout bit<32> cur_replicated_seq_no, out bit<32> status) {
	    if (meta.upper_global_seq_no > cur_replicated_seq_no) {
	         cur_replicated_seq_no = meta.upper_global_seq_no;
		 status = 2;
	    } else if (meta.upper_global_seq_no == cur_replicated_seq_no) {
		 status = 1;
	    } else {
		 status = 0;
	    }
        }
    };
    RegisterAction<bit<32>, bit<1>, bit<32>>(highest_upper_replicated_seq_no) get_upper_tail = {
        void apply(inout bit<32> curr_replicated_seq_no, out bit<32> old_replicated_seq_no) { // inout = register, out = output 
	    if (meta.upper_global_seq_no < curr_replicated_seq_no || meta.upper_global_seq_no == curr_replicated_seq_no) {
	       old_replicated_seq_no = curr_replicated_seq_no + 1;
	    } else {
                old_replicated_seq_no = 0;
	    }
        }
    };

    // Largest global sequence number known to the switch Updated every time control packet is received.
    Register<bit<32>, bit<16>>(MAX_GSN_MAP_SIZE, 0) map_from_lower_gsn; // TODO: 64 bit
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_lower_gsn) get_lower_gsn = {
        void apply(inout bit<32> gsn_at_idx, out bit<32> gsn) { // inout = register, out = output 
	    gsn = gsn_at_idx;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_lower_gsn) add_lower_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = meta.lower_global_seq_no;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_lower_gsn) zero_lower_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = 0;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_lower_gsn) compare_lower_gsn = {
        void apply(inout bit<32> gsn_slot, out bit<32> higher) { // inout = register, out = output 
	    if (meta.lower_global_seq_no < gsn_slot || meta.lower_global_seq_no == gsn_slot) { // hdr.append.g_idx
	        higher = 1;
	    } else {
	        higher = 0;
	    }
        }
    };
    Register<bit<32>, bit<16>>(MAX_GSN_MAP_SIZE, 0) map_from_upper_gsn; // TODO: 64 bit
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_upper_gsn) get_upper_gsn = {
        void apply(inout bit<32> gsn_at_idx, out bit<32> gsn) { // inout = register, out = output 
	    gsn = gsn_at_idx;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_upper_gsn) add_upper_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = meta.upper_global_seq_no;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_upper_gsn) zero_upper_gsn = {
        void apply(inout bit<32> gsn_slot) { // inout = register, out = output 
	    gsn_slot = 0;
        }
    };
    RegisterAction<bit<32>, bit<16>, bit<32>>(map_from_upper_gsn) compare_upper_gsn = {
        void apply(inout bit<32> gsn_slot, out bit<32> higher) { // inout = register, out = output 
	    if (meta.upper_global_seq_no < gsn_slot) {
	        higher = 2;
	    } else if (meta.upper_global_seq_no == gsn_slot) {
	        higher = 1;
	    }  else {
	        higher = 0;
	    }
        }
    };
  

    /**** SETTING UP VARIABLES ****/
    action constants(bit<8> switch_id, 
		     bit<32> shard_size,
		     bit<16> number_of_shards,
		     bit<32> ack_threshold,
		     bit<8> num_recirc_ports,
		     bit<8> num_wait_ports) {
	
	meta.switch_id = switch_id;
	meta.shard_size = shard_size;	
	meta.num_shards = number_of_shards;
	meta.ack_threshold = ack_threshold;	
        meta.num_recirc_ports = num_recirc_ports;
        meta.num_wait_ports = num_wait_ports;
    }
    @pragma ternary 1 
    table get_constants {
	actions = {
	    constants;
	}
	size = 1;
	default_action = constants(1, 0, 0, 1, 0, 0);
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

    /**** CONTROL PACKET *****/

    // Number of total times the switch has seen the control packet
    // NOTE: It is anticipated that this value will overflow
    Register<bit<16>, bit<1>>(1, 0) cntrl_pkt_it; 
    RegisterAction<bit<16>, bit<1>, bit<16>>(cntrl_pkt_it) update_cntrl_pkt_it = {
        void apply(inout bit<16> cur_cntrl_pkt_it, out bit<16> output_cntrl_pkt_it) {
	    output_cntrl_pkt_it = cur_cntrl_pkt_it;
	    if (cur_cntrl_pkt_it == (bit<16>)MAX_GSN_MAP_IDX_VAL) {
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
	    if (cur_cntrl_pkt_it == 0) {
	        output_cntrl_pkt_it = (bit<16>)MAX_GSN_MAP_IDX_VAL;
	    } else {
	        output_cntrl_pkt_it = cur_cntrl_pkt_it - 1;
	    }	
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
    
    

    /**** TAIL *****/
    
    /****** SUBSCRIPTION ********/
    
    /****** ACKNOWLEDGEMENTS **********/
    Register<bit<32>, bit<32>>(MAX_INFLIGHT_ACKS, 0) ack_array; // TODO: 64 bit 
    RegisterAction<bit<32>, bit<32>, bit<8>>(ack_array) check_ack = {
        void apply(inout bit<32> curr_ack_cnt, out bit<8> ack_count) {
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
	    ret_ack_idx = hdr.append.lower_g_idx & (MAX_INFLIGHT_ACKS - 1);
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

    
    // Total packet counter 
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) cntrl_counter_ig1; 
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) seq_only_packet_counter; 
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) tot_packet_counter1; 
    Register<bit<32>, bit<1>>(1, 0) seq_wait_recirc_counter; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(seq_wait_recirc_counter) update_seq_recirc_cntr = {
        void apply(inout bit<32> new_pkt_cntr) {
	    new_pkt_cntr = new_pkt_cntr + (bit<32>)hdr.append.seq_recirc;
	}
    };


    /******** CANARY **********/
    Register<bit<8>, bit<1>>(1, 1) true_magic_val;
    RegisterAction<bit<8>, bit<1>, bit<8>>(true_magic_val) write_magic = {
        void apply(inout bit<8> magic_val) {
            magic_val = hdr.canary.magic;
        }
    };
    Register<bit<16>, bit<1>>(1, 0) pesky_type;
    RegisterAction<bit<16>, bit<1>, bit<16>>(pesky_type) read_type = {
        void apply(inout bit<16> type) {
	    if (hdr.ethernet.isValid()) {
                type = hdr.ethernet.type;
	    } else {
	    	type = 0;
	    }
        }
    };

    action read_local() { // Control packet
	meta.local_seq_no = read_local_seq_no.execute(0);
    }
    action write_local() { // First append packet
        meta.local_seq_no = write_local_seq_no.execute(0); 
    }
    table cntrl_local_seq {
        key = {
            meta.ring_process: exact;
	    meta.append_g_zero: exact;
 	    hdr.cntrl.isValid(): exact;
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
	    meta.ring_process: exact; // ONLY get_it
            hdr.cntrl.isValid(): exact;
            hdr.trigger_view.isValid(): exact;
            meta.update_iteration: exact;
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

    action get_local_idx() {
	hdr.append.cntrl_pkt_it = meta.cntrl_pkt_it;
	hdr.append.local_seq_cntr = meta.local_seq_no;	    
	meta.recirc_port_type = 1;
	hdr.append.seq_recirc = hdr.append.seq_recirc + 1;
    }
    action wait_append() {
	hdr.append.seq_recirc = hdr.append.seq_recirc + 1;
	meta.recirc_port_type = 2;
    }
    action wait_ack_append() {
	meta.recirc_port_type = 2;
	hdr.append.seq_recirc = hdr.append.seq_recirc + 1;
    }
    action wait_to_seq() {
	meta.recirc_port_type = 1;
	hdr.append.seq_recirc = hdr.append.seq_recirc + 1;
    }
    action get_read_resp() {
	meta.dst_addr = hdr.ipv4.dst;
    }
    table append_first_tbl {
        key = {
            meta.ring_process: exact;
	    meta.append_g_zero: exact;
        }
        actions = {
            get_local_idx;
	    wait_append;
	    wait_ack_append;
	    wait_to_seq;
	    get_read_resp; // TYPE_READ_RESP
            NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }

    action exp_type_one() {
	seq_only_packet_counter.count(0);
	drop();
    }
    action exp_type_two() {
	hdr.ring_type.start_ts = meta.current_timestamp[15:0];

	meta.recirc_port_type = 2;
	hdr.append.seq_recirc = hdr.append.seq_recirc - 1;
    }
    action regular_stream() {
	meta.route_to_shard = 1;
	meta.recirc_port_type = 0;
	hdr.ring_type.shard_id = (bit<16>)hdr.append.stream_id;
    }
    action regular_idx() {
	meta.route_to_shard = 1;
	meta.recirc_port_type = 0;
	hdr.ring_type.shard_id = (bit<16>)hdr.append.local_seq_cntr;
    }
    table append_second_tbl {
        key = {
	    hdr.append.exp_type: exact;
        }
        actions = {
            exp_type_one;
	    exp_type_two;
            regular_stream;
            regular_idx;
	    NoAction;
        }
        size = 256;
        default_action = NoAction;
    }


    action lower_update_gsn() {
	add_lower_gsn.execute(meta.cntrl_pkt_it_idx); // TODO: What is the order? Check this!
    }
    action lower_get_ack_readiness() {
	// CHANGE LOG: ORIGINAL:
	//meta.ready_for_ack = (bit<1>)compare_gsn.execute(meta.cntrl_pkt_it_idx);

	// NEW:
	meta.lower_ready_for_ack = (bit<2>)compare_lower_gsn.execute(meta.cntrl_pkt_it_idx);
    }
    action lower_make_gsn_zero() {
	// CHANGE LOG: ORIGINAL:
	//zero_gsn.execute(meta.cntrl_pkt_it_idx); // Get Global sequence number

	// NEW:
	zero_lower_gsn.execute(meta.cntrl_pkt_it_idx); // Get Global sequence number
    }
    action lower_assign_gsn() {
        meta.lower_assign_sn = get_lower_gsn.execute(meta.cntrl_pkt_it_idx); // Get Global sequence number
    }
    table lower_process_gsn { // TODO TODO NEED TO ALTER SO MANY REGISTERS!!!!!
        key = {
            meta.ring_process: exact;
	    meta.append_g_zero: exact;
            hdr.cntrl.isValid(): exact;
            hdr.trigger_view.isValid(): exact;
        }
        actions = {
	    lower_get_ack_readiness;
            lower_make_gsn_zero;
	    lower_assign_gsn;
	    lower_update_gsn;
            NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }

    action upper_update_gsn() {
	add_upper_gsn.execute(meta.cntrl_pkt_it_idx); // TODO: What is the order? Check this!
    }
    action upper_get_ack_readiness() {
	meta.upper_ready_for_ack = (bit<2>)compare_upper_gsn.execute(meta.cntrl_pkt_it_idx);
    }
    action upper_make_gsn_zero() {
	zero_upper_gsn.execute(meta.cntrl_pkt_it_idx); // Get Global sequence number
    }
    action upper_assign_gsn() {
        meta.upper_assign_sn = get_upper_gsn.execute(meta.cntrl_pkt_it_idx); // Get Global sequence number
    }
    table upper_process_gsn { // TODO TODO NEED TO ALTER SO MANY REGISTERS!!!!!
        key = {
            meta.ring_process: exact;
	    meta.append_g_zero: exact;
            hdr.cntrl.isValid(): exact;
            hdr.trigger_view.isValid(): exact;
        }
        actions = {
	    upper_get_ack_readiness;
            upper_make_gsn_zero;
	    upper_assign_gsn;
	    upper_update_gsn;
            NoAction;
        }
        size = 1024;
        default_action = NoAction;
    }

    action emulate_storage_ports() {
	meta.port_idx = get_storagerecirc_idx.execute(1);
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
	size = 4;
	default_action = NoAction;
    }


    action ack_append() {
	meta.dst_addr = hdr.ipv4.dst;
    }
    action ack_read() {
	meta.dst_addr = hdr.ipv4.dst;
    }
    table ack_action_tbl{
        key = {
            meta.ring_process: exact;
	    meta.ack_ready: exact;
        }
        actions = {
	    ack_append;
	    ack_read;
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
	    meta.ack_ready: exact;
        }
        actions = {
	    low_latency;
	    NoAction;
        }
        size = 64;
        default_action = NoAction;
    }
    action upper_latency() {
	update_high_lat_cntr.execute(0);
    }
    table ack_get_upper_lat {
	key = {
            meta.ring_process: exact;
	    meta.ack_ready: exact;
        }
        actions = {
	    upper_latency;
	    NoAction;
        }
        size = 64;
        default_action = NoAction;

    }

    action forward_to_subscribers(MirrorId_t mirror_session_id, bit<16> group_id) {
	meta.do_ing_mirroring = 1;
	meta.ing_mir_ses = mirror_session_id;
	meta.pkt_type = PKT_TYPE_MIRROR;
	ig_dprsr_md.mirror_type = MIRROR_TYPE_I2E;
        ig_tm_md.mcast_grp_a = group_id;
    } 
    action receive_subscription(egress_spec_t port) {
        ig_tm_md.ucast_egress_port = port;
    }
    action send_multicast(bit<16> group_id) { // This does: shard multicast, subscription multicast
        ig_tm_md.mcast_grp_a = group_id;
    }
    action eth_forward(mac_addr_t dst_addr, egress_spec_t port) {
        ig_tm_md.ucast_egress_port = port;
        hdr.ethernet.src = hdr.ethernet.dst;
        hdr.ethernet.dst = dst_addr;
        hdr.ipv4.ttl = hdr.ipv4.ttl - 1;
    }
    action send_to(egress_spec_t port) { // This does: circulate, wait, cntrl, and subscribe to self
        ig_tm_md.ucast_egress_port = port;
    }
    table routing {
        key = {
	    meta.next_hop: exact; // encodes pkt_id
            meta.dst_addr: lpm;
	    meta.recirc_port_type: exact;
	    meta.port_idx: exact; 
	    meta.route_to_shard: exact; // encodes shard_id
	    meta.view_hops: exact; // multicast if hop = 0, send_to external switch if hop = 1, send to cpu port if hop 2 
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

    action update_next_hop() { // Triggered on a FinalizeView
	update_id.execute(0); // Set control packet next member
    }
    action get_next_hop() { // This does: circulate, wait, cntrl, and subscribe to self
	meta.next_hop = get_id.execute(0);

    }
    table next_hop_routing {
        key = {
	    hdr.tail.isValid(): exact;
	    hdr.finalize_view.isValid(): exact;
	    hdr.cntrl.isValid(): exact; // encodes pkt_id
	}
	actions = {
	   get_next_hop;
	   update_next_hop;
	   NoAction;
	}
	size = 8;
	default_action = NoAction;
    }

    Meter<bit<1>>(1, MeterType_t.PACKETS) seq_admission;  
    /* Start of Ingress Pipeline */
    apply {	
	ig_dprsr_md.drop_ctl = 0;
        meta.recirc_port_type = 0;
        meta.route_to_shard= 0;
        meta.lower_ready_for_ack = 0;
        meta.upper_ready_for_ack = 0;
        meta.ack_ready = 0;

        meta.port_idx = 0;

        meta.view_change_active = 0;
        meta.accept_view_change = 0;

        meta.time_since_last_cntrl = 0; 
        meta.next_hop = 0;
        meta.update_iteration = 0;

        
        meta.cntrl_pkt_it = 0;
        meta.wait_time = 0;
        meta.ring_view = 0;
        meta.num_shards = 0;

        meta.dst_addr = 0;
        meta.cnt = 0;
        meta.lower_assign_sn = 0;
        meta.upper_assign_sn = 0;
        meta.local_seq_no = 0;
        meta.lower_global_seq_no = 0;
        meta.upper_global_seq_no = 0;
        meta.batch_size = 0; // For keeping tabs on the GSN table size
        meta.queue_congest = 0;
        meta.duration = 0;
        meta.overflow = 0;
        meta.output = 0;
        meta.ack_threshold = 0;
        meta.last_time = 0;
	bit<8> color = 0;

	meta.current_timestamp = standard_metadata.ingress_mac_tstamp[31:0];
        meta.do_ing_mirroring = 0;
        meta.do_egr_mirroring = 0;
        meta.ing_mir_ses = 0; 
        meta.egr_mir_ses = 9; 
        //meta.pkt_type = PKT_NORMAL;

        meta.elapsed_time = 0;
	meta.append_g_zero = 0;

	get_constants.apply();

	if (hdr.cntrl.isValid()) {
	    meta.ring_process = 0;
	    cntrl_counter_ig1.count(0);

	   meta.upper_global_seq_no = hdr.cntrl.upper_global_seq_no;
	   meta.lower_global_seq_no = hdr.cntrl.lower_global_seq_no;
	} else if (hdr.ring_type.isValid()) {
	    // Determine if this packet needs to be routed to another switch
	    if (hdr.append.isValid()) {
		if (hdr.ring_type.type == TYPE_APPEND_PRE) {
	    	    color = seq_admission.execute(0);
	    	    if (color == MeterColor_t.RED) {
	    	        meta.append_g_zero = 0;
	    	        ig_dprsr_md.drop_ctl = 1;
			meta.ring_process = 0;
	    	    } else {
	    	        meta.append_g_zero = 1;
	    	    	hdr.append.start_ts = meta.current_timestamp;
			hdr.ring_type.type = TYPE_APPEND;
			meta.ring_process = TYPE_APPEND;
	    	    }
		} else {

	            meta.ring_process = hdr.ring_type.type;
	    	    meta.append_g_zero = 0;
		}
		meta.local_seq_no = hdr.append.local_seq_cntr;
	        meta.upper_global_seq_no = hdr.append.upper_g_idx;
	        meta.lower_global_seq_no = hdr.append.lower_g_idx;
	    } else {
	        meta.ring_process = hdr.ring_type.type;
	    }
	    hdr.ring_type.pipe_ts = ig_prsr_md.global_tstamp[15:0];
	} else if (hdr.finalize_view.isValid()) {
	    meta.upper_global_seq_no = hdr.finalize_view.highest_quorum_upper_seq_no; // TODO TODO NOT SAFETY 
	    meta.lower_global_seq_no = hdr.finalize_view.highest_quorum_lower_seq_no; // TODO TODO NOT SAFETY 
	} else {
	    meta.ring_process = 0;
	}

	if (hdr.tail.isValid()) {
	    if (hdr.tail.start_switch_id == meta.switch_id && hdr.tail.hops != 0) {
		meta.dst_addr = hdr.ipv4.dst;
	    } else {
		hdr.tail.hops = hdr.tail.hops + 1;
	    }
	    meta.upper_global_seq_no = hdr.tail.upper_tail_seq_no;
	    meta.lower_global_seq_no = hdr.tail.lower_tail_seq_no;
	} 
	
	bit<1> trigger_dst = 0;
	if (hdr.trigger_view.isValid() && hdr.trigger_view.view_member_switch_id == meta.switch_id) {
	    trigger_dst = 1;
	}
		
	cntrl_local_seq.apply();

	if (hdr.cntrl.isValid()) { // Only update pkt it if cntrl packet
	    meta.update_iteration = 1;
	}

	fill_cntrl_pkt_it.apply();

	if (meta.ring_process == TYPE_APPEND && meta.append_g_zero == 0) {
	    meta.cntrl_pkt_it_idx = hdr.append.cntrl_pkt_it;
	} else {
	    meta.cntrl_pkt_it_idx = meta.cntrl_pkt_it;
	}

	append_first_tbl.apply(); // handles g_idx == 0 and TYPE_WAIT

	lower_process_gsn.apply(); // handles TYPE_APPEND_RESP, handles GSN updates
	upper_process_gsn.apply(); // handles TYPE_APPEND_RESP, handles GSN updates
	
	if (hdr.cntrl.isValid() && meta.local_seq_no != 0) {
            hdr.cntrl.temp_local_seq_no = meta.local_seq_no; // TODO TODO THIS NEEDS EGRESS ACTION
	    meta.batch_size = meta.local_seq_no;
	    set_batch_sz.execute(meta.cntrl_pkt_it);
	    add_batch.execute(0);
	} else if (meta.ring_process == TYPE_APPEND && hdr.append.cntrl_pkt_it != meta.cntrl_pkt_it) {
	    bit<32> cnt = update_batch_sz.execute(hdr.append.cntrl_pkt_it);
	    if (cnt == 0) {
	        sub_batch.execute(0);
	    }
	    hdr.ring_type.type = TYPE_APPEND_RESP;
	    hdr.append.lower_g_idx = meta.lower_assign_sn;
	    hdr.append.upper_g_idx = meta.upper_assign_sn;
	    append_second_tbl.apply(); // handles post-sequencing exp_type
	}

	get_port_idx.apply();

	//next_hop_routing.apply(); <-- TODO new change
	if (hdr.cntrl.isValid()) {
	    meta.next_hop = get_id.execute(0);
	} else if (hdr.tail.isValid()) {
	    meta.next_hop = get_id.execute(0);
	} else if (hdr.finalize_view.isValid()) {
	    if (hdr.finalize_view.view_member_switch_id == meta.switch_id) {
	        update_id.execute(0); // Set control packet next member
	    }
	}

	if (trigger_dst == 1) {
	    hdr.trigger_view.highest_seen_upper_seq_no = meta.upper_assign_sn;
	    hdr.trigger_view.highest_seen_lower_seq_no = meta.lower_assign_sn;
	}

	if (hdr.trigger_view.isValid()) {
	    meta.view_hops = 1;
	    if (hdr.trigger_view.view_member_switch_id == meta.switch_id) {
		hdr.trigger_view.hops = 1;
		if (hdr.trigger_view.leader_switch_id != meta.switch_id) {
		    meta.next_hop = hdr.trigger_view.leader_switch_id; 
		} else {
		    meta.next_hop = meta.switch_id;
		}
		meta.next_hop = hdr.trigger_view.view_member_switch_id;
	    } else if (hdr.trigger_view.hops == 0) {
		meta.next_hop = hdr.trigger_view.view_member_switch_id; 
	    } else if (hdr.trigger_view.leader_switch_id != meta.switch_id) {
		meta.next_hop = hdr.trigger_view.leader_switch_id;
	    } else {
		meta.next_hop = meta.switch_id;
	    }
        } else if (hdr.finalize_view.isValid()) {
	    meta.view_hops = 1;
	    if (hdr.finalize_view.view_member_switch_id == meta.switch_id) {
		hdr.finalize_view.hops = 1;
		if (hdr.finalize_view.leader_switch_id != meta.switch_id) {
		    meta.next_hop = hdr.finalize_view.leader_switch_id; 
		} else {
		    meta.next_hop = meta.switch_id;
		}
		meta.next_hop = hdr.finalize_view.view_member_switch_id;
	    } else if (hdr.finalize_view.hops == 0) {
		meta.next_hop = hdr.finalize_view.view_member_switch_id; 
	    } else if (hdr.finalize_view.leader_switch_id != meta.switch_id) {
		meta.next_hop = hdr.finalize_view.leader_switch_id;
	    } else {
		meta.next_hop = meta.switch_id;
	    }
        } else {
	    meta.view_hops = 0;
	}

	if (meta.upper_ready_for_ack == 2 || (meta.lower_ready_for_ack == 1 && meta.upper_ready_for_ack == 1)) {
	    bit<32> slot = get_ack_idx.execute(0);
	    meta.ack_ready = check_ack.execute(slot);
	    if (meta.ring_process == TYPE_APPEND_RESP && meta.ack_ready == 1) {
		bit<32> status = write_upper_replicated_seq_no.execute(0); // TODO TODO
		if (status == 2) {
		    meta.new_upper = 1;
	        } else {
		    meta.new_upper = 0;
		}
		if (status != 0) {
		    write_lower_replicated_seq_no.execute(0);
		}


	        tot_packet_counter1.count(0);
	        update_seq_recirc_cntr.execute(0);
	        meta.duration = meta.current_timestamp - hdr.append.start_ts;
	        ack_get_lower_lat.apply();
	        ack_get_upper_lat.apply();
	    }

	    ack_action_tbl.apply(); // TODO redo
	    if (hdr.append.isValid() && hdr.append.exp_type == 2) {
	        drop(); // <-- TODO put in routing table?
	    }
	    meta.recirc_port_type = 0;
	    meta.port_idx = 0;
	} else {
	    if (meta.ring_process == TYPE_TAIL) {
	        bit<32> upper_temp = get_upper_tail.execute(0); 
		if (upper_temp != 0) {
		    bit<32> lower_temp = 0;
		    upper_temp = upper_temp - 1;
		    if (meta.upper_global_seq_no != upper_temp) {
	                lower_temp = get_all_lower_tail.execute(0); 
		    } else {
	                lower_temp = get_lower_tail.execute(0); 
		    }
		    if (lower_temp != 0) {
			hdr.tail.lower_tail_seq_no = lower_temp;
			hdr.tail.upper_tail_seq_no = upper_temp;
		    }
		}
	    }
	}

        routing.apply();
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
    Digest<digest_timeout_t>() digest_timeout;

    apply {
	if (ig_dprsr_md.digest_type == 1) {
	    // We have: meta.last_time, and meta.current_time
	    bit<8> hello = 0;
	    //digest_timeout.pack({hello});
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
 		      out metadata meta,
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
            transition select(hdr.ethernet.type) {
                TYPE_CONTROL: parse_control;
		TYPE_VIEW_INDUCE: parse_induce_view;
		TYPE_VIEW_CHANGE: parse_trigger_view;
            	TYPE_VIEW_ACK: parse_finalize_view;
                ETHERTYPE_IPV4: parse_ipv4;
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

	state parse_induce_view {
   	    packet.extract(hdr.induce_view);
   	    transition parse_canary;
   	} 

   	state parse_trigger_view {
   	    packet.extract(hdr.trigger_view);
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

    Register<bit<32>, bit<16>>(MAX_NUM_QUEUES, 0) queue_cnt; 
    RegisterAction<bit<32>, bit<16>, bit<32>>(queue_cnt) curr_queue_cnt = {
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
	    is_active = 0;
	    output = 0;
	}
    };
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_change_active) get_active_view_change = {
        void apply(inout bit<8> is_active, out bit<8> output) {
	    output = is_active;
	}
    };

    
    Register<bit<32>, bit<1>>(2, 0) view_change_start_time; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(view_change_start_time) log_view_change_time = {
        void apply(inout bit<32> start_time) {
	    start_time = eg_prsr_md.global_tstamp[31:0];
	}
    };

    Register<bit<16>, bit<1>>(1, 0) time_in_pipeline; 
    RegisterAction<bit<16>, bit<1>, bit<16>>(time_in_pipeline) update_pipeline_time = {
        void apply(inout bit<16> time) {
	    time = meta.pipeline_elapsed_time;
	}
    };

    // Tracks how long packet was waiting to be allowed to advance to acknowledgements (part of waiting)
    Register<bit<16>, bit<1>>(1, 0) raw_elapsed_time; 
    RegisterAction<bit<16>, bit<1>, bit<16>>(raw_elapsed_time) update_raw_elapsed_time = {
        void apply(inout bit<16> raw_cntr) {
	    raw_cntr = meta.elapsed_time;
	}
    };

    action update_type() {
	hdr.ring_type.type = TYPE_APPEND_RESP;
    }
    
    table check_duration {
	key = {
            meta.elapsed_time: range;
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
	    hdr.ethernet.type: exact;
	}
	actions = {
	    drop;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }

    action set_ring_process(bit<16> process_type) {
	meta.ring_process = process_type;
    }
    table get_packet_type {
        key = {
            hdr.ring_type.type: exact; // This is being set at the endpoints
        }
        actions = {
	    set_ring_process;
	    NoAction;
        }
        size = 256;
        default_action = NoAction;
    }

    
    // Register keeps track of the current view that the switch/pipeline is currently part of
    Register<bit<16>, bit<1>>(1, 0) egress_current_view;
    RegisterAction<bit<16>, bit<1>, bit<16>>(egress_current_view) trigger_view_change = {
        void apply(inout bit<16> curr_view, out bit<16> output) {
	    if (curr_view <= meta.ring_view) {
	        output = curr_view;
	    } else {
	        output = 0;
	    }
	}
    };
    RegisterAction<bit<16>, bit<1>, bit<16>>(egress_current_view) check_cntrl_view = {
        void apply(inout bit<16> curr_view, out bit<16> output) {
	    if (curr_view == meta.ring_view) {
	        output = 0;
	    } else {
	        output = 1;
	    }
	}
    };
    RegisterAction<bit<16>, bit<1>, bit<16>>(egress_current_view) update_view = {
        void apply(inout bit<16> curr_view) {
	    curr_view = meta.ring_view;
	}
    };


    Register<bit<8>, bit<1>>(1, 0) view_leader;
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_leader) record_view_leader = {
        void apply(inout bit<8> curr_view_leader, out bit<8> output) {
	    if (curr_view_leader == 0) {
	        curr_view_leader = meta.leader_switch_id;
		output = 1;
	    } else {
		output = 0;
	    }
	}
    };
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_leader) unset_view_leader = {
        void apply(inout bit<8> curr_view_leader) {
	    curr_view_leader = 0;
	}
    };

    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) canary_no_match_counter_eg0; 

    apply { 
	eg_dprsr_md.drop_ctl = 0;
        meta.route_to_shard= 0;
        meta.lower_ready_for_ack = 0;
        meta.upper_ready_for_ack = 0;
        meta.ack_ready = 0;

        meta.ring_process = 0;
        meta.num_recirc_ports = 0;
        meta.num_wait_ports = 0;
        meta.switch_id = 0;

        meta.view_hops = 0;

        meta.time_since_last_cntrl = 0; 
        meta.next_hop = 0;
        meta.update_iteration = 0;
        meta.append_g_zero = 0;
        
        meta.cntrl_pkt_it = 0;
        meta.wait_time = 0;
        meta.num_shards = 0;

        meta.dst_addr = 0;
        meta.cnt = 0;
        meta.lower_assign_sn = 0;
        meta.upper_assign_sn = 0;
        meta.local_seq_no = 0;
        meta.upper_global_seq_no = 0;
        meta.lower_global_seq_no = 0;
        meta.batch_size = 0; // For keeping tabs on the GSN table size

        meta.shard_size = 0;
        meta.duration = 0;
        meta.overflow = 0;
        meta.output = 0;
        meta.ack_threshold = 0;
        meta.last_time = 0;

        meta.do_ing_mirroring = 0;
        meta.do_egr_mirroring = 0;
        meta.ing_mir_ses = 0; 
        meta.egr_mir_ses = 9; 
        //meta.pkt_type = PKT_NORMAL;


	// Combine the control packet components
	if (hdr.cntrl.isValid()) {
 	    meta.low_bit = hdr.cntrl.lower_global_seq_no[31:31];
	    hdr.cntrl.lower_global_seq_no = hdr.cntrl.temp_local_seq_no + hdr.cntrl.lower_global_seq_no;
	    if (meta.low_bit == 1 && hdr.cntrl.lower_global_seq_no[31:31] == 0) {
	    	hdr.cntrl.upper_global_seq_no = hdr.cntrl.upper_global_seq_no + 1;
	    }
	    hdr.cntrl.temp_local_seq_no = 0;
	} else if (hdr.append.isValid() && hdr.ring_type.type == TYPE_APPEND_RESP) {
	    meta.low_bit = hdr.append.lower_g_idx[31:31];
	    hdr.append.lower_g_idx = hdr.append.local_seq_cntr + hdr.append.lower_g_idx;
	    if (meta.low_bit == 1 && hdr.append.lower_g_idx[31:31] == 0) {
	    	hdr.append.upper_g_idx = hdr.append.upper_g_idx + 1;
	    }
	    hdr.append.local_seq_cntr = 0;
	}

	if (hdr.ring_type.isValid()) {
            if (eg_dprsr_md.mirror_type != 0) {
                hdr.ring_type.type = TYPE_SUB_RESP;
	    }

	    get_packet_type.apply();
            meta.queue_congest = (bit<32>)(standard_metadata.deq_qdepth);
	    bit<16> queue_indx = (bit<16>)(standard_metadata.egress_port ++ standard_metadata.egress_qid);
            curr_queue_cnt.execute(queue_indx);

	    meta.pipeline_elapsed_time = eg_prsr_md.global_tstamp[15:0] - hdr.ring_type.pipe_ts;
	    update_pipeline_time.execute(0);

	    if (hdr.ring_type.type == TYPE_APPEND_WAIT) {
	        meta.elapsed_time = eg_prsr_md.global_tstamp[15:0] - hdr.ring_type.start_ts;
                check_duration.apply();
	    } 
	} else {
            meta.queue_congest = 0;
            meta.elapsed_time = 0;
	}
			

	// Read, Update, or Check view number info
	// Check whether you should accept the changes in the finalize_view if it's the right leader
	if (hdr.trigger_view.isValid()) {
	    meta.ring_view = hdr.trigger_view.current_ring_view;
            meta.accept_view_change = trigger_view_change.execute(0);
	    hdr.trigger_view.current_ring_view = meta.accept_view_change;
	    meta.accept_finalize_view = 0; 
	    if (meta.accept_view_change != 0) {
	        meta.view_change_active = set_active_view_change.execute(0);
	        meta.leader_switch_id = hdr.trigger_view.leader_switch_id;
	        meta.accept_finalize_view = record_view_leader.execute(0); 
	    }
	} else if (hdr.cntrl.isValid()) {
	    meta.ring_view = hdr.cntrl.ring_view;
            meta.accept_view_change = check_cntrl_view.execute(0);
	    meta.accept_finalize_view = 0;
	    if (meta.accept_view_change != 0) {
	        meta.view_change_active = set_active_view_change.execute(0);
	    } else {
	        meta.view_change_active = unset_active_view_change.execute(0);
	        unset_view_leader.execute(0);
	    }
	} else if (hdr.finalize_view.isValid()) {

	    meta.ring_view = hdr.finalize_view.current_ring_view;
	    update_view.execute(0); // UNSETS the view change!
	    meta.view_change_active = 0;
	    meta.accept_finalize_view = 0;
	} else {
	    meta.ring_view = 0;
	    meta.accept_view_change = 0;
	    meta.accept_finalize_view = 0;
	    meta.view_change_active = get_active_view_change.execute(0);
	}

	//if (hdr.trigger_view.isValid()) {
	//    meta.leader_switch_id = hdr.trigger_view.leader_switch_id;
	//    meta.accept_finalize_view = record_view_leader.execute(0); 

	//    if (meta.accept_finalize_view == 1) {
	//        meta.ring_view = hdr.trigger_view.current_ring_view;
        //        meta.accept_view_change = trigger_view_change.execute(0);
	//	hdr.trigger_view.current_ring_view = meta.accept_view_change;
	//    } else {
	//	meta.ring_view = 0;
	//	meta.accept_view_change = 0;
	//    }

	//    if (meta.accept_view_change != 0) {
	//        meta.view_change_active = set_active_view_change.execute(0);
	//    } else if (meta.accept_view_change == 0) {
	//        meta.view_change_active = 0;
	//    }
	//} else if (hdr.cntrl.isValid()) {
	//    meta.ring_view = hdr.cntrl.ring_view;
        //    meta.accept_view_change = check_cntrl_view.execute(0);
	//    meta.accept_finalize_view = 0;
	//    if (meta.accept_view_change == 1) {
	//        meta.view_change_active = set_active_view_change.execute(0);
	//    } else {
	//        meta.view_change_active = unset_active_view_change.execute(0);
	//        unset_view_leader.execute(0);
	//    }
	//    meta.accept_finalize_view = 0;
	//} else if (hdr.finalize_view.isValid()) {

	//    meta.ring_view = hdr.finalize_view.current_ring_view;
	//    update_view.execute(0); // UNSETS the view change!
	//    meta.view_change_active = 0;
	//    meta.accept_finalize_view = 0;
	//} else {
	//    meta.accept_finalize_view = 0;
	//    meta.ring_view = 0;
	//    meta.accept_view_change = 0;
	//    meta.view_change_active = get_active_view_change.execute(0);
	//}

	// Determine whether control packet should be dropped
	// Mark this packet as being received during an active view change
	// Timestamping
	if (hdr.cntrl.isValid() && meta.view_change_active == 0) {
	    log_view_change_time.execute(1);
	} else if (hdr.trigger_view.isValid() && meta.view_change_active == 1) {
	    log_view_change_time.execute(0);
	}

	// do we drop some packets?
	check_view_change.apply();

	if ((hdr.ring_type.isValid() || hdr.cntrl.isValid()) && hdr.canary.magic != CANARY_VALUE) { // TODO: Put canary in table
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

    Register<bit<32>, bit<16>>(MAX_NUM_QUEUES, 0) queue_cnt; 
    RegisterAction<bit<32>, bit<16>, bit<32>>(queue_cnt) curr_queue_cnt = {
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
	    is_active = 0;
	    output = 0;
	}
    };
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_change_active) get_active_view_change = {
        void apply(inout bit<8> is_active, out bit<8> output) {
	    output = is_active;
	}
    };

    
    Register<bit<32>, bit<1>>(2, 0) view_change_start_time; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(view_change_start_time) log_view_change_time = {
        void apply(inout bit<32> start_time) {
	    start_time = eg_prsr_md.global_tstamp[31:0];
	}
    };

    Register<bit<16>, bit<1>>(1, 0) time_in_pipeline; 
    RegisterAction<bit<16>, bit<1>, bit<16>>(time_in_pipeline) update_pipeline_time = {
        void apply(inout bit<16> time) {
	    time = meta.pipeline_elapsed_time;
	}
    };

    // Tracks how long packet was waiting to be allowed to advance to acknowledgements (part of waiting)
    Register<bit<16>, bit<1>>(1, 0) raw_elapsed_time; 
    RegisterAction<bit<16>, bit<1>, bit<16>>(raw_elapsed_time) update_raw_elapsed_time = {
        void apply(inout bit<16> raw_cntr) {
	    raw_cntr = meta.elapsed_time;
	}
    };

    action update_type() {
	hdr.ring_type.type = TYPE_APPEND_RESP;
    }
    
    table check_duration {
	key = {
            meta.elapsed_time: range;
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
	    hdr.ethernet.type: exact;
	}
	actions = {
	    drop;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }

    action set_ring_process(bit<16> process_type) {
	meta.ring_process = process_type;
    }
    table get_packet_type {
        key = {
            hdr.ring_type.type: exact; // This is being set at the endpoints
        }
        actions = {
	    set_ring_process;
	    NoAction;
        }
        size = 256;
        default_action = NoAction;
    }

    
    // Register keeps track of the current view that the switch/pipeline is currently part of
    Register<bit<16>, bit<1>>(1, 0) egress_current_view;
    RegisterAction<bit<16>, bit<1>, bit<16>>(egress_current_view) trigger_view_change = {
        void apply(inout bit<16> curr_view, out bit<16> output) {
	    if (curr_view <= meta.ring_view) {
	        output = curr_view;
	    } else {
	        output = 0;
	    }
	}
    };
    RegisterAction<bit<16>, bit<1>, bit<16>>(egress_current_view) check_cntrl_view = {
        void apply(inout bit<16> curr_view, out bit<16> output) {
	    if (curr_view == meta.ring_view) {
	        output = 0;
	    } else {
	        output = 1;
	    }
	}
    };
    RegisterAction<bit<16>, bit<1>, bit<16>>(egress_current_view) update_view = {
        void apply(inout bit<16> curr_view) {
	    curr_view = meta.ring_view;
	}
    };


    Register<bit<8>, bit<1>>(1, 0) view_leader;
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_leader) record_view_leader = {
        void apply(inout bit<8> curr_view_leader, out bit<8> output) {
	    if (curr_view_leader == 0) {
	        curr_view_leader = meta.leader_switch_id;
		output = 1;
	    } else {
		output = 0;
	    }
	}
    };
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_leader) unset_view_leader = {
        void apply(inout bit<8> curr_view_leader) {
	    curr_view_leader = 0;
	}
    };

    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) canary_no_match_counter_eg1; 

    apply { 
	eg_dprsr_md.drop_ctl = 0;
        meta.route_to_shard= 0;
        meta.lower_ready_for_ack = 0;
        meta.upper_ready_for_ack = 0;
        meta.ack_ready = 0;

        meta.ring_process = 0;
        meta.num_recirc_ports = 0;
        meta.num_wait_ports = 0;
        meta.switch_id = 0;

        meta.view_hops = 0;

        meta.time_since_last_cntrl = 0; 
        meta.next_hop = 0;
        meta.update_iteration = 0;
        meta.append_g_zero = 0;
        
        meta.cntrl_pkt_it = 0;
        meta.wait_time = 0;
        meta.num_shards = 0;

        meta.dst_addr = 0;
        meta.cnt = 0;
        meta.lower_assign_sn = 0;
        meta.upper_assign_sn = 0;
        meta.local_seq_no = 0;
        meta.upper_global_seq_no = 0;
        meta.lower_global_seq_no = 0;
        meta.batch_size = 0; // For keeping tabs on the GSN table size

        meta.shard_size = 0;
        meta.duration = 0;
        meta.overflow = 0;
        meta.output = 0;
        meta.ack_threshold = 0;
        meta.last_time = 0;

        meta.do_ing_mirroring = 0;
        meta.do_egr_mirroring = 0;
        meta.ing_mir_ses = 0; 
        meta.egr_mir_ses = 9; 
        //meta.pkt_type = PKT_NORMAL;


	// Combine the control packet components
	if (hdr.cntrl.isValid()) {
 	    meta.low_bit = hdr.cntrl.lower_global_seq_no[31:31];
	    hdr.cntrl.lower_global_seq_no = hdr.cntrl.temp_local_seq_no + hdr.cntrl.lower_global_seq_no;
	    if (meta.low_bit == 1 && hdr.cntrl.lower_global_seq_no[31:31] == 0) {
	    	hdr.cntrl.upper_global_seq_no = hdr.cntrl.upper_global_seq_no + 1;
	    }
	    hdr.cntrl.temp_local_seq_no = 0;
	} else if (hdr.append.isValid() && hdr.ring_type.type == TYPE_APPEND_RESP) {
	    meta.low_bit = hdr.append.lower_g_idx[31:31];
	    hdr.append.lower_g_idx = hdr.append.local_seq_cntr + hdr.append.lower_g_idx;
	    if (meta.low_bit == 1 && hdr.append.lower_g_idx[31:31] == 0) {
	    	hdr.append.upper_g_idx = hdr.append.upper_g_idx + 1;
	    }
	    hdr.append.local_seq_cntr = 0;
	}

	if (hdr.ring_type.isValid()) {
            if (eg_dprsr_md.mirror_type != 0) {
                hdr.ring_type.type = TYPE_SUB_RESP;
	    }

	    get_packet_type.apply();
            meta.queue_congest = (bit<32>)(standard_metadata.deq_qdepth);
	    bit<16> queue_indx = (bit<16>)(standard_metadata.egress_port ++ standard_metadata.egress_qid);
            curr_queue_cnt.execute(queue_indx);

	    meta.pipeline_elapsed_time = eg_prsr_md.global_tstamp[15:0] - hdr.ring_type.pipe_ts;
	    update_pipeline_time.execute(0);

	    if (hdr.ring_type.type == TYPE_APPEND_WAIT) {
	        meta.elapsed_time = eg_prsr_md.global_tstamp[15:0] - hdr.ring_type.start_ts;
                check_duration.apply();
	    } 
	} else {
            meta.queue_congest = 0;
            meta.elapsed_time = 0;
	}
			

	// Read, Update, or Check view number info
	// Check whether you should accept the changes in the finalize_view if it's the right leader

	if (hdr.trigger_view.isValid()) {
	    meta.ring_view = hdr.trigger_view.current_ring_view;
            meta.accept_view_change = trigger_view_change.execute(0);
	    hdr.trigger_view.current_ring_view = meta.accept_view_change;
	    meta.accept_finalize_view = 0; 
	    if (meta.accept_view_change != 0) {
	        meta.view_change_active = set_active_view_change.execute(0);
	        meta.leader_switch_id = hdr.trigger_view.leader_switch_id;
	        meta.accept_finalize_view = record_view_leader.execute(0); 
	    }
	} else if (hdr.cntrl.isValid()) {
	    meta.ring_view = hdr.cntrl.ring_view;
            meta.accept_view_change = check_cntrl_view.execute(0);
	    meta.accept_finalize_view = 0;
	    if (meta.accept_view_change != 0) {
	        meta.view_change_active = set_active_view_change.execute(0);
	    } else {
	        meta.view_change_active = unset_active_view_change.execute(0);
	        unset_view_leader.execute(0);
	    }
	} else if (hdr.finalize_view.isValid()) {

	    meta.ring_view = hdr.finalize_view.current_ring_view;
	    update_view.execute(0); // UNSETS the view change!
	    meta.view_change_active = 0;
	    meta.accept_finalize_view = 0;
	} else {
	    meta.ring_view = 0;
	    meta.accept_view_change = 0;
	    meta.accept_finalize_view = 0;
	    meta.view_change_active = get_active_view_change.execute(0);
	}

	// Determine whether control packet should be dropped
	// Mark this packet as being received during an active view change
	// Timestamping
	if (hdr.cntrl.isValid() && meta.view_change_active == 0) {
	    log_view_change_time.execute(1);
	} else if (hdr.trigger_view.isValid() && meta.view_change_active == 1) {
	    log_view_change_time.execute(0);
	}

	// do we drop some packets?
	check_view_change.apply();

	if ((hdr.ring_type.isValid() || hdr.cntrl.isValid()) && hdr.canary.magic != CANARY_VALUE) { // TODO: Put canary in table
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
