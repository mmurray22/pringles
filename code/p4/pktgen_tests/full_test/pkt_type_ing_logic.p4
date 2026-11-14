/*******************************************************************************
 * P4_16: THIS IS NOT A REAL PROGRAM, DO NOT ATTEMPT TO RUN
 * THIS IS FOR IDEATION/DESIGN UNDERSTANDING ONLY
 ******************************************************************************/


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
    action constants(bit<8> switch_id, 
		     bit<32> shard_size,
		     bit<16> number_of_shards,
		     bit<32> ack_threshold,
		     bit<8> num_recirc_ports,
		     bit<8> num_wait_ports,
		     bit<32> view_timeout) {
	meta.port_idx = 0;
	meta.route_to_client = 0;
	meta.recirc_port_type = 0;
	meta.subscribe = 0;
	meta.route_to_shard = 0;
	meta.hops = 0;
	meta.dst_addr = 0;
	meta.ring_process = 0;
	meta.update_iteration = 0;
	meta.append_g_zero = 0;
	meta.ack_ready = 0;
	meta.send_to_control_plane = 0;	
	meta.ring_view = 0;
	meta.accept_view_change = 0;


	meta.switch_id = switch_id;
	meta.shard_size = shard_size;	
	meta.num_shards = number_of_shards;
	meta.ack_threshold = ack_threshold;	
        meta.num_recirc_ports = num_recirc_ports;
        meta.num_wait_ports = num_wait_ports;
        meta.view_timeout = view_timeout;
    }
    @pragma ternary 1 
    table get_constants {
	actions = {
	    constants;
	}
	size = 1;
	default_action = constants(1, 0, 0, 1, 0, 0, 2500);
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
    action route_next_switch(egress_spec_t port) {
        ig_tm_md.ucast_egress_port = port;
    }
    action set_ring_process(bit<16> process_type) {
	meta.ring_process = process_type;
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
        default_action = NoAction;
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
    RegisterAction<bit<16>, bit<1>, bit<16>>(current_view) check_view = {
        void apply(inout bit<16> curr_view, out bit<16> output) {
	    if (curr_view <= hdr.trigger_view.current_ring_view) {
		output = 1;
	    } else {
		output = 0;
	    }
	}
    };


    

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
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) cntrl_counter_ig1; 
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
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) canary_no_match_counter_ig0; 
    Register<bit<8>, bit<1>>(1, 1) true_magic_val;
    RegisterAction<bit<8>, bit<1>, bit<8>>(true_magic_val) write_magic = {
        void apply(inout bit<8> magic_val) {
            magic_val = hdr.canary.magic;
        }
    };
    Register<bit<16>, bit<1>>(1, 0) pesky_ether_type;
    RegisterAction<bit<16>, bit<1>, bit<16>>(pesky_ether_type) read_ether_type = {
        void apply(inout bit<16> ether_type) {
	    if (hdr.ethernet.isValid()) {
                ether_type = hdr.ethernet.ether_type;
	    } else {
	    	ether_type = 0;
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
	    meta.ring_process: exact; // ONLY get_it
            meta.cntrl_to_process: exact;
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

    action batch_add() {
	meta.global_seq_no = hdr.cntrl.global_seq_no;
        hdr.cntrl.global_seq_no = hdr.cntrl.global_seq_no + meta.local_seq_no;
	meta.batch_size = meta.local_seq_no;
	add_batch.execute(0);
    }
    action batch_sub() {
	sub_batch.execute(0);
    }
    table update_batches {
        key = {
            meta.update_iteration: exact;
            meta.cntrl_to_process: exact;
	    meta.ring_process: exact;
        }
        actions = {
            batch_add;
	    batch_sub;
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
            meta.update_iteration: exact;
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
	meta.dst_addr = hdr.ipv4.dst_addr;
    }
    table append_first_tbl {
        key = {
            meta.ring_process: exact;
	    meta.append_g_zero: exact;
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
    table process_gsn {
        key = {
            meta.ring_process: exact;
	    meta.append_g_zero: exact;
	    meta.update_iteration: exact;
            meta.cntrl_to_process: exact;
        }
        actions = {
	    get_ack_readiness;
            make_gsn_zero;
	    assign_gsn;
	    update_gsn;
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
	meta.dst_addr = hdr.ipv4.dst_addr;
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
    action receive_subscription(egress_spec_t port) {
        ig_tm_md.ucast_egress_port = port;
    }
    action send_multicast(bit<16> group_id) { // This does: shard multicast, subscription multicast
        ig_tm_md.mcast_grp_a = group_id;
    }
    action eth_forward(mac_addr_t dst_addr, egress_spec_t port) {
        ig_tm_md.ucast_egress_port = port;
        hdr.ethernet.src_addr = hdr.ethernet.dst_addr;
        hdr.ethernet.dst_addr = dst_addr;
        hdr.ipv4.ttl = hdr.ipv4.ttl - 1;
    }
    action send_to(egress_spec_t port) { // This does: circulate, wait, cntrl, and subscribe to self
        ig_tm_md.ucast_egress_port = port;
    }
    table routing {
        key = {
	    meta.next_hop: exact; // encodes pkt_id
	    meta.route_to_client: exact;
            meta.dst_addr: lpm;
	    meta.recirc_port_type: exact;
	    meta.port_idx: exact; 
	    meta.subscribe: exact;
	    meta.view_change: exact;
	    meta.send_to_control_plane: exact;
	    meta.route_to_shard: exact; // encodes shard_id
	    meta.hops: exact; // send_to if more hops to go, eth_forward to send back to client
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
	    meta.ring_process: exact; // encodes pkt_id
	    meta.cntrl_to_process: exact; // encodes pkt_id
	}
	actions = {
	   get_next_hop;
	   update_next_hop;
	   NoAction;
	}
	size = 64;
	default_action = NoAction;
    }

    action set_trigger_view() {
	hdr.trigger_view.highest_seen_seq_no = meta.assign_sn;
	hdr.trigger_view.view_member_switch_id = meta.switch_id;
	meta.view_hops = hdr.trigger_view.hops;
	hdr.trigger_view.hops = hdr.trigger_view.hops + 1;
    }
    action set_finalize_view() {
	meta.global_seq_no = hdr.finalize_view.highest_quorum_seq_no;
	meta.view_hops = hdr.finalize_view.hops;
	hdr.finalize_view.hops = hdr.finalize_view.hops + 1;
    }
    table view_change_tbl {
	key = {
	    hdr.ethernet.ether_type: exact;
	}
	actions = {
	    set_trigger_view;
	    set_finalize_view;
	    NoAction;
	size = 64;
	default_action = NoAction;
    }

    action get_view_number() { // All other packets (cntrl and ring_type)
	meta.ring_view = get_view.execute(0);
    }
    action update_view_number() { // Finalize View
	update_view.execute(0);
	meta.accept_view_change = 1;
	meta.view_change = 1;
    }
    table ingress_view_tbl {
	key = {
	    hdr.ethernet.ether_type: exact;
	}
	actions = {
	    get_view_number;
	    update_view_number;
	}
	size = 64;
	default_action = get_view_number();
    }

    
    action send_digest() {
	ig_dprsr_md.digest_type = 1;
    }
    table check_induce_view_change {
	key = {
            meta.time_since_last_cntrl: range;
	}
	actions = {
	    send_digest;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }

    Register<bit<32>, bit<1>>(1, 0) last_seen_cntrl_pkt;
    RegisterAction<bit<32>, bit<1>, bit<32>>(last_seen_cntrl_pkt) log_cntrl_pkt_time = {
        void apply(inout bit<32> time) {
	    time = (bit<32>)standard_metadata.ingress_mac_tstamp;
        }
    };
    RegisterAction<bit<32>, bit<1>, bit<32>>(last_seen_cntrl_pkt) get_cntrl_pkt_time = {
        void apply(inout bit<32> time, out bit<32> output) {
	    output = time;
        }
    };

    /* Ingress Pipeline: Control Packet */
    apply {	

	hdr.timer.setInvalid();
	get_constants.apply();

	ingress_view_tbl.apply();	

	if (meta.ring_view != hdr.cntrl.ring_view) {
	    meta.cntrl_to_process = 0;
	} else {
	    meta.cntrl_to_process = 1;
	    cntrl_counter_ig1.count(0);
	}

	cntrl_local_seq.apply();
	if (meta.local_seq_no != 0) {
	    meta.update_iteration = 1;
	}

	fill_cntrl_pkt_it.apply();

	edit_batch_size.apply();
	update_batches.apply();
	process_gsn.apply(); // handles TYPE_APPEND_RESP, handles GSN updates	 // Triggered on a FinalizeView

	next_hop_routing.apply();

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
    Digest<digest_timeout_t>() digest_timeout;

    apply {
	if (ig_dprsr_md.digest_type == 1) {
	    // We have: meta.view_timeout, meta.last_time, and meta.current_time
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
	    meta.recirc_port_type = 0;
            meta.route_to_shard = 0;

            meta.ready_for_ack = 1;
            meta.ack_ready = 0;
            meta.cntrl_to_process = 0;
            meta.route_to_client = 0;

            meta.ring_process = 0; // MAKE 8 bits
            meta.subscribe = 0;
            meta.canary_ok = 0; 
            meta.num_recirc_ports = 0;
            meta.num_wait_ports = 0;
            meta.port_idx = 0;
            meta.hops = 0;
            meta.switch_id = 0;
            meta.view_change_active = 0;
            meta.view_change_prev = 0;
            meta.next_hop = 0;
            
            meta.cntrl_pkt_it = 0;
            meta.wait_time = 0;
            meta.nonce = 0;
            meta.ring_view = 0;
            meta.num_shards = 0;

            meta.dst_addr = 0;
            meta.stream_id = 0;
            meta.cnt = 0;
            meta.assign_sn = 0;
            meta.local_seq_no = 0;
            meta.global_seq_no = 0;
            meta.view_change_start_time = 0;
            meta.batch_size = 0; // For keeping tabs on the GSN table size
            meta.queue_congest = 0;
            meta.shard_size = 0;
            meta.duration = 0;
            meta.overflow = 0;
            meta.output = 0;
            meta.ack_threshold = 0;

            meta.do_ing_mirroring = 0; // Enable ingress mirroring
            meta.do_egr_mirroring = 0; // Enable egress mirroring
            meta.ing_mir_ses = 0; // Ingress mirror session ID
            meta.egr_mir_ses = 0; // Egress mirror session ID
            meta.pkt_type = PKT_TYPE_NORMAL;

            meta.start_ts = 0;
            meta.end_ts = 0;
            meta.egress_end_time = 0;
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
            transition select(hdr.ethernet.ether_type) {
                TYPE_CONTROL: parse_control;
		TYPE_VIEW_INDUCE: parse_induce_view;
		TYPE_VIEW_TRIGGER: parse_trigger_view;
		TYPE_VIEW_CHANGE: parse_start_view;
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

    Register<bit<32>, bit<1>>(1, 0) queue_cnt; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(queue_cnt) curr_queue_cnt = {
        void apply(inout bit<32> new_queue_cnt) {
	    new_queue_cnt = meta.queue_congest;
	}
    };

    Register<bit<8>, bit<1>>(1, 0) view_change_status;  // TODO : Setting the point at which the view change happens
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_change_status) received_start_view = {
        void apply(inout bit<8> is_active, out bit<8> output) {
	    is_active = 1;
	    output = 1;
	}
    };


    Register<bit<8>, bit<1>>(1, 0) view_change_active; 
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_change_active) induce_active_view_change = {
        void apply(inout bit<8> is_active, out bit<8> output) {
	    is_active = 1;
	    output = 1;
	}
    };

    RegisterAction<bit<8>, bit<1>, bit<8>>(view_change_active) started_active_view_change = {
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
            meta.ring_process: exact;
	    hdr.ethernet.ether_type: exact; // TYPE_CNTRL, TYPE_IP
	}
	actions = {
	    drop;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }

    action induce_active_view() { // Trigger View packet, accpet = 1
	meta.view_change_active = induce_active_view_change.execute(0);
    }
    action trigger_active_view() { // Trigger View packet, accpet = 1
	meta.view_change_active = started_active_view_change.execute(0);
    }
    action unset_active_view() { // New next control packet, accept = 0
	meta.view_change_active = unset_active_view_change.execute(0);
    }
    action get_active_view() { // Every other packet, accept = 0
	meta.view_change_active = get_active_view_change.execute(0);
    }
    table update_active_view {
	key = {
	    hdr.ethernet.ether_type: exact;
	    meta.accept_view_change: exact;
	}
	actions = {
	    trigger_active_view;
	    unset_active_view;
	    get_active_view;
	    induce_active_view;
	}
	size = 64;
	default_action = get_active_view();
    }

    action log_end_ts() { // TYPE_CNTRL, accept = 0
	log_view_change_time.execute(1);
    }
    action log_start_ts() { // TYPE_VIEW_TRIGGER, accept = 1
	log_view_change_time.execute(0);
    }
    table view_logistics {
	key = {
	    hdr.ethernet.ether_type: exact;
	    meta.accept_view_change: exact;
	}
	actions = {
	    log_start_ts;
	    log_end_ts;
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

    

    Register<bit<16>, bit<1>>(1, 1) current_view;
    RegisterAction<bit<16>, bit<1>, bit<16>>(current_view) update_view = {
        void apply(inout bit<16> curr_view) {
	    curr_view = meta.ring_view;
	}
    };
    RegisterAction<bit<16>, bit<1>, bit<16>>(current_view) check_view = {
        void apply(inout bit<16> curr_view, out bit<16> output) {
	    if (curr_view <= meta.ring_view) {
		output = 1;
	    } else {
		output = 0;
	    }
	    curr_view = 0;
	}
    };
    RegisterAction<bit<16>, bit<1>, bit<16>>(current_view) check_cntrl_view = {
        void apply(inout bit<16> curr_view, out bit<16> output) {
	    if (curr_view == meta.ring_view) {
		output = 0;
	    } else {
		output = 1;
	    }
	}
    };


    action check_accept_view_change() { // Trigger View
        meta.accept_view_change = check_view.execute(0);
    }
    action check_cntrl_view_change() { // Cntrl View
        meta.accept_view_change = check_cntrl_view.execute(0);
    }
    action update_view_number() { // Finalize View
	update_view.execute(0);
    }
    table get_view_number_info {
	key = {
	    hdr.ethernet.ether_type: exact;
	}
	actions = {
	    check_accept_view_change;
	    check_cntrl_view_change;
	    update_view_number;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }


    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) canary_no_match_counter_eg1; 
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) drop_cntrl_counter_eg1; 

    apply { 
	meta.view_change = 0;
	meta.view_change_status = 0;
	meta.ring_process = 0;
	meta.view_change_active = 0;

	if (eg_dprsr_md.mirror_type != 0) {
            hdr.ring_type.type = TYPE_SUB_RESP;
	}

	meta.egress_end_time = eg_prsr_md.global_tstamp[31:0];
	if (hdr.ring_type.isValid()) {
	    get_packet_type.apply();
            meta.queue_congest = (bit<32>)(standard_metadata.deq_qdepth);
            curr_queue_cnt.execute(0);
	    meta.elapse_time = meta.egress_end_time - hdr.ring_type.pipe_ts;
	    update_pipeline_time.execute(0);
	}

	if (hdr.ring_type.type == TYPE_APPEND_WAIT) {
	    hdr.ring_type.raw_elapsed_time = (bit<16>)(hdr.ring_type.end_ts - hdr.ring_type.start_ts);
            check_duration.apply();
	} 


	
	if (hdr.trigger_view.isValid()) {
	    meta.ring_view = hdr.trigger_view.current_ring_view;
	} else if (hdr.finalize_view.isValid()) {
	    meta.ring_view = hdr.finalize_view.current_ring_view;
	} else if (hdr.cntrl.isValid()) {
	    meta.ring_view = hdr.cntrl.ring_view;
	}



	// are we in a view change?
	get_view_number_info.apply();
	update_active_view.apply();

	// do we log a view timestamp?
	view_logistics.apply();

	// do we drop some packets?
	check_view_change.apply();


	if ((hdr.ring_type.isValid() || hdr.cntrl.isValid()) && hdr.canary.magic != CANARY_VALUE) { // TODO: Put canary in table
	    canary_no_match_counter_eg1.count(0);
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

    Register<bit<32>, bit<1>>(1, 0) queue_cnt; 
    RegisterAction<bit<32>, bit<1>, bit<32>>(queue_cnt) curr_queue_cnt = {
        void apply(inout bit<32> new_queue_cnt) {
	    new_queue_cnt = meta.queue_congest;
	}
    };

    Register<bit<8>, bit<1>>(1, 0) view_change_status;  // TODO : Setting the point at which the view change happens
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_change_status) received_start_view = {
        void apply(inout bit<8> is_active, out bit<8> output) {
	    is_active = 1;
	    output = 1;
	}
    };


    Register<bit<8>, bit<1>>(1, 0) view_change_active; 
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_change_active) induce_active_view_change = {
        void apply(inout bit<8> is_active, out bit<8> output) {
	    is_active = 1;
	    output = 1;
	}
    };
    RegisterAction<bit<8>, bit<1>, bit<8>>(view_change_active) started_active_view_change = {
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
            meta.ring_process: exact;
	    hdr.ethernet.ether_type: exact; // TYPE_CNTRL, TYPE_IP
	}
	actions = {
	    drop;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }

    action induce_active_view() { // Trigger View packet, accpet = 1
	meta.view_change_active = induce_active_view_change.execute(0);
    }
    action trigger_active_view() { // Trigger View packet, accpet = 1
	meta.view_change_active = started_active_view_change.execute(0);
    }
    action unset_active_view() { // New next control packet, accept = 0
	meta.view_change_active = unset_active_view_change.execute(0);
    }
    action get_active_view() { // Every other packet, accept = 0
	meta.view_change_active = get_active_view_change.execute(0);
    }
    table update_active_view {
	key = {
	    hdr.ethernet.ether_type: exact;
	    meta.accept_view_change: exact;
	}
	actions = {
	    trigger_active_view;
	    unset_active_view;
	    get_active_view;
	    induce_active_view;
	}
	size = 64;
	default_action = get_active_view();
    }

    action log_end_ts() { // TYPE_CNTRL, accept = 0
	log_view_change_time.execute(1);
    }
    action log_start_ts() { // TYPE_VIEW_TRIGGER, accept = 1
	log_view_change_time.execute(0);
    }
    table view_logistics {
	key = {
	    hdr.ethernet.ether_type: exact;
	    meta.accept_view_change: exact;
	}
	actions = {
	    log_start_ts;
	    log_end_ts;
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

    

    Register<bit<16>, bit<1>>(1, 1) current_view;
    RegisterAction<bit<16>, bit<1>, bit<16>>(current_view) update_view = {
        void apply(inout bit<16> curr_view) {
	    curr_view = meta.ring_view;
	}
    };
    RegisterAction<bit<16>, bit<1>, bit<16>>(current_view) check_view = {
        void apply(inout bit<16> curr_view, out bit<16> output) {
	    if (curr_view <= meta.ring_view) {
		output = 1;
	    } else {
		output = 0;
	    }
	    curr_view = 0;
	}
    };
    RegisterAction<bit<16>, bit<1>, bit<16>>(current_view) check_cntrl_view = {
        void apply(inout bit<16> curr_view, out bit<16> output) {
	    if (curr_view == meta.ring_view) {
		output = 0;
	    } else {
		output = 1;
	    }
	}
    };


    action check_accept_view_change() { // Trigger View
        meta.accept_view_change = check_view.execute(0);
    }
    action check_cntrl_view_change() { // Cntrl View
        meta.accept_view_change = check_cntrl_view.execute(0);
    }
    action update_view_number() { // Finalize View
	update_view.execute(0);
    }
    table get_view_number_info {
	key = {
	    hdr.ethernet.ether_type: exact;
	}
	actions = {
	    check_accept_view_change;
	    check_cntrl_view_change;
	    update_view_number;
	    NoAction;
	}
	size = 64;
	default_action = NoAction;
    }


    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) canary_no_match_counter_eg1; 
    Counter<bit<32>, bit<1>>(1, CounterType_t.PACKETS) drop_cntrl_counter_eg1; 

    apply { 
	meta.view_change = 0;
	meta.view_change_status = 0;
	meta.ring_process = 0;
	meta.view_change_active = 0;

	if (eg_dprsr_md.mirror_type != 0) {
            hdr.ring_type.type = TYPE_SUB_RESP;
	}

	meta.egress_end_time = eg_prsr_md.global_tstamp[31:0];
	if (hdr.ring_type.isValid()) {
	    get_packet_type.apply();
            meta.queue_congest = (bit<32>)(standard_metadata.deq_qdepth);
            curr_queue_cnt.execute(0);
	    meta.elapse_time = meta.egress_end_time - hdr.ring_type.pipe_ts;
	    update_pipeline_time.execute(0);
	}

	if (hdr.ring_type.type == TYPE_APPEND_WAIT) {
	    hdr.ring_type.raw_elapsed_time = (bit<16>)(hdr.ring_type.end_ts - hdr.ring_type.start_ts);
            check_duration.apply();
	} 


	
	if (hdr.trigger_view.isValid()) {
	    meta.ring_view = hdr.trigger_view.current_ring_view;
	} else if (hdr.finalize_view.isValid()) {
	    meta.ring_view = hdr.finalize_view.current_ring_view;
	} else if (hdr.cntrl.isValid()) {
	    meta.ring_view = hdr.cntrl.ring_view;
	}



	// are we in a view change?
	get_view_number_info.apply();
	update_active_view.apply();

	// do we log a view timestamp?
	view_logistics.apply();

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
