################################################################################
# BAREFOOT NETWORKS CONFIDENTIAL & PROPRIETARY
#
# Copyright (c) 2019-present Barefoot Networks, Inc.
#
# All Rights Reserved.
#
# NOTICE: All information contained herein is, and remains the property of
# Barefoot Networks, Inc. and its suppliers, if any. The intellectual and
# technical concepts contained herein are proprietary to Barefoot Networks, Inc.
# and its suppliers and may be covered by U.S. and Foreign Patents, patents in
# process, and are protected by trade secret or copyright law.  Dissemination of
# this information or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Barefoot Networks, Inc.
#
# No warranty, explicit or implicit is provided, unless granted under a written
# agreement with Barefoot Networks, Inc.
#
################################################################################
import threading
import logging
import socket 
import time
import copy
import random
import yaml
import json
from multiprocessing import Process
import ptf
import ptf.dataplane as dataplane
from ptf import config
import ptf.testutils as testutils
from bfruntime_client_base_tests import BfRuntimeTest
import pltfm_pm_rpc as pltfm_pm
import bfrt_grpc.client as gc
import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2
from ptf.thriftutils import *

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../controlplane_libs")))
from headers import *
from portsetup import *
from tables import * 
from view_change import *


p4_program_name = "pktgen"#"viewchange" # TODO make parameter
g_timer_app_id = 1
global_group_id = 0
g_num_pipes = int(testutils.test_param_get("num_pipes"))
listen_ip = "0.0.0.0" # Listen on all interfaces
listen_port = 5005
CONST_MAC_DST = "00:90:fb:70:65:71"
CONST_MAC_SRC = "00:25:90:53:e6:00"
CONST_IP = "10.229.49.9"
pkt_gen_app_id = 0
timeout_app_id = 2
cpu_port = 192

# List of Types:
# 0 = Dummy 0x0
# 1 = Append 0x0860 
# 2 = Append_Wait 0x0862 
# 3 = Append_Resp 0x0861 
# 4 = Read 0x0870 
# 5 = Read_Resp 0x0871
# 6 = Tail 0x0840
# 7 = Sub 0x0850
# 8 = Sub_Resp 0x0851
# 9 = View_Change 0x0900
# 10 = View_Ack 0x0910
list_of_types = [0x0, 0x0860, 0x0862, 0x0861, 0x0870, 0x0871, 0x0840, 0x0850, 0x0851, 0x0900, 0x910]
#list_of_types = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


# Initialize the logger
logger = logging.getLogger('ViewChangeTest')
if not len(logger.handlers):
    logger.addHandler(logging.StreamHandler())

class ViewChangeTest(BfRuntimeTest):
    def load_config(self, file_path):
        with open(file_path, 'r') as file:
            try:
                # safe_load prevents execution of arbitrary code in YAML files
                config = yaml.safe_load(file)
                return config
            except yaml.YAMLError as exc:
                logger.info("Error parsing YAML: ", exc)
    
    def setUp(self):
        client_id = 0
        BfRuntimeTest.setUp(self, client_id, p4_program_name)
    
    ###################### PORT SETUP ##############################
    def setup_all_switch_ports(self, target, pipes_cfg, port_speed, port_fec, portSetup):
        loop_ports = set()
        no_loop_ports = set()
        for pipe_cfg in pipes_cfg:
            for port in pipe_cfg['recirc_ports']:
                loop_ports.add(port)
            for port in pipe_cfg['wait_ports']:
                loop_ports.add(port)
            for i in range(0, len(pipe_cfg['physical_ports'])):
                port = pipe_cfg['physical_ports'][i]
                if i == self.physical_switch_id:
                    loop_ports.add(port)
                else:
                    no_loop_ports.add(port)
            for port in pipe_cfg['switch_ports']:
                no_loop_ports.add(port)
            loop_ports.add(pipe_cfg['port_to_pipe_0'])
        logger.info(loop_ports)
        logger.info(no_loop_ports)
        logger.info(port_speed)
        logger.info(port_fec)
	for i in loop_ports:
	    portSetup.port_setup(target, i, True, port_speed, port_fec)
        for i in no_loop_ports:
	    portSetup.port_setup(target, i, False, port_speed, port_fec)
	portSetup.store_all_ports(loop_ports, no_loop_ports)

    ###################### CONTROL PLANE RUNTIME ##############################3
    def pgen_timer_hdr_to_dmac(self, pipe_id, app_id, batch_id, packet_id): #### TODO: DELETE
        """
        Given the fields of a 6-byte packet-gen header return an Ethernet MAC address
        which encodes the same values.
        """
        pipe_shift = 3
        return '%02x:00:%02x:%02x:%02x:%02x' % ((pipe_id << pipe_shift) | app_id,
                                                batch_id >> 8,
                                                batch_id & 0xFF,
                                                packet_id >> 8,
                                                packet_id & 0xFF)
    def pgen_port(self, pipe_id, pipe_local_port_idx): ###### TODO: DELETE
        """
        Given a pipe return a port in that pipe which is usable for packet
        generation.  Note that Tofino allows ports 68-71 in each pipe to be used for
        packet generation while Tofino2 allows ports 0-7.  This example will use
        either port 68 or port 6 in a pipe depending on chip type.
        """
        list_pgen_ports = [68, 69, 70, 71]
        pipe_local_port = list_pgen_ports[pipe_local_port_idx]
        return self.make_port(pipe_id, pipe_local_port)
    
    def make_port(self, pipe, local_port): #### TODO : DELETE
        return (pipe << 7) | local_port
    
    def setup_timer_pkt_gen(self, bfrt_info, target, dstAddr, srcAddr, ip_addr, payload_size, cpu_interface, duration, nsperpkt, pipe_id, exp_type, switch_id):
        logger.info("=============== Testing Packet Generator trigger by Timer ===============")
        pktgen_app_cfg_table = bfrt_info.table_get("$PKTGEN_APPLICATION_CFG")
        pktgen_pkt_buffer_table = bfrt_info.table_get("$PKTGEN_PKT_BUFFER")
        pktgen_port_cfg_table = bfrt_info.table_get("$PKTGEN_PORT_CFG")

        # timer pktgen app_id = 1 one shot 0
        app_id = pkt_gen_app_id
        nonce = 33
        payload = "P" * payload_size
        packed_ip = socket.inet_aton(ip_addr)
        ip_int = struct.unpack("!I", packed_ip)[0]

        # TODO: Arbitrary values in UDP, Append headers?
	p = Ether(dst=dstAddr, src=srcAddr, type=TYPE_IP)/ \
            IP(dst=ip_addr)/ \
            UDP(dport=1234, sport=5678)/ \
	    RingType(type=TYPE_APPEND_PRE, 
                     shard_id=0,
                     pipe_ts=0,
                     start_ts=0)/ \
            Append(stream_id=0,
                   upper_g_idx=0,
                   lower_g_idx=0,
                   local_seq_cntr=0,
                   cntrl_pkt_it=0,
                   exp_type=exp_type,
                   seq_recirc=0,
                   start_ts=0,
                   payload_size=payload_size)/ \
            Canary(magic=165)/ \
            Raw(load=payload)
        p.show()
        raw_p = bytes(p)
        pktlen = len(raw_p) #92 bytes: ethernet + ip + udp + ring_type + append
        logger.info("Length of the packet being sent is: {}".format(pktlen))

        pgen_pipe_id = pipe_id
        src_port = self.pgen_port(pgen_pipe_id, 0)
        # B x P to emulate 100G of client traffic
        p_count = 1 #132 #99 # packets per batch
        b_count = 1 # batch number
        buff_offset = 144 + (0 * ((pktlen + 15) // 16) * 16) 
        out_port = 192
        try:
            logger.info("configure forwarding table")
            # Enable packet generation on the port
            logger.info("enable pktgen port")
            pktgen_port_cfg_table.entry_add(
                target,
                [pktgen_port_cfg_table.make_key([gc.KeyTuple('dev_port', src_port)])],
                [pktgen_port_cfg_table.make_data([gc.DataTuple('pktgen_enable', bool_val=True)])])

            # Configure the packet generation timer application
            logger.info("configure pktgen application")
            data = pktgen_app_cfg_table.make_data([gc.DataTuple('timer_nanosec', nsperpkt),
                                                   gc.DataTuple('app_enable', bool_val=False),
                                                   gc.DataTuple('pkt_len', pktlen),
                                                   gc.DataTuple('pkt_buffer_offset', buff_offset),
                                                   gc.DataTuple('pipe_local_source_port', (src_port & 0x7F)),
                                                   gc.DataTuple('increment_source_port', bool_val=False),
                                                   gc.DataTuple('batch_count_cfg', b_count-1),
                                                   gc.DataTuple('packets_per_batch_cfg', p_count-1),
                                                   gc.DataTuple('ibg', 0),
                                                   gc.DataTuple('ibg_jitter', 0),
                                                   gc.DataTuple('ipg', 0),
                                                   gc.DataTuple('ipg_jitter', 0),
                                                   gc.DataTuple('batch_counter', 0),
                                                   gc.DataTuple('pkt_counter', 0),
                                                   gc.DataTuple('trigger_counter', 0)],
                                                  '$PKTGEN_TRIGGER_TIMER_PERIODIC') # PERIODIC ONE_SHOT
            pktgen_app_cfg_table.entry_add(
                target,
                [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', app_id)])],
                [data])
            logger.info("configure packet buffer")
            pktgen_pkt_buffer_table.entry_add(
                target,
                [pktgen_pkt_buffer_table.make_key([gc.KeyTuple('pkt_buffer_offset', buff_offset),
                                                   gc.KeyTuple('pkt_buffer_size', pktlen)])],
                [pktgen_pkt_buffer_table.make_data([gc.DataTuple('buffer', raw_p)])])
        except gc.BfruntimeRpcException as e:
            logger.info(e)
            raise e
        finally:
            pass
    
    def enable_timer_pkt_gen(self, bfrt_info, target):
        logger.info("=============== Testing Packet Generator trigger by Timer ===============")
        pktgen_app_cfg_table = bfrt_info.table_get("$PKTGEN_APPLICATION_CFG")

        # timer pktgen app_id = 1 one shot 0
        app_id = pkt_gen_app_id
        try:
            logger.info("enable pktgen")
            pktgen_app_cfg_table.entry_mod(
                target,
                [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', app_id)])],
                [pktgen_app_cfg_table.make_data([gc.DataTuple('app_enable', bool_val=True)],
                                                 '$PKTGEN_TRIGGER_TIMER_PERIODIC')]
            )
        except gc.BfruntimeRpcException as e:
            logger.info(e)
            raise e
        finally:
            pass
    
    def disable_timer_pkt_gen(self, bfrt_info, target):
        logger.info("=============== Testing Packet Generator trigger by Timer ===============")
        pktgen_app_cfg_table = bfrt_info.table_get("$PKTGEN_APPLICATION_CFG")

        # timer pktgen app_id = 1 one shot 0
        app_id = pkt_gen_app_id
        try:
            logger.info("disable pktgen")
            pktgen_app_cfg_table.entry_mod(
                target,
                [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', app_id)])],
                [pktgen_app_cfg_table.make_data([gc.DataTuple('app_enable', bool_val=False)],
                                                 '$PKTGEN_TRIGGER_TIMER_PERIODIC')]
            )
        except gc.BfruntimeRpcException as e:
            logger.info(e)
            raise e
        finally:
            pass
    

    def get_counter_number(self, bfrt_info, counter_name):
        counter = bfrt_info.table_get(counter_name)
        # 3. Request the entry at index 0
	# 'from_hw=True' ensures you get the latest count from the ASIC registers, 
	# not a cached software value.
	resp = counter.entry_get(
	    self.target,
	    [counter.make_key([gc.KeyTuple('$COUNTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	
	# 4. Parse the response
        try:
	    data, _ = next(resp)
	    data_dict = data.to_dict()
	    
	    # Tofino counters usually return a dictionary with '$COUNTER_SPEC_PKTS'
	    return data_dict['$COUNTER_SPEC_PKTS']
        except gc.BfruntimeRpcException as e:
            logger.info(e)
            return 0

    def get_register_number(self, bfrt_info, target, register_name):
        reg_table = bfrt_info.table_get(register_name)
	table_resp = reg_table.entry_get(
	    target,
	    [reg_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	reg_data, _ = next(table_resp)
	reg_data_dict = reg_data.to_dict()
        register_entry_name = register_name + ".f1"
        return reg_data_dict[register_entry_name][0]
   
    def get_register_number_at_idx(self, bfrt_info, target, register_name, idx):
        reg_table = bfrt_info.table_get(register_name)
	table_resp = reg_table.entry_get(
	    target,
	    [reg_table.make_key([gc.KeyTuple('$REGISTER_INDEX', idx)])],
	    {"from_hw": True}
	)
	reg_data, _ = next(table_resp)
	reg_data_dict = reg_data.to_dict()
        register_entry_name = register_name + ".f1"
        return reg_data_dict[register_entry_name][0]
   

    # Note: It seems that in order to insert a register entry, you necessarily must taret 
    # the entire device, but to just read from a register, you can just read from that particular
    # pipe
    def insert_register_entry(self, bfrt_info, register_name, register_idx, register_val):
        print(register_name)
        reg_table = bfrt_info.table_get(register_name)
        register_entry_name = register_name + ".f1"
        print(register_entry_name)
        reg_table.entry_add(
            self.target,
            [reg_table.make_key([gc.KeyTuple('$REGISTER_INDEX', register_idx)])],
            [reg_table.make_data([gc.DataTuple(register_entry_name, register_val)])])
        reg_table.operations_execute(self.target, 'Sync')

    def get_full_register(self, bfrt_info, target, register_name):
        reg_table = bfrt_info.table_get(register_name)
	table_resp = reg_table.entry_get(
	    target,
	    [reg_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	reg_data, _ = next(table_resp)
	reg_data_dict = reg_data.to_dict()
        register_entry_name = register_name + ".f1"
        return reg_data_dict[register_entry_name]

    def maintain_view_change(self, viewChangeMaintainer, bfrt_info, target, pipe_id, duration, start_view, switch_id):
        # View Change
        self.response_thread = threading.Thread(target=viewChangeMaintainer.collect_responses, args=())
        self.response_thread.start()
        print("===============Thread: Maintain view change!!! View timeout is: {}".format(self.view_timeout))
        cntr_name = "MyIngress{}.cntrl_counter_ig{}".format(pipe_id, pipe_id)
        pkt_it_reg_name = "MyIngress{}.cntrl_pkt_it".format(pipe_id)
	lower_register_name = "MyIngress{}.map_from_lower_gsn".format(pipe_id)
       	upper_register_name = "MyIngress{}.map_from_upper_gsn".format(pipe_id)
	drop_table = bfrt_info.table_get("MyEgress{}.check_view_change".format(pipe_id))
	key = [drop_table.make_key([gc.KeyTuple('meta.view_change_active', 0),
	                            gc.KeyTuple('hdr.ethernet.type', TYPE_CONTROL)])]

        start_time = time.time() # TODO make sure you keep track of the units here
        experiment_time = time.time()

    	prev_cntrl_pkt_cnt = self.get_counter_number(bfrt_info, cntr_name)
        prev_view = start_view
	injected_trigger = False
        vc_ts = {}
        vc_ts['rounds'] = 0
	restore_t = None
	t_restore_start = 0 
	poll_interval = 0.05
	stall_threshold = 0.05 #0.03
        sleep_time = self.view_timeout

        while True:
            time.sleep(sleep_time)
            if (time.time() - experiment_time) > duration: # If the experiment finishes, return
                break
	    
	    if (not injected_trigger) and (time.time() - experiment_time) > self.trigger_wait_time and pipe_id == self.view_change_pipe and switch_id == self.view_change_leader and self.do_view_change:
	    	injected_trigger = True
	    	vc_ts['inject_start'] = time.time()  # ADDED: start time of the table add, for the log line
		add_t = threading.Thread(target=drop_table.entry_add, args=(target, key, [drop_table.make_data([], "MyEgress{}.drop".format(pipe_id))]))  # ADDED: run the existing entry_add call in a helper thread so it can be bounded
		add_t.daemon = True  # ADDED: never block process exit on this thread
		add_t.start()  # ADDED: start the table add
		add_t.join(1.0)  # ADDED: wait at most 1 s for the add (a gRPC call cannot be cancelled; on a timeout we just stop waiting)
                t0 = time.time()
                vc_ts['inject_end'] = t0
		if add_t.is_alive():  # ADDED: the add did not finish within the bound
		    print("[inject] drop-entry add STILL RUNNING after 1000 ms (not cancelled)")  # ADDED: log the timeout
		else:
		    print("[inject] drop-entry add returned after %.1f ms (a traceback above means it failed, e.g. 'Already exists')" % ((time.time() - t0) * 1e3))  # ADDED: log how long the add took; the thread prints its own traceback on error
		    start_time = time.time()
        
    	    cntrl_pkt_cnt = self.get_counter_number(bfrt_info, cntr_name)
            #print("Previously I received {} cntrl packets and now received {} cntrl packets".format(prev_cntrl_pkt_cnt, cntrl_pkt_cnt))
            if (prev_cntrl_pkt_cnt == cntrl_pkt_cnt) and (time.time() - start_time) > stall_threshold:
	        t_stall = time.time()
                vc_ts['stall'] = t_stall
		if injected_trigger and restore_t is None and switch_id == self.view_change_leader:
		    restore_t = threading.Thread(target=drop_table.entry_del, args=(target, key))
		    restore_t.daemon = True
		    t_restore_start = time.time()
		    restore_t.start()
	        curr_view = self.get_register_number(bfrt_info, target, "MyEgress{}.egress_current_view".format(pipe_id))
		print("[vc-timeline] pipe %d +%6.1f ms  read egress_current_view" % (pipe_id, (time.time() - t_stall) * 1e3))
                if prev_view < curr_view:
                    prev_view = curr_view
                    start_time = time.time()
                    continue

	        view_leader_set = self.get_register_number(bfrt_info, target, "MyEgress{}.view_leader".format(pipe_id))
		pkt_it = self.get_register_number_at_idx(bfrt_info, target, pkt_it_reg_name, 0)
		raw_it = pkt_it
                if pkt_it > 0:
                    pkt_it = pkt_it - 1
		else:
		    pkt_it = 65534 # 2^16 - 1
		print("[stall?] pipe %d: control counter %d -> %d unchanged for %.0f ms | view %d (register %d) | view_leader %d | iteration %d" % (pipe_id, prev_cntrl_pkt_cnt, cntrl_pkt_cnt, (time.time() - start_time) * 1e3, prev_view, curr_view, view_leader_set, raw_it))
		time.sleep(0.02)
		if self.get_register_number_at_idx(bfrt_info, target, pkt_it_reg_name, 0) != raw_it:
		    print("[stall?] pipe %d: FALSE ALARM, the token iteration advanced; no Triggers sent" % pipe_id)
		    prev_cntrl_pkt_cnt = self.get_counter_number(bfrt_info, cntr_name)
		    start_time = time.time()
		    continue


		propose_new_view = prev_view + 1


		if switch_id != self.view_change_leader: # the NON leader is the first to get wind of necessary view change
		    if view_leader_set == 0:
		        print("====================================== Packet iteration: {}".format(pkt_it))
		        viewChangeMaintainer.send_start_view_change(0, 0, prev_view)
                        vc_ts['trigger_sent'] = time.time()
			print("[vc-timeline] pipe %d +%6.1f ms  non-leader sent the Triggers" % (pipe_id, (time.time() - t_stall) * 1e3))
		    start_time = time.time()
		    continue
	  
	  	# If this IS the view change leader, then it continues....
		external_trigger = 1 if view_leader_set != 0 else 0
		timeout = 1 - external_trigger
		#GATE THIS - ONLY THE VIEW CHANGE LEADER CAN EXECUTE THIS!!!! EVERYONE ELSE ONLY EXECUTE SOLE TRIGGER
                (new_view_num, new_start_seq_no) = viewChangeMaintainer.run_view_change(bfrt_info, target, pkt_it, duration, pipe_id, propose_new_view, prev_view, timeout, external_trigger)
                vc_ts['rounds'] = vc_ts['rounds'] + 1
                vc_ts['leader_trigger_sent'] = viewChangeMaintainer.trigger_sent
                vc_ts['trigger_wait_end'] = viewChangeMaintainer.trigger_wait_end
                vc_ts['last_trigger_reply'] = viewChangeMaintainer.last_trigger_reply
                vc_ts['finalize_sent'] = viewChangeMaintainer.finalize_sent
                vc_ts['finalize_wait_end'] = viewChangeMaintainer.finalize_wait_end
                vc_ts['last_finalize_reply'] = viewChangeMaintainer.last_finalize_reply


                if new_view_num == 0: # The View Change FAILED
		    start_time = time.time()
                    continue

		if new_view_num > prev_view:
		    prev_view = new_view_num
		    if restore_t is not None:
		        restore_t.join(1.0)
			if restore_t.is_alive():
			    print("[restore] WARNING: the drop-entry delete is STILL running after 2000 ms; injecting anyway, this run may be tainted")
			else:
			    print("[restore] drop-entry delete finished %.1f ms after it was started at the stall decision (a traceback above means there was no entry to delete)" % ((time.time() - t_restore_start) * 1e3))
                            vc_ts['restore_joined'] = time.time()
		    print("[vc-timeline] pipe %d +%6.1f ms  restore joined" % (pipe_id, (time.time() - t_stall) * 1e3))

		    self.send_cntrl_packet(self.cpu_interface, CONST_MAC_DST, CONST_MAC_SRC, 0, prev_view, new_start_seq_no)
                    vc_ts['token_sent'] = time.time()
		    print("[vc-timeline] pipe %d +%6.1f ms  TOKEN SENT: host-clock total from the stall decision to the new token" % (pipe_id, (time.time() - t_stall) * 1e3))
                    register_name = "MyEgress{}.view_change_start_time".format(pipe_id)
		    print(register_name)
        	    view_change_start_time = self.get_register_number_at_idx(bfrt_info, target, register_name, 0)
        	    view_change_end_time = self.get_register_number_at_idx(bfrt_info, target, register_name, 1)
		    print("=============== Start time view change: {}ns and end time view change: {}ns".format(view_change_start_time, view_change_end_time))
                    elapsed_time = 0
		    if view_change_end_time < view_change_start_time:
		    	view_change_end_time = (1 << 32) + view_change_end_time
                    elapsed_time = view_change_end_time - view_change_start_time
		    print("New view change end time: {}".format(view_change_end_time))	
		    print("=============== View change elapsed time: {}ns".format(elapsed_time))
                    self.view_change_duration = elapsed_time
                    start_time = time.time()
            elif prev_cntrl_pkt_cnt != cntrl_pkt_cnt:
                prev_cntrl_pkt_cnt = cntrl_pkt_cnt
                start_time = time.time()
	    continue
        self.response_thread.join()
        self.view_change_time_breakdown[pipe_id] = vc_ts

    ######################################## Packet Gen Stats ###################################
    def get_final_pktgen_stats(self, bfrt_info, target, duration, nsperpkt, num_recircs, pipe_id, num_pipelines, payload_size, exp_type, lower_time_bound, upper_time_bound, time_interval_ms):
        # 1. Get a reference to the counter table
        pkts = 0
        pkts_tput = 0
        logger.info("Here is the actual pipe id: {}".format(pipe_id))

        ############### Number of total packets TODO this should be DELETED!
        target_dest = "/root/pipe{}_{}_nsperpkt.json".format(pipe_id, nsperpkt)
        telemetry_payload = {
            #"total_number_of_packets": int(cnt_pkts),
            #"throughput_pps": float(pkts_tput),
            #"total_latency_ns": float(total_latency_ns),
            #"average_latency_us": avg_lat,
            "final_committed_pkts": int(self.final_pkt_count[pipe_id]),
            "final_committed_seq_no": int(self.final_committed_seq_no[pipe_id]),
            "start_seed_seq_no": int(self.seed),
            "final_agg_lat": int(self.final_total_latency[pipe_id]),
            "final_elapsed_time": self.final_elapsed_tput_time,
            "nsperpkt": int(nsperpkt),
            "duration": int(self.final_elapsed[pipe_id]),
            "total_loopback": int(num_recircs),
            "num_pipelines": int(num_pipelines),
            "payload_size": int(payload_size),
            "queue_count_timeseries": self.queue_cnt_over_time[pipe_id],
            "control_packet_count_timeseries": self.cntrl_pkt_cnt_over_time[pipe_id],
            "queue_timestamps": self.queue_timestamps,
            "agg_lat_timeseries": self.avg_lat_over_time[pipe_id], # ACTUALLY aggregate latency
            "agg_seq_recirc_timeseries": self.agg_seq_recirc_cntr[pipe_id],
            "committed_pkts_timeseries": self.pkts_over_time[pipe_id],
            "pktgen_pkts_timeseries": self.pktgen_pkt_counters[pipe_id],
            "pipeline_time_timeseries": self.pipeline_time[pipe_id],
            "personal_timestamps": self.pipe_timestamps[pipe_id],
            "global_timestamps": self.timestamps,
            "tput_jitter": self.tput_jitter[pipe_id],
            "pktgen_jitter": self.pktgen_jitter[pipe_id],
            "exp_type": exp_type,
            "lower_time_bound": lower_time_bound,
            "upper_time_bound": upper_time_bound,
            "pipe_id": pipe_id,
            "timeseries_interval_ms": time_interval_ms,
            "do_view_change": int(self.do_view_change),
            "view_change_duration": self.view_change_duration, # TODO THIS IS MISLEADING!!!
            "view_change_breakdown": self.view_change_time_breakdown[pipe_id]
            #"baseline_pkts": self.baseline_pkts[pipe_id],
            #"baseline_curr_time": self.baseline_curr_time[pipe_id],
            #"baseline_total_latency": self.baseline_total_latency[pipe_id],
            #"timestamp_start_epoch": self.timestamp_start_epoch,

        }
        self.write_experiment_telemetry(target_dest, telemetry_payload)
     
    def write_experiment_telemetry(self, file_path, telemetry_payload):
        """
        Serializes benchmarking telemetry safely into a standardized JSON payload structure.
        """
        # 2. Open and write out using a safe with context block
        try:
            with open(file_path, 'w') as json_file:
                # indent=4 formats the output nicely for human-readable debugging
                json.dump(telemetry_payload, json_file, indent=4)
            logger.info("[INFO] Telemetry footlogger.info written successfully to {}".format(file_path))
        except IOError as e:
            logger.info("[ERROR] Failed writing metrics to destination layout: {}".format(e))
    
    def send_cntrl_packet(self, interface, dstAddr, srcAddr, seq_no, view, start_seq_no):
        upper_seq_no = (start_seq_no >> 32) & 0xFFFFFFFF
        lower_seq_no = start_seq_no & 0xFFFFFFFF
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_CONTROL)/ \
              Cntrl(upper_global_seq_no=upper_seq_no, lower_global_seq_no=lower_seq_no, temp_local_seq_no=0, ring_view=view)/ \
              Canary(magic=165)
        #logger.info("CONTROL PACKET WE ARE SENDING:")
        #pkt.show()
        sendp(pkt, iface=interface, verbose=True)

    ######################################## Per Pipe Operations ###################################
    def pipe_queue_monitor(self, bfrt_info, targets, active_pipes, duration, queue_idx_map, queues_of_interest):
        start_time = time.time()
	while (time.time() - start_time) < duration:
            global_timestamp = time.time()
            self.queue_timestamps.append(global_timestamp)
            for pipe_id in range(2):
                if pipe_id not in active_pipes:
                    continue
                target = targets[pipe_id]
                q_name = "MyEgress{}.queue_cnt".format(pipe_id)
                for idx in queues_of_interest[pipe_id]:
                    q_res = self.get_register_number_at_idx(bfrt_info, target, q_name, idx)
                    #logger.info("[IN PROGRESS] Num of packets in the egress queue for port {} and queue id {} : {}".format(queue_idx_map[idx][0], queue_idx_map[idx][1], q_res))
                    self.queue_cnt_over_time[pipe_id][queue_idx_map[pipe_id][idx][0]].append(q_res)
	    time.sleep(self.time_interval_s)

    # Runtime per pipe maintenance which collects data from each pipe and manages the view change
    def pipe_pktgen_runtime(self, bfrt_info, targets, duration, active_pipes):
        pktgen_app_cfg_table = bfrt_info.table_get("$PKTGEN_APPLICATION_CFG")
        first = [True, True]
        extra_telemetry = False
        failed = False
        disable = True

        start_time = time.time()
        while (time.time() - start_time) < duration:
            #if (time.time() - start_time) > 5 and disable:
            #    self.disable_timer_pkt_gen(bfrt_info, target)
            #    disable = False
            global_timestamp = time.time()
            for pipe_id in range(2):
                if pipe_id not in active_pipes:
                    continue
                target = targets[pipe_id]
                tot_cnt_name = "MyIngress{}.tot_packet_counter{}".format(pipe_id, pipe_id)
                low_lat_reg_name = "MyIngress{}.latency_lower".format(pipe_id)
                high_lat_reg_name = "MyIngress{}.latency_higher".format(pipe_id)
                seq_recirc_name = "MyIngress{}.seq_wait_recirc_counter".format(pipe_id)
                pipe_name = "MyEgress{}.time_in_pipeline".format(pipe_id)
                cntrl_cnt_reg_name = "MyIngress{}.cntrl_counter_ig{}".format(pipe_id, pipe_id)
		low_rep_reg_name = "MyIngress{}.highest_lower_replicated_seq_no".format(pipe_id)
		high_rep_reg_name = "MyIngress{}.highest_upper_replicated_seq_no".format(pipe_id)
                if extra_telemetry:
                    canary_drop_cnt_name = "MyEgress{}.canary_no_match_counter_eg{}".format(pipe_id, pipe_id)
                    canary_drop_cnt = self.get_counter_number(bfrt_info, canary_drop_cnt_name)
                    logger.info("[IN PROGRESS] Egress {} Canary Drop count: {}".format(pipe_id, canary_drop_cnt))
                    vc_time_start_name = "MyEgress{}.view_change_start_time".format(pipe_id)
                    vc_end_time = self.get_register_number_at_idx(bfrt_info, target, vc_time_start_name, 1)
                    logger.info("[IN PROGRESS] Egress {} End timestamp: {}".format(pipe_id, vc_end_time))
	            view_register_name = "MyEgress{}.egress_current_view".format(pipe_id)
                    output = self.get_register_number(bfrt_info, target, view_register_name)
                    print("For pipe {}, new value of {}: {}".format(pipe_id, view_register_name, output))
                    view_register_name = "MyEgress{}.view_change_active".format(pipe_id)
                    output = self.get_register_number(bfrt_info, target, view_register_name)
                    print("For pipe {}, new value of {}: {}".format(pipe_id, view_register_name, output))
	            pkt_it_reg_name = "MyIngress{}.cntrl_pkt_it".format(pipe_id)
                    pkt_it = self.get_register_number_at_idx(bfrt_info, target, pkt_it_reg_name, 0)
                    print("Pkt it: {}".format(pkt_it))
                
                avg_lat = 0
                rate_pkt_prod = None
                low_lat = 0
                high_lat = 0
                pkts = 0
                pkt_value = 0
                tri_value = 0
                batch_value = 0
                tput_jitter = 0
                pktgen_jitter = 0
                t_pg = 0
                pkt_value = 0
                try:
                    # Packet generation rate, timestamps
                    resp = pktgen_app_cfg_table.entry_get(
                        target,
                        [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', pkt_gen_app_id)])],
                        {"from_hw": True},
                        pktgen_app_cfg_table.make_data([gc.DataTuple('pkt_counter')], '$PKTGEN_TRIGGER_TIMER_PERIODIC', get=True)
                    )
                    t_pg = time.time()
                    data_dict = next(resp)[0].to_dict()
                    pkt_value = data_dict["pkt_counter"]
                    t_pg_end = time.time()
                    pkts = self.get_counter_number(bfrt_info, tot_cnt_name)
                    now = time.time()

                    low_lat = self.get_register_number(bfrt_info, target, low_lat_reg_name)
                    high_lat = self.get_register_number(bfrt_info, target, high_lat_reg_name)
                    pipe_res = self.get_register_number(bfrt_info, target, pipe_name)
                    seq_recirc_cntr = self.get_register_number(bfrt_info, target, seq_recirc_name)
                    cntrl_cnt = self.get_counter_number(bfrt_info, cntrl_cnt_reg_name)
                    logger.info("[IN PROGRESS] Ingress {} Control packet count: {}".format(pipe_id, cntrl_cnt))
		    upper_commit_seq = self.get_register_number(bfrt_info, targets[pipe_id], high_rep_reg_name)
		    lower_commit_seq = self.get_register_number(bfrt_info, targets[pipe_id], low_rep_reg_name)
		    print("Highest committed sequence number components: {} upper and {} lower".format(upper_commit_seq, lower_commit_seq))

                   
                    # measure size of ack count table TODO
                    #ack_name = "MyIngress{}.ack_array".format(pipe_id)
                    #ack_res = self.get_full_register(bfrt_info, targets[pipe_id], ack_name)
                    #logger.info("[IN PROGRESS] Number of pending acks: {}".format(ack_res))
                    #self.ack_cnt_over_time[pipe_id].append(ack_res)
                
                    # measure elapsed time - note this is SPOT checks, each value is just one randomly sample packet
                    #raw_et_reg_name = "MyIngress{}.raw_elapsed_time".format(pipe_id)
                    #raw_et_lat = self.get_register_number(bfrt_info, target, raw_et_reg_name)
                    #self.raw_et_over_time[pipe_id].append(raw_et_lat)
                    #logger.info("[IN PROGRESS] Raw elapsed time: {}ns".format(raw_et_lat))
 
                    # measure total number of packets 
                except Exception as e:
                    logger.error("read of a register/counter failed with error: %r" % (e))
                    failed = True
                    print("EXCEPTION REACHED!")

                if failed:
                    failed = False
                    time.sleep(self.time_interval_s)
                    continue

                # Calculate timestamps
                # Latency
                total_latency_ns = (high_lat << 31) + low_lat

                # Record data
                #if first[pipe_id]:
                #    first[pipe_id] = False
                #    self.baseline_pkts[pipe_id] = pkts
                #    self.baseline_curr_time[pipe_id] = curr_time
                #    self.baseline_total_latency[pipe_id] = total_latency_ns
                #    continue
                
                # Number of comitted appends + number of total packets generated
                self.pkts_over_time[pipe_id].append(pkts)
                self.pktgen_pkt_counters[pipe_id].append(pkt_value)

                # Total aggregate latency over all appends up until this point
                self.avg_lat_over_time[pipe_id].append(total_latency_ns)

                # Latest time a random append spent in the pipeline
                self.pipeline_time[pipe_id].append(pipe_res)
		self.cntrl_pkt_cnt_over_time[pipe_id].append(cntrl_cnt)

                # Current timestamp
                self.pipe_timestamps[pipe_id].append(now)


                self.agg_seq_recirc_cntr[pipe_id].append(seq_recirc_cntr)


                # How long between start and end of pktgen + throughput measurements
                self.pktgen_jitter[pipe_id].append((t_pg_end - t_pg))
                self.tput_jitter[pipe_id].append((now - t_pg_end))

                #print("=======================================================================")
                #logger.info("PIPE %d Triggered %d times", pipe_id, tri_value)
                #logger.info("PIPE %d Generated %d batches", pipe_id, batch_value)
                #logger.info("PIPE %d Generated %d packets", pipe_id, pkt_value)
                #logger.info("Time elapsed: %f", pktgen_time_elapsed)
                #logger.info("Rate of packet production is %d packets for 1 second", rate_pkt_prod)
                #logger.info("[IN PROGRESS] Total Append Packets: {}".format(pkts))
	        #logger.info("[IN PROGRESS] Append Throughput from PIPE {}: {}".format(pipe_id, pkts_tput))
                #logger.info("[IN PROGRESS] High Latency: {}ns and Low Latency: {}ns".format(high_lat, low_lat))
                #logger.info("[IN PROGRESS] Aggregate Latency: {}ns".format(total_latency_ns))
                #if pkts != 0:
                #    logger.info("[IN PROGRESS] Average latency: {} microseconds".format((total_latency_ns/float(pkts))/float(1000)))
                #logger.info("[IN PROGRESS] Time in the pipeline {}: {}".format(pipe_id, pipe_res))
                #print("=======================================================================")
            self.timestamps.append(global_timestamp)
	    time.sleep(self.time_interval_s)
        self.final_elapsed_tput_time = time.time() - start_time
        
        for pipe_id in range(2):
            if pipe_id not in active_pipes:
                continue
            resp = pktgen_app_cfg_table.entry_get(
                target,
                [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', pkt_gen_app_id)])],
                {"from_hw": True},
                pktgen_app_cfg_table.make_data([gc.DataTuple('pkt_counter')], '$PKTGEN_TRIGGER_TIMER_PERIODIC', get=True)
            )
            data_dict = next(resp)[0].to_dict()
            self.final_pktgen_count = data_dict["pkt_counter"]
            self.disable_timer_pkt_gen(bfrt_info, targets[pipe_id])
	    time.sleep(1)
            tot_cnt_name = "MyIngress{}.tot_packet_counter{}".format(pipe_id, pipe_id)
            self.final_pkt_count[pipe_id] = self.get_counter_number(bfrt_info, tot_cnt_name)
            low_rep_reg_name = "MyIngress{}.highest_lower_replicated_seq_no".format(pipe_id)
            high_rep_reg_name = "MyIngress{}.highest_upper_replicated_seq_no".format(pipe_id)
            upper_commit_seq = self.get_register_number(bfrt_info, targets[pipe_id], high_rep_reg_name)
            lower_commit_seq = self.get_register_number(bfrt_info, targets[pipe_id], low_rep_reg_name)
	    print("Highest committed sequence number components: {} upper and {} lower".format(upper_commit_seq, lower_commit_seq))
            self.final_committed_seq_no[pipe_id] = ((upper_commit_seq & 0xFFFFFFFF) << 32) | (lower_commit_seq & 0xFFFFFFFF)
            low_lat = self.get_register_number(bfrt_info, targets[pipe_id], "MyIngress{}.latency_lower".format(pipe_id))
            high_lat = self.get_register_number(bfrt_info, targets[pipe_id], "MyIngress{}.latency_higher".format(pipe_id))
            self.final_total_latency[pipe_id] = (high_lat << 31) + low_lat

    ######################################## Core Test Runner ###################################
    def runTest(self):
	filepath = "/tmp/switch_config.yaml"
        logger.info(filepath)
	data = self.load_config(filepath)
	os.system("taskset -p -c 1 {}".format(os.getpid()))
	dev_number = data['device_number']

	# Device-wide startup (once per physical switch -- these resources are shared
	# across both pipes, NOT duplicated per pipe: PRE/multicast lives in the Traffic
	# Manager, not in a specific pipe, and there is only one bfrt_info session for
	# this whole ASIC).
	bfrt_info = self.interface.bfrt_info_get(p4_program_name)
        self.target = gc.Target(device_id=0, pipe_id=0xffff)
        self.target0 = gc.Target(device_id=0, pipe_id=0x00)
        self.target1 = gc.Target(device_id=0, pipe_id=0x01)
        targets = [self.target0, self.target1]

        self.setup_gen_lock = threading.Lock()

        port_speed = data['port_speed']
	port_fec = data['port_fec']
	cpu_interface = data['cpu_interface']
        self.cpu_interface = cpu_interface
	cpu_port = data['cpu_port']
	in_cntrl = data['in_cntrl']
	out_cntrl = data['out_cntrl']
        self.view = data['start_view']

        self.view_timeout = float(data['view_timeout_ms'])/1000 
        self.view_phase_timeout = float(data['view_phase_timeout_ms'])/1000 
        ack_threshold = data['ack_threshold'] # NEW
        self.payload_size = data['payload_size'] # NEW
	switch_send_cntrl = data['send_cntrl_pkt']
        num_shards = data['num_shards']
        shard_size = data['shard_size']

        self.physical_switch_id = data['physical_switch_id']
        timeout = data['timeout']

        view_threshold = data['num_switches'] / 2 + 1

        nsperpkt = data['nsperpkt']
        duration = data['duration']
        wait_time = data['wait_time'] # How long to wait for all switches to setup
        self.trigger_wait_time = data['trigger_wait_time'] # How long to wait until dropping control packet
        lower_time_bound = data['lower_time_bound']
        upper_time_bound = data['upper_time_bound']
        experiment_type = data['experiment_type']
        self.time_interval_s = float(data['time_interval_ms'])/1000
        physical_switch_to_logical = data['physical_switch_to_logical']
        self.view_change_pipe = data['view_change_pipe']
        self.view_change_switch = data['view_change_switch']
	self.view_change_leader = self.view_change_switch
        self.do_view_change = data['do_view_change']
        self.seed = data['seed']

        active_switches = data['active_switches']

        portSetup = PortSetup() 
        portSetup.initialize_ports(bfrt_info, logger) 


        logger.info("Experiment type: {}".format(experiment_type))
        logger.info("Starting view number: {}".format(self.view))
         
        warmup = 5
        cooldown = 5
        buffer_time = 10
        total_time = duration + warmup + cooldown + buffer_time
        app_id = 0
	pktgen_app_cfg_table = bfrt_info.table_get("$PKTGEN_APPLICATION_CFG")
        
        self.view_change_time_breakdown = {0: {}, 1: {}}
        self.raw_et_over_time = {0: [], 1: []}
        self.queue_cnt_over_time = {0: {}, 1: {}}
        self.ack_cnt_over_time = {0: [], 1: []}
        self.avg_lat_over_time = {0: [], 1: []}
        self.tput_jitter = {0: [], 1: []}
        self.pktgen_jitter = {0: [], 1: []}
        self.tput_over_time = {0: [], 1: []}
        self.pkts_over_time = {0: [], 1: []}
        self.pipeline_time = {0: [], 1: []}
        self.cntrl_pkt_cnt_over_time = {0: [], 1: []}
        self.generate_pkts_over_time = {0: [], 1: []}
        self.pipe_timestamps = {0: [], 1:[]}
        self.agg_seq_recirc_cntr = {0: [], 1:[]}
        self.timestamps = []
        self.queue_timestamps = []
        self.pktgen_pkt_counters = {0: [], 1:[]}
        self.final_pkt_count = [0, 0]
        self.final_elapsed = [0, 0]
        self.final_total_latency = [0, 0]
        self.final_committed_seq_no = [0, 0]
        self.baseline_pkts = [0, 0]
        self.baseline_curr_time = [0, 0]
        self.baseline_total_latency = [0, 0]
        self.timestamp_start_epoch = 0
        self.view_change_duration = 0

        run_setup = True # TODO

	# NEW: per-pipe sections. Each entry describes everything that is genuinely
	# scoped to ONE pipe: which devport is used for cntrl forwarding, which ports
	# are held in loopback, which front-panel ports belong to that pipe, how many
	# recirc ports are active, and whether this pipe is the one that kicks off the
	# ring's first control packet.
	pipes_cfg = data['pipes']

    	all_ports = [] 
        self.setup_all_switch_ports(self.target, pipes_cfg, port_speed, port_fec, portSetup)
        p4TableObj = []

	for pipe_cfg in pipes_cfg:
	    recirc_ports = pipe_cfg['recirc_ports']
	    wait_ports = pipe_cfg['wait_ports']
	    physical_ports = pipe_cfg['physical_ports']
	    tot_num_recirc_ports = len(recirc_ports)
	    tot_num_wait_ports = len(wait_ports)
	    #list_of_switch_ports = pipe_cfg['switch_ports']
	    port_to_pipe_0 = pipe_cfg['port_to_pipe_0']
            pipe_idx = pipe_cfg['pipe_id']
            next_hop_port_id = pipe_cfg['next_ring_hop_id']
            switch_id = pipe_cfg['logical_switch_id']
            queue_ports_pipe = []
            if pipe_idx == 0:
                queue_ports_pipe = recirc_ports + wait_ports + [port_to_pipe_0]
            else:
                queue_ports_pipe = recirc_ports + wait_ports + physical_ports
            all_ports.append(queue_ports_pipe)

            client_machines = []
            p4Tables = P4Tables(switch_id, self.physical_switch_id, bfrt_info, global_group_id, list_of_types, logger)
            p4TableObj.append(p4Tables)

	    logger.info("Pipe {} - ports to ring members: {}".format(pipe_idx, physical_ports))

	    if run_setup:
	        ####################### SETUP MATCH-ACTION TABLES (this pipe) ##########################
                logger.info("Pipe {}: Number of loopback ports {}".format(pipe_idx, len(recirc_ports)))
                logger.info("Pipe: {},  Hop: {}".format(pipe_idx, next_hop_port_id))
                logger.info("View change: {}".format(self.view))

                view_register_name = "MyEgress{}.egress_current_view".format(pipe_idx)
                view_register_idx = 0
                self.insert_register_entry(bfrt_info, view_register_name, view_register_idx, self.view)
                output = self.get_register_number(bfrt_info, targets[pipe_idx], view_register_name)
                print("For pipe {}, new value of {}: {}".format(pipe_idx, view_register_name, output))

                ring_register_name = "MyIngress{}.next_ring_hop_id".format(pipe_idx)
                ring_register_idx = 0
                self.insert_register_entry(bfrt_info, ring_register_name, ring_register_idx, next_hop_port_id)
                output = self.get_register_number(bfrt_info, targets[pipe_idx], ring_register_name)
                print("For pipe {}, new value of {}: {}".format(pipe_idx, ring_register_name, output))

                pkt_it_reg_name = "MyIngress{}.cntrl_pkt_it".format(pipe_idx)
                pkt_it = self.get_register_number_at_idx(bfrt_info, targets[pipe_idx], pkt_it_reg_name, 0)
                print("Pkt it: {}".format(pkt_it))
        
                p4Tables.setup_constants_table(tot_num_recirc_ports, tot_num_wait_ports, ack_threshold, num_shards, shard_size, switch_id, pipe_idx)
                # Tracks control packet iteration
                p4Tables.setup_pkt_it(pipe_idx)

                # Track per switch batches
                #p4Tables.setup_edit_batch_size(pipe_idx)
                #p4Tables.setup_update_batches(pipe_idx)

                # Handles sequence numbers
                p4Tables.setup_local_seq(pipe_idx)
                p4Tables.setup_lower_process_gsn(pipe_idx)
                p4Tables.setup_upper_process_gsn(pipe_idx)

                # Append tables
                p4Tables.setup_append_first_tbl(pipe_idx)
                p4Tables.setup_append_second_tbl(pipe_idx)
                p4Tables.setup_ack_action_tbl(pipe_idx)

                # Measure latency
                p4Tables.setup_ack_latency(pipe_idx)

                # Determines whether the next hop ID should be set
                #p4Tables.setup_cntrl_next_hop(pipe_idx)

                # Setup routing for sending packets to different members of the ring
                p4Tables.setup_ring_routing(physical_ports, pipe_idx, port_to_pipe_0, physical_switch_to_logical)

                # Setup routing for recirculating packets
                p4Tables.setup_seq_port_recirc(1, pipe_idx)
                p4Tables.setup_stor_port_recirc(2, pipe_idx)

                p4Tables.setup_recirculation_routing(recirc_ports, pipe_idx, 1) # Wait for sequencing
                p4Tables.setup_recirculation_routing(wait_ports, pipe_idx, 2) # Wait for storage

                # Setup routing for view related traffic
                p4Tables.setup_view_routing(physical_ports, pipe_idx, port_to_pipe_0, physical_switch_to_logical, cpu_port)

                ## Setup routing for responding to client machines TODO needs more thought
                p4Tables.setup_client_routing(client_machines, pipe_idx)
                ##p4Tables.setup_cpu_dummy(pipe_idx, cpu_port)

                # Egress tables
                p4Tables.setup_check_dur_table(lower_time_bound, upper_time_bound, pipe_idx)
                p4Tables.setup_check_view_change_table(pipe_idx)

                # Meter programming
		min_frame_len = self.payload_size + 77 # TODO: Don't hardcode 77...
		headroom = .99
		n_seq = tot_num_recirc_ports
		rate_pps = int(headroom * n_seq * 100e9 / (8.0 * (min_frame_len + 24)))
		burst_pkts = 512 # low
                p4Tables.setup_seq_mtr_table(rate_pps, burst_pkts, pipe_idx)

        
        ###### DO ALL SETUP #####
	before = portSetup.port_stats_snapshot(bfrt_info, self.target)

        active_view_threads = []
        active_pipe_q_threads = []
        
        # Setup packet generation
        for pipe_cfg in pipes_cfg:
            pipe_id = pipe_cfg['pipe_id']
            switch_id = pipe_cfg['logical_switch_id']
            if pipe_cfg['active_pipe']:
                self.setup_timer_pkt_gen(bfrt_info, targets[pipe_id], CONST_MAC_DST, CONST_MAC_SRC, CONST_IP, self.payload_size, cpu_interface, duration, nsperpkt, pipe_id, experiment_type, switch_id)
        
        # Start experiment observation threads
        active_pipes = []
       
        # Setup the queue map
        queue_idx_map = {}
        queues_of_interest = {}
        for pipe_id in range(0, 2):
            self.queue_cnt_over_time[pipe_id] =  {}
            queues_of_interest[pipe_id] = []
            queue_idx_map[pipe_id] = {}
            for port in all_ports[pipe_id]:
                self.queue_cnt_over_time[pipe_id][port] =  []
                for i in range(0, 1):
                    port_idx = (port << 5) | i
                    queues_of_interest[pipe_id].append(port_idx)
                    queue_idx_map[pipe_id][port_idx] = [port, i]
        
        ##### START THE EXPERIMENT ####
        time.sleep(wait_time)
        if switch_send_cntrl:
	    logger.info("Sending control packet!")
	    self.send_cntrl_packet(cpu_interface, CONST_MAC_DST, CONST_MAC_SRC, 0, self.view, self.seed)
    
        for pipe_cfg in pipes_cfg:
            pipe_id = pipe_cfg['pipe_id']
            switch_id = pipe_cfg['logical_switch_id']
            if pipe_cfg['active_pipe']:
                active_pipes.append(pipe_id)
	    	print("Starting view change manager for active pipe {}".format(pipe_id))
            	switch_id = pipe_cfg['logical_switch_id']
                viewChangeMaintainer = ViewChange(self.view, view_threshold, switch_id, CONST_MAC_DST, CONST_MAC_SRC, cpu_interface, cpu_port, self.view_phase_timeout, duration, logger, active_switches, self.view_change_switch)
                view_maintain_t = threading.Thread(target=self.maintain_view_change, args=(viewChangeMaintainer, bfrt_info, targets[pipe_id], pipe_id, duration,self.view,switch_id,), kwargs={})
                active_view_threads.append(view_maintain_t)
        total_duration = duration + 10 
        active_q_pipe_t = threading.Thread(target=self.pipe_queue_monitor, args=(bfrt_info, targets, active_pipes, total_duration, queue_idx_map, queues_of_interest,), kwargs={})
        active_pipe_t = threading.Thread(target=self.pipe_pktgen_runtime, args=(bfrt_info, targets, duration, active_pipes,), kwargs={})
        
        # Start packet generation AFTER observing threads are spawned
        start_time = time.time()
        cntrl_is_ready = []
        for pipe_id in active_pipes:
            while (time.time() - start_time) < duration:
               cntrl_cnt_reg_name = "MyIngress{}.cntrl_counter_ig{}".format(pipe_id, pipe_id)
               cntrl_cnt = self.get_counter_number(bfrt_info, cntrl_cnt_reg_name)
               if cntrl_cnt == 0:
                   continue
               else:
                   cntrl_is_ready.append(pipe_id)
                   break
        
        # Enable packet generation
        active_q_pipe_t.start()
        active_pipe_t.start()

        idx = 0
	for pipe_cfg in pipes_cfg:
           pipe_id = pipe_cfg['pipe_id']
           switch_id = pipe_cfg['logical_switch_id']
           if pipe_cfg['active_pipe']:
               active_view_threads[idx].start()
               idx += 1


        # TODO: cntrl is ready might only have some active pipes
        for pipe_cfg in pipes_cfg:
           pipe_id = pipe_cfg['pipe_id']
           switch_id = pipe_cfg['logical_switch_id']
           if pipe_cfg['active_pipe'] and pipe_id in cntrl_is_ready:
               self.enable_timer_pkt_gen(bfrt_info, targets[pipe_id])

        #### WAIT FOR THE EXPERIMENT TO END ####
        active_pipe_t.join()
        active_q_pipe_t.join()
        for t in active_view_threads:
            t.join()

	after  = portSetup.port_stats_snapshot(bfrt_info, self.target)
	portSetup.port_stats_report(before, after)

        print("==================================================================Done!")

        num_pipelines = len(pipes_cfg)

        # Collect final data
	for pipe_cfg in pipes_cfg:
            if pipe_cfg['active_pipe']:
	        pipe_id = pipe_cfg['pipe_id']
	        num_recirc_ports = pipe_cfg['total_recirc_ports']
                logger.info("Processing pipe {} info!".format(pipe_id))
                self.get_final_pktgen_stats(bfrt_info, targets[pipe_id], duration, nsperpkt, num_recirc_ports, pipe_id, num_pipelines, self.payload_size, experiment_type, lower_time_bound, upper_time_bound, self.time_interval_s*1000)
