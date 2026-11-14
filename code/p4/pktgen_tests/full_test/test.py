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


p4_program_name = "viewchange" # TODO make parameter
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
	    RingType(type=TYPE_APPEND, 
                     num_entries=1, 
                     shard_id=0,
                     switch_to_process=switch_id,
                     start_ts=0,
                     end_ts=0,
                     raw_elapsed_time=0,
                     pipe_ts=0)/ \
            Append(nonce=nonce,
                   payload_size=payload_size,
                   stream_id=0,
                   g_idx=0,
                   cntrl_pkt_it=0,
                   recv_port=192,
                   exp_type=exp_type,
                   start_ts=0)/ \
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

    ######################################## View Change maintenance ############################
    def trigger_view_change(self, viewChangeMaintainer, bfrt_info, target, pipe_id, trigger_wait_time, duration):
        print("DOING TRIGGER VIEW CHANGE NOW!!!")
        time.sleep(trigger_wait_time)
	self.insert_register_entry(bfrt_info, "MyEgress{}.egress_current_view".format(pipe_id), 0, self.view - 1) # TODO: Setting egress current view to dummy value to INDUCE control packet drop

    def maintain_view_change(self, viewChangeMaintainer, bfrt_info, target, pipe_id, duration):
        # View Change
        print("===============Thread: Maintain view change!!! View timeout is: {}".format(self.view_timeout))
        self.response_thread = threading.Thread(target=viewChangeMaintainer.collect_responses, args=())
        self.response_thread.start()
        print("PUBLIC SERVICE ANNOUNCEMENT: MAKE SURE THAT YOUR TRIGGER THREAD IS NOT ALSO CREATING A COLLECT RESPONSES THREAD!!!!")
        cntr_name = "MyIngress{}.cntrl_counter_ig{}".format(pipe_id, pipe_id)
        pkt_it_reg_name = "MyIngress{}.cntrl_pkt_it".format(pipe_id)
        start_time = time.time() # TODO make sure you keep track of the units here
        experiment_time = time.time()
        sleep_time = self.view_timeout / 3
    	prev_cntrl_pkt_cnt = self.get_counter_number(bfrt_info, cntr_name)
        while True:
            time.sleep(sleep_time)
            if (time.time() - experiment_time) > duration: # If the experiment finishes, return
                break
    	    cntrl_pkt_cnt = self.get_counter_number(bfrt_info, cntr_name)
	    #print("Previously I received {} cntrl packets and now received {} cntrl packets".format(prev_cntrl_pkt_cnt, cntrl_pkt_cnt))
            if (prev_cntrl_pkt_cnt == cntrl_pkt_cnt) and (time.time() - start_time) > self.view_timeout:
                pkt_it = self.get_register_number_at_idx(bfrt_info, target, pkt_it_reg_name, 0)
                (new_view_num, new_start_seq_no) = viewChangeMaintainer.run_view_change(bfrt_info, self.target, pkt_it, duration, pipe_id)
		if new_view_num > self.view:
		    self.view = new_view_num
		    self.send_cntrl_packet(self.cpu_interface, CONST_MAC_DST, CONST_MAC_SRC, 0, self.view, new_start_seq_no)
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
            elif prev_cntrl_pkt_cnt != cntrl_pkt_cnt:
                prev_cntrl_pkt_cnt = cntrl_pkt_cnt
                start_time = time.time()
	    continue
        self.response_thread.join()

    ######################################## Packet Gen Stats ###################################
    def get_final_pktgen_stats(self, bfrt_info, target, duration, nsperpkt, num_recircs, pipe_id, num_pipelines, payload_size, exp_type, lower_time_bound, upper_time_bound, time_interval_ms):
        # 1. Get a reference to the counter table
        pkts = 0
        pkts_tput = 0
        logger.info("Here is the actual pipe id: {}".format(pipe_id))

        ############### Number of total packets
        tot_cnt_name = "MyIngress{}.tot_packet_counter{}".format(pipe_id, pipe_id)
        cnt_pkts = self.final_pkt_count[pipe_id]
        logger.info("======================Total COUNTED Append Packets: {}".format(cnt_pkts))
	logger.info("======================COUNTED Append Throughput from PIPE {}: {}".format(pipe_id, cnt_pkts/float(duration)))
        #low_tot_name = "MyIngress{}.total_pkt_cnt_lower".format(pipe_id)
        #high_tot_name = "MyIngress{}.total_pkt_cnt_higher".format(pipe_id)
	#low_num_tot_pkts = self.get_register_number(bfrt_info, target, low_tot_name)
	#high_num_tot_pkts = self.get_register_number(bfrt_info, target, high_tot_name)
        pkts = cnt_pkts #TODO (high_num_tot_pkts << 32) + low_num_tot_pkts - 1 # NOTE: the -1 is to correct for a small hack in how this tabulation is done in P4
        pkts_tput = 0
        if self.final_elapsed[pipe_id] != 0:
            pkts_tput = pkts/float(self.final_elapsed[pipe_id])
        #logger.info("[FAKE UPDATE]======================Upper Append Packets: {} and Lower Append Packets: {}".format(high_num_tot_pkts, low_num_tot_pkts))
	logger.info("[FAKE UPDATE]======================Total Append Packets: {}".format(pkts))
	logger.info("[FAKE UPDATE]======================Append Throughput from PIPE {}: {}".format(pipe_id, pkts_tput))

        ###### Depth of egress queue
        q_name = "MyEgress{}.queue_cnt".format(pipe_id)
        q_res = self.get_register_number(bfrt_info, target, q_name)
        logger.info("======================Num of packets in the egress queue: {}".format(q_res))

    	# Tofino counters usually return a dictionary with '$COUNTER_SPEC_PKTS'
        #low_lat_reg_name = "MyIngress{}.latency_lower".format(pipe_id)
        #high_lat_reg_name = "MyIngress{}.latency_higher".format(pipe_id)
        #low_lat = self.get_register_number(bfrt_info, target, low_lat_reg_name)
        #high_lat = self.get_register_number(bfrt_info, target, high_lat_reg_name)
        total_latency_ns = self.final_total_latency[pipe_id] #(high_lat << 31) + low_lat
        #logger.info("High Latency: {}ns and Low Latency: {}ns".format(high_lat, low_lat))
        logger.info("Aggregate Latency: {}ns".format(total_latency_ns))
        avg_lat = 0
        if pkts != 0:
	    avg_lat = (total_latency_ns/float(pkts))/float(1000)
            logger.info("======================Average latency: {} microseconds".format(avg_lat))
       
        #raw_dur_reg_name = "MyIngress{}.raw_duration".format(pipe_id)
        #raw_dur_lat = self.get_register_number(bfrt_info, target, raw_dur_reg_name)
        #logger.info("Raw duration: {}ns".format(raw_dur_lat))

        #raw_et_reg_name = "MyIngress{}.raw_elapsed_time".format(pipe_id)
        #raw_et_lat = self.get_register_number(bfrt_info, target, raw_et_reg_name)
        #logger.info("Raw elapsed time: {}ns".format(raw_et_lat))
        
        target_dest = "/root/pipe{}_{}_nsperpkt.json".format(pipe_id, nsperpkt)
        telemetry_payload = {
            "total_number_of_packets": int(pkts),
            "throughput_pps": float(pkts_tput),
            "total_latency_ns": float(total_latency_ns),
            "average_latency_us": avg_lat,
            "nsperpkt": int(nsperpkt),
            "duration": int(self.final_elapsed[pipe_id]),
            "total_loopback": int(num_recircs),
            "num_pipelines": int(num_pipelines),
            "payload_size": int(payload_size),
            "elapsed_time_timeseries": self.raw_et_over_time[pipe_id],
            "queue_count_timeseries": self.queue_cnt_over_time[pipe_id],
            "avg_lat_timeseries": self.avg_lat_over_time[pipe_id],
            "true_gen_pkt_rate_timeseries": self.generate_pkts_over_time[pipe_id],
            "tput_timeseries": self.tput_over_time[pipe_id],
            "num_pkts_timeseries": self.pkts_over_time[pipe_id],
            "pipeline_time_timeseries": self.pipeline_time[pipe_id],
            "timestamps": self.timestamps,
            "exp_type": exp_type,
            "lower_time_bound": lower_time_bound,
            "upper_time_bound": upper_time_bound,
            "pipe_id": pipe_id,
            "timeseries_interval_ms": time_interval_ms
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
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_CONTROL)/ \
              Cntrl(global_seq_no=start_seq_no, ring_view=view)/ \
              Canary(magic=165)
        logger.info("CONTROL PACKET WE ARE SENDING:")
        pkt.show()
        sendp(pkt, iface=interface, verbose=True)

    ######################################## Per Pipe Operations ###################################
    # Runtime per pipe maintenance which collects data from each pipe and manages the view change
    def pipe_pktgen_runtime(self, bfrt_info, target, cpu_interface, pipe_id, duration, nsperpkt, experiment_type, view_threshold, switch_id, active_switches):
        # Initialize and start the view change components
        viewChangeMaintainer = ViewChange(self.view, view_threshold, switch_id, CONST_MAC_DST, CONST_MAC_SRC, cpu_interface, cpu_port, self.view_timeout, duration, logger, active_switches)
        view_maintain_t = threading.Thread(target=self.maintain_view_change, args=(viewChangeMaintainer, bfrt_info, target, pipe_id, duration,), kwargs={})
        view_maintain_t.start()
        trigger_view_t = threading.Thread(target=self.trigger_view_change, args=(viewChangeMaintainer, bfrt_info, target, pipe_id, self.trigger_wait_time,duration), kwargs={})
        if pipe_id == self.view_change_pipe and switch_id == self.view_change_switch and self.do_view_change:
            trigger_view_t.start()
        
        self.setup_timer_pkt_gen(bfrt_info, target, CONST_MAC_DST, CONST_MAC_SRC, CONST_IP, self.payload_size, cpu_interface, duration, nsperpkt, pipe_id, experiment_type, switch_id)
        pktgen_app_cfg_table = bfrt_info.table_get("$PKTGEN_APPLICATION_CFG")
        prev_num_pkts = 0
        prev_time = 0
        
        start_time = time.time()
	while (time.time() - start_time) < duration:
            cntrl_cnt_reg_name = "MyIngress{}.cntrl_counter_ig{}".format(pipe_id, pipe_id)
            cntrl_cnt = self.get_counter_number(bfrt_info, cntrl_cnt_reg_name)
            logger.info("[IN PROGRESS] Ingress {} Control packet count: {}".format(pipe_id, cntrl_cnt))
            canary_drop_cnt_name = "MyEgress{}.canary_no_match_counter_eg{}".format(pipe_id, pipe_id)
            canary_drop_cnt = self.get_counter_number(bfrt_info, canary_drop_cnt_name)
            logger.info("[IN PROGRESS] Egress {} Canary Drop count: {}".format(pipe_id, canary_drop_cnt))
            #vc_time_start_name = "MyEgress{}.view_change_start_time".format(pipe_id)
            #vc_end_time = self.get_register_number_at_idx(bfrt_info, target, vc_time_start_name, 1)
            #logger.info("[IN PROGRESS] Egress {} End timestamp: {}".format(pipe_id, vc_end_time))
	    view_register_name = "MyEgress{}.egress_current_view".format(pipe_id)
            output = self.get_register_number(bfrt_info, target, view_register_name)
            print("For pipe {}, new value of {}: {}".format(pipe_id, view_register_name, output))
            view_register_name = "MyEgress{}.view_change_active".format(pipe_id)
            output = self.get_register_number(bfrt_info, target, view_register_name)
            print("For pipe {}, new value of {}: {}".format(pipe_id, view_register_name, output))

	    pkt_it_reg_name = "MyIngress{}.cntrl_pkt_it".format(pipe_id)
            pkt_it = self.get_register_number_at_idx(bfrt_info, target, pkt_it_reg_name, 0)
            print("Pkt it: {}".format(pkt_it))
        
            print("=======================================================================")
            # verify pktgen related counters
            curr_time = time.time() - start_time
            self.timestamps.append(curr_time)
            resp = pktgen_app_cfg_table.entry_get(
                target,
                [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', pkt_gen_app_id)])],
                {"from_hw": True},
                pktgen_app_cfg_table.make_data([gc.DataTuple('batch_counter'),
                                                gc.DataTuple('pkt_counter'),
                                                gc.DataTuple('trigger_counter')],
                                               '$PKTGEN_TRIGGER_TIMER_PERIODIC', get=True)
            )
            data_dict = next(resp)[0].to_dict()
            tri_value = data_dict["trigger_counter"]
            logger.info("Triggered %d times", tri_value)
            batch_value = data_dict["batch_counter"]
            logger.info("Generated %d batches", batch_value)
            pkt_value = data_dict["pkt_counter"]
            logger.info("Generated %d packets", pkt_value)
            time_elapsed = float(time.time() - prev_time)
            logger.info("Time elapsed: %f", time_elapsed)
            rate_pkt_prod = (pkt_value - prev_num_pkts)/time_elapsed
            logger.info("Rate of packet production is %d packets for 1 second", rate_pkt_prod)
            self.generate_pkts_over_time[pipe_id].append(rate_pkt_prod)
            prev_num_pkts = pkt_value
            prev_time = time.time()

            # measure elapsed time - note this is SPOT checks, each value is just one randomly sample packet
            #raw_et_reg_name = "MyIngress{}.raw_elapsed_time".format(pipe_id)
            #raw_et_lat = self.get_register_number(bfrt_info, target, raw_et_reg_name)
            #self.raw_et_over_time[pipe_id].append(raw_et_lat)
            #logger.info("[IN PROGRESS] Raw elapsed time: {}ns".format(raw_et_lat))
 
            # measure total number of packets 
            tot_cnt_name = "MyIngress{}.tot_packet_counter{}".format(pipe_id, pipe_id)
            pkts = self.get_counter_number(bfrt_info, tot_cnt_name)
            #low_tot_name = "MyIngress{}.total_pkt_cnt_lower".format(pipe_id)
            #high_tot_name = "MyIngress{}.total_pkt_cnt_higher".format(pipe_id)
	    #low_num_tot_pkts = self.get_register_number(bfrt_info, targets[pipe_id], low_tot_name)
	    #high_num_tot_pkts = self.get_register_number(bfrt_info, targets[pipe_id], high_tot_name)
            #pkts = (high_num_tot_pkts << 32) + low_num_tot_pkts - 1 # NOTE: the -1 is to correct for a small hack in how this tabulation is done in P4
            #logger.info("[IN PROGRESS] Upper Append Packets: {} and Lower Append Packets: {}".format(high_num_tot_pkts, low_num_tot_pkts))

            curr_dur = time.time() - start_time
            pkts_tput = pkts/float(curr_dur)
	    logger.info("[IN PROGRESS] Total Append Packets: {}".format(pkts))
	    logger.info("[IN PROGRESS] Append Throughput from PIPE {}: {}".format(pipe_id, pkts_tput))
            self.pkts_over_time[pipe_id].append(pkts)
            self.tput_over_time[pipe_id].append(pkts_tput)

            # measure average latency at this time period
            low_lat_reg_name = "MyIngress{}.latency_lower".format(pipe_id)
            high_lat_reg_name = "MyIngress{}.latency_higher".format(pipe_id)
            try:
                low_lat = self.get_register_number(bfrt_info, target, low_lat_reg_name)
            except Exception as e:
                logger.error("read of %s failed on pipe %s target %s: %r" % (low_lat_reg_name, pipe_id, target, e))
                print("EXCEPTION REACHED!")
            try:
                high_lat = self.get_register_number(bfrt_info, target, high_lat_reg_name)
                total_latency_ns = (high_lat << 31) + low_lat
                logger.info("[IN PROGRESS] High Latency: {}ns and Low Latency: {}ns".format(high_lat, low_lat))
                logger.info("[IN PROGRESS] Aggregate Latency: {}ns".format(total_latency_ns))
                avg_lat = 0
                if pkts != 0:
	            avg_lat = (total_latency_ns/float(pkts))/float(1000)
                    logger.info("[IN PROGRESS] Average latency: {} microseconds".format((total_latency_ns/float(pkts))/float(1000)))
                self.avg_lat_over_time[pipe_id].append(avg_lat)
            except Exception as e:
                logger.error("read of %s failed on pipe %s target %s: %r" % (high_lat_reg_name, pipe_id, target, e))
                print("EXCEPTION REACHED!")

            # measure queue size at this time period TODO
            q_name = "MyEgress{}.queue_cnt".format(pipe_id)
            q_res = self.get_register_number(bfrt_info, target, q_name)
            logger.info("[IN PROGRESS] Num of packets in the egress queue: {}".format(q_res))
            self.queue_cnt_over_time[pipe_id].append(q_res)

            # measure amount of time  at this time period
            pipe_name = "MyEgress{}.time_in_pipeline".format(pipe_id)
            pipe_res = self.get_register_number(bfrt_info, target, pipe_name)
            logger.info("[IN PROGRESS] Time in the pipeline {}: {}".format(pipe_id, pipe_res))
            self.pipeline_time[pipe_id].append(pipe_res)

            # measure size of ack count table TODO
            #ack_name = "MyIngress{}.ack_array".format(pipe_id)
            #ack_res = self.get_full_register(bfrt_info, targets[pipe_id], ack_name)
            #logger.info("[IN PROGRESS] Number of pending acks: {}".format(ack_res))
            #self.ack_cnt_over_time[pipe_id].append(ack_res)

            print("=======================================================================")
	    time.sleep(self.time_interval_s)
        tot_cnt_name = "MyIngress{}.tot_packet_counter{}".format(pipe_id, pipe_id)
        self.final_pkt_count[pipe_id] = self.get_counter_number(bfrt_info, tot_cnt_name)
        self.final_elapsed[pipe_id] = time.time() - start_time
        low_lat = self.get_register_number(bfrt_info, target, "MyIngress{}.latency_lower".format(pipe_id))
        high_lat = self.get_register_number(bfrt_info, target, "MyIngress{}.latency_higher".format(pipe_id))
        self.final_total_latency[pipe_id] = (high_lat << 31) + low_lat
        logger.info("FINAL: High Latency: {}ns and Low Latency: {}ns".format(high_lat, low_lat))
        print(self.final_pkt_count)
        print(self.final_elapsed)
        print(self.final_total_latency)
        view_maintain_t.join()
        if pipe_id == self.view_change_pipe and switch_id == self.view_change_switch and self.do_view_change:
            trigger_view_t.join()

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

        port_speed = data['port_speed']
	port_fec = data['port_fec']
	cpu_interface = data['cpu_interface']
        self.cpu_interface = cpu_interface
	cpu_port = data['cpu_port']
	in_cntrl = data['in_cntrl']
	out_cntrl = data['out_cntrl']
        self.view = data['start_view']

        self.view_timeout = float(data['view_timeout_ms'])/1000
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
        self.do_view_change = data['do_view_change']

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
         
        self.raw_et_over_time = {0: [], 1: []}
        self.queue_cnt_over_time = {0: [], 1: []}
        self.ack_cnt_over_time = {0: [], 1: []}
        self.avg_lat_over_time = {0: [], 1: []}
        self.tput_over_time = {0: [], 1: []}
        self.pkts_over_time = {0: [], 1: []}
        self.pipeline_time = {0: [], 1: []}
        self.generate_pkts_over_time = {0: [], 1: []}
        self.timestamps = []
        self.final_pkt_count = [0, 0]
        self.final_elapsed = [0, 0]
        self.final_total_latency = [0, 0]

        run_setup = True # TODO

	# NEW: per-pipe sections. Each entry describes everything that is genuinely
	# scoped to ONE pipe: which devport is used for cntrl forwarding, which ports
	# are held in loopback, which front-panel ports belong to that pipe, how many
	# recirc ports are active, and whether this pipe is the one that kicks off the
	# ring's first control packet.
	pipes_cfg = data['pipes']

    	self.setup_all_switch_ports(self.target, pipes_cfg, port_speed, port_fec, portSetup)
        p4TableObj = []

	for pipe_cfg in pipes_cfg:
	    recirc_ports = pipe_cfg['recirc_ports']
	    wait_ports = pipe_cfg['wait_ports']
	    physical_ports = pipe_cfg['physical_ports']
	    tot_num_recirc_ports = len(recirc_ports)
	    tot_num_wait_ports = len(wait_ports)
	    list_of_switch_ports = pipe_cfg['switch_ports']
	    port_to_pipe_0 = pipe_cfg['port_to_pipe_0']
            pipe_idx = pipe_cfg['pipe_id']
            next_hop_port_id = pipe_cfg['next_ring_hop_id']
            switch_id = pipe_cfg['logical_switch_id']

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
                p4Tables.setup_process_gsn(pipe_idx)

                # Append tables
                p4Tables.setup_append_first_tbl(pipe_idx)
                p4Tables.setup_append_second_tbl(pipe_idx)
                p4Tables.setup_ack_action_tbl(pipe_idx)

                # Measure latency
                p4Tables.setup_ack_latency(pipe_idx)

                # Determines whether the next hop ID should be set
                p4Tables.setup_cntrl_next_hop(pipe_idx)

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
        time.sleep(wait_time)

	before = portSetup.port_stats_snapshot(bfrt_info, self.target)

        if switch_send_cntrl:
	    logger.info("Sending control packet!")
	    self.send_cntrl_packet(cpu_interface, CONST_MAC_DST, CONST_MAC_SRC, 0, self.view, 0)

        active_pipe_threads = []
        for pipe_cfg in pipes_cfg:
            pipe_id = pipe_cfg['pipe_id']
            switch_id = pipe_cfg['logical_switch_id']
            if pipe_cfg['active_pipe']:
	    	print("Starting runtime for active pipe {}".format(pipe_id))
                active_pipe_t = threading.Thread(target=self.pipe_pktgen_runtime, args=(bfrt_info, targets[pipe_id], cpu_interface, pipe_id, duration, nsperpkt, experiment_type, view_threshold,switch_id,active_switches,), kwargs={})
                active_pipe_t.start()
                active_pipe_threads.append(active_pipe_t)

        for t in active_pipe_threads:
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
