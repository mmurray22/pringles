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
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../controlplane_libs")))
from headers import *
from multicast import *
from portsetup import *
from tables import * 


p4_program_name = "pktgen" # TODO make parameter
g_timer_app_id = 1
g_num_pipes = int(testutils.test_param_get("num_pipes"))
listen_ip = "0.0.0.0" # Listen on all interfaces
listen_port = 5005
CONST_MAC_DST = "00:90:fb:70:65:71"
CONST_MAC_SRC = "00:25:90:53:e6:00"
CONST_IP = "10.229.49.9"
pkt_gen_app_id = 1


# Initialize the logger
logger = logging.getLogger('PacketGenTest')
if not len(logger.handlers):
    logger.addHandler(logging.StreamHandler())

class PacketGenTest(BfRuntimeTest, P4Tables):
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
            for port in pipe_cfg['loopback_ports']:
                loop_ports.add(port)
            for port in pipe_cfg['wait_ports']:
                loop_ports.add(port)
            for port in pipe_cfg['ports_to_ring_members']:
                no_loop_ports.add(port)
            for port in pipe_cfg['switch_ports']:
                no_loop_ports.add(port)
            if pipe_cfg['cntrl_port_is_loopback'] == True:
                loop_ports.add(pipe_cfg['cntrl_port'])
            else:
                no_loop_ports.add(pipe_cfg['cntrl_port'])
        logger.info(loop_ports)
        logger.info(no_loop_ports)
        logger.info(port_speed)
        logger.info(port_fec)
	for i in loop_ports:
	    portSetup.port_setup(target, i, True, port_speed, port_fec)
        for i in no_loop_ports:
	    portSetup.port_setup(target, i, False, port_speed, port_fec)

    ###################### CONTROL PLANE RUNTIME ##############################3
    def batch_check(self, bfrt_info, target, interface, duration):
        #os.system("taskset -p -c 0 {}".format(os.getpid()))
        # This creates a raw socket exactly like tcpdump
        recv_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(0x0003))
        recv_sock.bind((interface, 0))
        logger.info("[*] Packet Gen Socket Open and Listening...")
        # Block until 1 packet is received

        start_time = time.time()
        while (time.time() - start_time) < duration:
	    time.sleep(1)
            # Tofino counters usually return a dictionary with '$COUNTER_SPEC_PKTS'
            active_batch_tbl = bfrt_info.table_get("pipe.MyIngress.active_batches")
	    batch_val= active_batch_tbl.entry_get(
	        target,
	        [active_batch_tbl.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	        {"from_hw": True}
	    )
	    data, _ = next(batch_val)
	    data_dict = data.to_dict()
	    #logger.info(data_dict)
            logger.info("Total Active Batches: {}".format(data_dict['MyIngress.active_batches.f1'][1]))

    def pgen_timer_hdr_to_dmac(self, pipe_id, app_id, batch_id, packet_id):
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
    def pgen_port(self, pipe_id, pipe_local_port_idx):
        """
        Given a pipe return a port in that pipe which is usable for packet
        generation.  Note that Tofino allows ports 68-71 in each pipe to be used for
        packet generation while Tofino2 allows ports 0-7.  This example will use
        either port 68 or port 6 in a pipe depending on chip type.
        """
        list_pgen_ports = [68, 69, 70, 71]
        pipe_local_port = list_pgen_ports[pipe_local_port_idx]
        return self.make_port(pipe_id, pipe_local_port)
    
    def make_port(self, pipe, local_port):
        return (pipe << 7) | local_port
    
    def setup_timer_pkt_gen(self, bfrt_info, target, dstAddr, srcAddr, ip_addr, payload_size, in_cntrl, cpu_interface, duration, nsperpkt, pipe_id, exp_type):
        logger.info("=============== Testing Packet Generator trigger by Timer ===============")
        pktgen_app_cfg_table = bfrt_info.table_get("$PKTGEN_APPLICATION_CFG")
        pktgen_pkt_buffer_table = bfrt_info.table_get("$PKTGEN_PKT_BUFFER")
        pktgen_port_cfg_table = bfrt_info.table_get("$PKTGEN_PORT_CFG")

        # timer pktgen app_id = 1 one shot 0
        app_id = 0 #pkt_gen_app_id TODO
        nonce = 33
        payload = "P" * payload_size
        packed_ip = socket.inet_aton(ip_addr)
        ip_int = struct.unpack("!I", packed_ip)[0]
	p = Ether(dst=dstAddr, src=srcAddr, type=TYPE_IP)/ \
            IP(dst=ip_addr)/ \
	    UDP(dport=1234, sport=5678)/ \
	    RingType(type=TYPE_APPEND, num_entries=1, shard_id=0,switch_to_process=1)/ \
            Append(nonce=nonce,payload_size=payload_size,stream_id=0,g_idx=0,cntrl_pkt_it=0,client_ip=ip_int,recv_port=192,start_ts=0,exp_type=exp_type)/ \
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
        except gcBfruntimeRpcException as e:
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
     
    def get_final_pktgen_stats(self, bfrt_info, target, duration, nsperpkt, num_recircs, pipe_id, num_pipelines, payload_size, exp_type, lower_time_bound, upper_time_bound, time_interval_ms):
        # 1. Get a reference to the counter table
        pkts = 0
        pkts_tput = 0
        logger.info("Here is the actual pipe id: {}".format(pipe_id))

        ############### Number of total packets
        tot_cnt_name = "MyIngress{}.tot_packet_counter".format(pipe_id)
        cnt_pkts = self.get_counter_number(bfrt_info, tot_cnt_name)
        logger.info("======================Total COUNTED Append Packets: {}".format(cnt_pkts))
	logger.info("======================COUNTED Append Throughput from PIPE {}: {}".format(pipe_id, cnt_pkts/float(duration)))
        #low_tot_name = "MyIngress{}.total_pkt_cnt_lower".format(pipe_id)
        #high_tot_name = "MyIngress{}.total_pkt_cnt_higher".format(pipe_id)
	#low_num_tot_pkts = self.get_register_number(bfrt_info, target, low_tot_name)
	#high_num_tot_pkts = self.get_register_number(bfrt_info, target, high_tot_name)
        pkts = cnt_pkts #TODO (high_num_tot_pkts << 32) + low_num_tot_pkts - 1 # NOTE: the -1 is to correct for a small hack in how this tabulation is done in P4
        pkts_tput = pkts/float(duration)
        #logger.info("[FAKE UPDATE]======================Upper Append Packets: {} and Lower Append Packets: {}".format(high_num_tot_pkts, low_num_tot_pkts))
	logger.info("[FAKE UPDATE]======================Total Append Packets: {}".format(pkts))
	logger.info("[FAKE UPDATE]======================Append Throughput from PIPE {}: {}".format(pipe_id, pkts/float(duration)))

        ###### Depth of egress queue
        q_name = "MyEgress{}.queue_cnt".format(pipe_id)
        q_res = self.get_register_number(bfrt_info, target, q_name)
        logger.info("======================Num of packets in the egress queue: {}".format(q_res))

    	# Tofino counters usually return a dictionary with '$COUNTER_SPEC_PKTS'
        low_lat_reg_name = "MyIngress{}.latency_lower".format(pipe_id)
        high_lat_reg_name = "MyIngress{}.latency_higher".format(pipe_id)
        low_lat = self.get_register_number(bfrt_info, target, low_lat_reg_name)
        high_lat = self.get_register_number(bfrt_info, target, high_lat_reg_name)
        total_latency_ns = (high_lat << 31) + low_lat
        logger.info("High Latency: {}ns and Low Latency: {}ns".format(high_lat, low_lat))
        logger.info("Aggregate Latency: {}ns".format(total_latency_ns))
        avg_lat = 0
        if pkts != 0:
	    avg_lat = (total_latency_ns/float(pkts))/float(1000)
            logger.info("======================Average latency: {} microseconds".format(avg_lat))
       
        raw_dur_reg_name = "MyIngress{}.raw_duration".format(pipe_id)
        raw_dur_lat = self.get_register_number(bfrt_info, target, raw_dur_reg_name)
        logger.info("Raw duration: {}ns".format(raw_dur_lat))

        raw_et_reg_name = "MyIngress{}.raw_elapsed_time".format(pipe_id)
        raw_et_lat = self.get_register_number(bfrt_info, target, raw_et_reg_name)
        logger.info("Raw elapsed time: {}ns".format(raw_et_lat))
        
        target_dest = "/root/pipe{}_{}_nsperpkt.json".format(pipe_id, nsperpkt)
        telemetry_payload = {
            "total_number_of_packets": int(pkts),
            "throughput_pps": float(pkts_tput),
            "total_latency_ns": float(total_latency_ns),
            "average_latency_us": avg_lat,
            "nsperpkt": int(nsperpkt),
            "duration": int(duration),
            "total_loopback": int(num_recircs),
            "num_pipelines": int(num_pipelines),
            "payload_size": int(payload_size),
            "elapsed_time_timeseries": self.raw_et_over_time[pipe_id],
            "queue_count_timeseries": self.queue_cnt_over_time[pipe_id],
            "avg_lat_timeseries": self.avg_lat_over_time[pipe_id],
            "true_gen_pkt_rate_timeseries": self.generate_pkts_over_time[pipe_id],
            "tput_timeseries": self.tput_over_time[pipe_id],
            "num_pkts_timeseries": self.pkts_over_time[pipe_id],
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
    
    def send_cntrl_packet(self, interface, dstAddr, srcAddr, in_cntrl, seq_no):
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_CONTROL)/ \
              Cntrl(global_seq_no=seq_no, ring_view=1, pkt_id=in_cntrl)
        logger.info("CONTROL PACKET WE ARE SENDING: {}".format(pkt.show()))
        sendp(pkt, iface=interface, verbose=True)

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

        portSetup = PortSetup() 
        portSetup.initialize_ports(bfrt_info, logger) 

	port_speed = data['port_speed']
	port_fec = data['port_fec']
	cpu_interface = data['cpu_interface']
	cpu_port = data['cpu_port']
	in_cntrl = data['in_cntrl']
	out_cntrl = data['out_cntrl']
        size_of_ring = data['size_of_ring']
        #use_stor = data['use_stor'] TODO add in eventually?
        self.view = data['start_view']

        cntrl_timeout = data['cntrl_timeout']
        ack_threshold = data['ack_threshold'] # NEW
        payload_size = data['payload_size'] # NEW
	switch_send_cntrl = data['send_cntrl_pkt']

        nsperpkt = data['nsperpkt']
        duration = data['duration']
        wait_time = data['wait_time'] # How long to wait for all switches to setup
        lower_time_bound = data['lower_time_bound']
        upper_time_bound = data['upper_time_bound']
        experiment_type = data['experiment_type']
        logger.info("Experiment type: {}".format(experiment_type))
	
        warmup = 5
        cooldown = 5
        buffer_time = 10
        total_time = duration + warmup + cooldown + buffer_time
        app_id = 0
	pktgen_app_cfg_table = bfrt_info.table_get("$PKTGEN_APPLICATION_CFG")
         
        self.raw_et_over_time = {0: [], 1: []}
        self.queue_cnt_over_time = {0: [], 1: []}
        self.avg_lat_over_time = {0: [], 1: []}
        self.tput_over_time = {0: [], 1: []}
        self.pkts_over_time = {0: [], 1: []}
        self.generate_pkts_over_time = {0: [], 1: []}
        prev_num_pkts = 0

        run_setup = True

	# NEW: per-pipe sections. Each entry describes everything that is genuinely
	# scoped to ONE pipe: which devport is used for cntrl forwarding, which ports
	# are held in loopback, which front-panel ports belong to that pipe, how many
	# recirc ports are active, and whether this pipe is the one that kicks off the
	# ring's first control packet.
	pipes_cfg = data['pipes']


    	self.setup_all_switch_ports(self.target, pipes_cfg, port_speed, port_fec, portSetup)

	for pipe_cfg in pipes_cfg:
	    loopback_ports = pipe_cfg['loopback_ports']
	    wait_ports = pipe_cfg['wait_ports']
	    ports_to_ring_members = pipe_cfg['ports_to_ring_members']
	    tot_num_recirc_ports = pipe_cfg['total_recirc_ports']
	    list_of_switch_ports = pipe_cfg['switch_ports']
	    cntrl_port = pipe_cfg['cntrl_port']
            is_cntrl_pkt_loopback = pipe_cfg['cntrl_port_is_loopback']
            pipe_id = pipe_cfg['pipe_id']


	    logger.info("Pipe {} - ports to ring members: {}".format(pipe_id, ports_to_ring_members))

	    if run_setup:
	        ####################### SETUP MATCH-ACTION TABLES (this pipe) ##########################
	        logger.info("Pipe {} - Number of loopback ports: {}".format(pipe_id , len(loopback_ports)))
	        logger.info("Pipe {} - Control port: {}, with in port {} and out port {}".format(pipe_id, cntrl_port, in_cntrl, out_cntrl))
	        P4Tables.setup_recirc_port_table(bfrt_info, self.target, targets[pipe_id], len(loopback_ports), pipe_id)
	        P4Tables.setup_circulate_table(bfrt_info, self.target, targets[pipe_id], loopback_ports, pipe_id)

                P4Tables.setup_wait_port_table(bfrt_info, self.target, targets[pipe_id], len(wait_ports), pipe_id)
	        P4Tables.setup_wait_table(bfrt_info, self.target, targets[pipe_id], wait_ports, pipe_id)
	        
                P4Tables.setup_check_dur_table(bfrt_info, self.target, targets[pipe_id], lower_time_bound, upper_time_bound, pipe_id)
	        P4Tables.setup_cntrl_table(bfrt_info, self.target, targets[pipe_id], in_cntrl, out_cntrl, cntrl_port, pipe_id)

        time.sleep(wait_time)
        if switch_send_cntrl:
	    logger.info("Sending control packet!")
	    self.send_cntrl_packet(cpu_interface, CONST_MAC_DST, CONST_MAC_SRC, in_cntrl, 0)

        for pipe_cfg in pipes_cfg:
            pipe_id = pipe_cfg['pipe_id']
            if pipe_cfg['active_pipe']:
                self.setup_timer_pkt_gen(bfrt_info, targets[pipe_id], CONST_MAC_DST, CONST_MAC_SRC, CONST_IP, payload_size, in_cntrl, cpu_interface, duration, nsperpkt, pipe_id, experiment_type)

        start_time = time.time()
	while (time.time() - start_time) < duration:
            # verify pktgen related counters
            for pipe_id in range(2):
                resp = pktgen_app_cfg_table.entry_get(
                    targets[pipe_id],
                    [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', app_id)])],
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
                rate_pkt_prod = pkt_value - prev_num_pkts
                logger.info("Rate of packet production is %d packets for 1 second", rate_pkt_prod)
                self.generate_pkts_over_time[pipe_id].append(rate_pkt_prod)
                prev_num_pkts = pkt_value

                # measure elapsed time - note this is SPOT checks, each value is just one randomly sample packet
                raw_et_reg_name = "MyIngress{}.raw_elapsed_time".format(pipe_id)
                raw_et_lat = self.get_register_number(bfrt_info, targets[pipe_id], raw_et_reg_name)
                self.raw_et_over_time[pipe_id].append(raw_et_lat)
                logger.info("[IN PROGRESS] Raw elapsed time: {}ns".format(raw_et_lat))
 
                # measure total number of packets 
                tot_cnt_name = "MyIngress{}.tot_packet_counter".format(pipe_id)
                pkts = self.get_counter_number(bfrt_info, tot_cnt_name)
                #low_tot_name = "MyIngress{}.total_pkt_cnt_lower".format(pipe_id)
                #high_tot_name = "MyIngress{}.total_pkt_cnt_higher".format(pipe_id)
	        #low_num_tot_pkts = self.get_register_number(bfrt_info, targets[pipe_id], low_tot_name)
	        #high_num_tot_pkts = self.get_register_number(bfrt_info, targets[pipe_id], high_tot_name)
                #pkts = (high_num_tot_pkts << 32) + low_num_tot_pkts - 1 # NOTE: the -1 is to correct for a small hack in how this tabulation is done in P4
                curr_dur = time.time() - start_time
                pkts_tput = pkts/float(curr_dur)
                #logger.info("[IN PROGRESS] Upper Append Packets: {} and Lower Append Packets: {}".format(high_num_tot_pkts, low_num_tot_pkts))
	        logger.info("[IN PROGRESS] Total Append Packets: {}".format(pkts))
	        logger.info("[IN PROGRESS] Append Throughput from PIPE {}: {}".format(pipe_id, pkts_tput))
                self.pkts_over_time[pipe_id].append(pkts)
                self.tput_over_time[pipe_id].append(pkts_tput)

                # measure average latency at this time period
                low_lat_reg_name = "MyIngress{}.latency_lower".format(pipe_id)
                high_lat_reg_name = "MyIngress{}.latency_higher".format(pipe_id)
                low_lat = self.get_register_number(bfrt_info, targets[pipe_id], low_lat_reg_name)
                high_lat = self.get_register_number(bfrt_info, targets[pipe_id], high_lat_reg_name)
                total_latency_ns = (high_lat << 31) + low_lat
                logger.info("[IN PROGRESS] High Latency: {}ns and Low Latency: {}ns".format(high_lat, low_lat))
                logger.info("[IN PROGRESS] Aggregate Latency: {}ns".format(total_latency_ns))
                avg_lat = 0
                if pkts != 0:
	            avg_lat = (total_latency_ns/float(pkts))/float(1000)
                    logger.info("[IN PROGRESS] Average latency: {} microseconds".format((total_latency_ns/float(pkts))/float(1000)))
                self.avg_lat_over_time[pipe_id].append(avg_lat)

                # measure queue size at this time period
                q_name = "MyEgress{}.queue_cnt".format(pipe_id)
                q_res = self.get_register_number(bfrt_info, targets[pipe_id], q_name)
                logger.info("[IN PROGRESS] Num of packets in the egress queue: {}".format(q_res))
                self.queue_cnt_over_time[pipe_id].append(q_res)
	    time.sleep(1)
        num_pipelines = len(pipes_cfg)

	for pipe_cfg in pipes_cfg:
	    pipe_id = pipe_cfg['pipe_id']
	    num_recirc_ports = pipe_cfg['total_recirc_ports']
            logger.info("Processing pipe {} info!".format(pipe_id))
            self.get_final_pktgen_stats(bfrt_info, targets[pipe_id], duration, nsperpkt, num_recirc_ports, pipe_id, num_pipelines, payload_size, experiment_type, lower_time_bound, upper_time_bound, 1000)
