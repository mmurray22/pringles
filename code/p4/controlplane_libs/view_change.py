"""
What edge cases need to be handled in the view change?
- Multiple switches trigger view change at the same time
- Resetting control packet timeout
- Which tables need to be updated when a failure takes place?
- What can fit in the data plane?
- Should view change processing be moved to egress? implications?
- How to stop all traffic processing?


What is the control and data flow of the system when view change occurs?
"""
import logging
import socket 
from scapy.all import *
import time
import sys
import copy
import random
import os
import yaml
from multiprocessing import Process
from helper import *
import ptf
import ptf.dataplane as dataplane
from ptf import config
import ptf.testutils as testutils
from bfruntime_client_base_tests import BfRuntimeTest
import pltfm_pm_rpc as pltfm_pm
import bfrt_grpc.client as gc
import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2
from ptf.thriftutils import *

from headers import *
from helper import *

class ViewChange:
    def __init__(self, view_num, view_member_threshold, num_shards, switch_id, defaultDstAddr, defaultSrcAddr, interface, timeout, logger, timeout_app_id):
        self.view = view_num
        self.prev_view = 0
        self.num_shards = num_shards
        self.view_member_threshold = view_member_threshold
        self.switch_id = switch_id
        self.dstAddr = defaultDstAddr
        self.srcAddr = defaultSrcAddr
        self.cpu_interface = interface
        self.timeout = timeout
        self.timeout_app_id = timeout_app_id
        self.logger = logger

    #def get_counter_number(bfrt_info, target, counter_name):
    #    counter = bfrt_info.table_get(counter_name)
    #    # 3. Request the entry at index 0
    #    # 'from_hw=True' ensures you get the latest count from the ASIC registers, 
    #    # not a cached software value.
    #    resp = counter.entry_get(
    #        target,
    #        [counter.make_key([gc.KeyTuple('$COUNTER_INDEX', 0)])],
    #        {"from_hw": True}
    #    )
    #    
    #    # 4. Parse the response
    #    try:
    #        data, _ = next(resp)
    #        data_dict = data.to_dict()
    #        
    #        # Tofino counters usually return a dictionary with '$COUNTER_SPEC_PKTS'
    #        return data_dict['$COUNTER_SPEC_PKTS']
    #    except gcBfruntimeRpcException as e:
    #        logger.info(e)
    #        return 0

    def send_start_view_change(self, interface, switch_highest_seq_no):
        self.prev_view = self.view
        self.view = self.view + 1
        pkt = Ether(dst=self.dstAddr, src=self.srcAddr, type=TYPE_VIEW_CHANGE)/ \
              StartViewChange(current_ring_view=self.view, 
                              old_ring_view=old_view, 
                              switch_id=0, 
                              sender_switch_id=self.switch_id, 
                              num_shards=self.num_shards, 
                              highest_quorum_seq_no=switch_highest_seq_no)/ \
              Canary(magic=165)
        logger.info("VIEW CHANGE PACKET WE ARE SENDING:")
	pkt.show()
        sendp(pkt, iface=interface, verbose=True)

    def send_view_change_ack(self, interface, highest_quorum_seq_no):
        for i in range(0, len(members)):
            final_view_pkt = Ether(dstAddr, srcAddr, type=TYPE_FINAL_VIEW)/ \
                             FinalizeViewChange(dst_id=members[i],
                                                current_ring_view=self.view,
                                                next_switch_id=members[(i+1)%len(members)],
                                                highest_quorum_seq_no=highest_quorum_seq_no)/ \
                             Canary(magic=165)
            logger.info("VIEW ACK PACKET WE ARE SENDING:")
	    pkt.show()
            sendp(final_view_pkt, iface=interface, verbose=True)
        
    def collect_responses(self, pkt_type):
        os.system("taskset -p -c 0 {}".format(os.getpid()))
        # This creates a raw socket exactly like tcpdump
        recv_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(0x0003))
        recv_sock.bind((interface, 0))
        # The Magic Constant: 18 (PACKET_IGNORE_OUTGOING)
        # This prevents the socket from receiving packets sent by the local host
        recv_sock.setsockopt(263, 18, 1)
        new_view_members = 0
        pkt_arr = []
        start_time = time.time()
        #while (time.time() - start_time) < self.timeout:
        #    print("[*] Tofino Receive Socket Open and Listening...")
        #    try:
        #        raw_data, addr = recv_sock.recvfrom(65535)
        #        pkttype = addr[2]
        #        if pkttype == 4:
        #            continue
        #        ether_pkt = Ether(raw_data)
        #        if ether_pkt.haslayer(RingType):
        #            if ether_pkt[RingType] == pkt_type:
        #                new_view_members += 1
        #                pkt_arr.append(ether_pkt)
        #return (new_view_members, pkt_arr)

    def prep_new_view(self, view_pkts):
        highest_quorum_seq_no = 0
        members = []
        ring_idx = 0
        for pkt in range(0, len(view_pkts)):
            # Get the sequence number seen by ALL switches in the new quorum from the last view
            if pkt[StartViewChange].old_ring_view < self.prev_view: # TODO what if previous view is higher?
                continue
            elif pkt[StartViewChange].old_ring_view > self.prev_view:
                error = "UH OH! This switch is STALE NOT HANDLED!"
                self.logger.error(error)
                raise ValueError(error)
            elif highest_quorum_seq_no == 0:
                highest_quorum_seq_no = pkt[StartViewChange].highest_quorum_seq_no
            elif pkt[StartViewChange].highest_quorum_seq_no < highest_quorum_seq_no:
                highest_quorum_seq_no = pkt[StartViewChange].highest_quorum_seq_no

            # Construct members of the new ring
            members.append(pkt[StartViewChange].switch_id)
        return (highest_quorum_seq_no, members)

    def run_view_change(self, bfrt_info, target, counter_name, duration):
        # Timeout has been surpassed, start the view change process!
        register_name = "map_from_gsn"
        highest_quorum_seq_no = get_register_info(bfrt_info, target, register_name, ref_pkt_it)
        self_pkt = self.send_start_view_change(self.cpu_interface, highest_quorum_seq_no)
        (new_view_members, pkt_arr) = self.collect_responses()
        pkt_arr.append(self_pkt) # TODO: push self_pkt to the front
        if new_view_members < self.view_member_threshold:
            error = "DID NOT GET ENOUGH MEMBERS"
            logger.info(error)
            raise ValueError(error)

        (highest_seq_no, members) = self.prep_new_view(pkt_arr)

        self_pkt = self.send_view_ack(self.cpu_interface, highest_seq_no, members)
        (new_view_members, pkt_arr) = self.collect_responses(pkt_type)
        pkt_arr.append(self_pkt)

        if new_view_members < self.view_member_threshold:
            error = "DID NOT GET ENOUGH MEMBERS"
            logger.info(error)
            raise ValueError(error)
        return True

    def setup_link_down_trigger(self, bfrt_info, target): # TODO NOT DONE, not highest priority
        logger.info("=============== Testing Packet Generator trigger by Timer ===============")
        pktgen_app_cfg_table = bfrt_info.table_get("$PKTGEN_APPLICATION_CFG")
        pktgen_pkt_buffer_table = bfrt_info.table_get("$PKTGEN_PKT_BUFFER")
        pktgen_port_cfg_table = bfrt_info.table_get("$PKTGEN_PORT_CFG")


        # timer pktgen app_id = 1 one shot 0
        app_id = self.timeout_app_id

        # Assuming not including the pkt gen header
        # Assuming in bytes
        pgen_pipe_id = 1
        src_port = self.pgen_port(pgen_pipe_id)
        # B x P to emulate 100G of client traffic
        p_count = 1 # packets per batch
        b_count = 1 # batch number
        buff_offset = 1  # generated packets' payload will be taken from the offset in buffer
        out_port = 192

        # build expected generated packets
        p = Ether(dst=self.dstAddr, src=self.srcAddr, type=TYPE_VIEW_CHANGE)/ \
              StartViewChange(current_ring_view=0, old_ring_view=0, switch_id=0, sender_switch_id=self.switch_id, num_shards=self.num_shards, highest_quorum_seq_no=0)
        raw_p = bytes(p)
        pktlen = len(raw_p)
        print("Length of the packet being sent is: {}".format(pktlen))
        try:
            logger.info("configure forwarding table")

            # Enable packet generation on the port
            logger.info("enable pktgen port")
	    #port_target = gc.Target(device_id=0, pipe_id=0x1)
            pktgen_port_cfg_table.entry_add(
                target,
                [pktgen_port_cfg_table.make_key([gc.KeyTuple('dev_port', src_port)])],
                [pktgen_port_cfg_table.make_data([gc.DataTuple('pktgen_enable', bool_val=True)])])

            # Configure the packet generation timer application
            logger.info("configure pktgen application")
            data = pktgen_app_cfg_table.make_data([gc.DataTuple('timer_nanosec', 100000),
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
                                                  '$PKTGEN_TRIGGER_PORT_DOWN')
            pktgen_app_cfg_table.entry_add(
                target,
                [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', g_timer_app_id)])],
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
                [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', g_timer_app_id)])],
                [pktgen_app_cfg_table.make_data([gc.DataTuple('app_enable', bool_val=True)],
                                                 '$PKTGEN_TRIGGER_PORT_DOWN')]
            )
            
        except gc.BfruntimeRpcException as e:
            print(e)
            raise e
        finally:
            pass
