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
import threading

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
    def __init__(self, view_num, view_member_threshold, switch_id, defaultDstAddr, defaultSrcAddr, interface, cpu_port, timeout, duration, logger, active_switches, view_change_switch):
        # View numbers
        self.view = view_num
        self.prev_view = 0

        # Number of switches needed to create new ring
        self.view_member_threshold = view_member_threshold

        # ID of the switch the current control plane is running on 
        self.switch_id = switch_id

        # Destination and Source Address 
        self.dstAddr = defaultDstAddr
        self.srcAddr = defaultSrcAddr

        # CPU Interface name and port
        self.cpu_interface = interface
        self.cpu_port = cpu_port

        # View times: View change phases timeouts and experiment duration
        self.view_timeout = timeout
        self.exp_duration = duration + 5

        # Queue of incoming packets
        self.received_start_view = []
        self.received_finalize_view = []

        # Link down timeout id
        #self.timeout_app_id = timeout_app_id

        self.active_switches = active_switches
	self.view_change_leader = view_change_switch

        # Logger
        self.logger = logger

        self.arr_lock = threading.Lock()

        self.trigger_sent = 0
        self.trigger_wait_end = 0
        self.last_trigger_reply = 0
        self.finalize_sent = 0
        self.finalize_wait_end = 0
        self.last_finalize_reply = 0

    #def __exit__(self):
    
    ##################################### Helper functions (TODO deduplicate) ############################
    def get_register_number(self, bfrt_info, target, register_name, idx, pipe_idx=0):
        reg_table = bfrt_info.table_get(register_name)
	table_resp = reg_table.entry_get(
	    target,
	    [reg_table.make_key([gc.KeyTuple('$REGISTER_INDEX', idx)])],
	    {"from_hw": True}
	)
	reg_data, _ = next(table_resp)
	reg_data_dict = reg_data.to_dict()
        register_entry_name = register_name + ".f1"
        return reg_data_dict[register_entry_name][pipe_idx] if len(reg_data_dict[register_entry_name]) > 1 else reg_data_dict[register_entry_name][0]
   

    ##################################### Send View Change Packets ############################
    def send_start_view_change(self, upper_seq_no, lower_seq_no, prev_view):
        its = 0
	pkts = []
        for active_switch_id in self.active_switches:
            pkt = Ether(dst=self.dstAddr, src=self.srcAddr, type=TYPE_VIEW_CHANGE)/ \
                  TriggerViewChange(leader_switch_id=self.view_change_leader,#self.switch_id, 
                                  current_ring_view=prev_view, 
                                  view_member_switch_id=active_switch_id,
                                  highest_seen_upper_seq_no=upper_seq_no,
                                  highest_seen_lower_seq_no=lower_seq_no,
                                  hops=0)/ \
                  Canary(magic=MAGIC_VALUE)
            #self.logger.info("START VIEW PACKET WE ARE SENDING:") # TODO change naming of highest_seen_seq_no!
	    #pkt.show()
	    pkts.append(pkt)
            its += 1
        sendp(pkts, iface=self.cpu_interface, verbose=True)
        #print("============================TOTAL NUMBER OF TRIGGERS SENT IS: {}".format(its))

    def send_view_change_ack(self, highest_quorum_seq_no, members):
        upper_seq_no = (highest_quorum_seq_no >> 32) & 0xFFFFFFFF
        lower_seq_no = highest_quorum_seq_no & 0xFFFFFFFF
	pkts = []
        
        for i in range(0, len(members)):
            final_view_pkt = Ether(dst=self.dstAddr, src=self.srcAddr, type=TYPE_VIEW_ACK)/ \
                             FinalizeViewChange(leader_switch_id=self.switch_id,
                                                view_member_switch_id=members[i],
                                                current_ring_view=self.view,
                                                next_switch_id=members[(i+1)%len(members)],
                                                highest_quorum_upper_seq_no=upper_seq_no,
                                                highest_quorum_lower_seq_no=lower_seq_no,
                                                hops=0)/ \
                             Canary(magic=MAGIC_VALUE)
	    pkts.append(final_view_pkt)
            #self.logger.info("FINAL VIEW PACKET WE ARE SENDING:")
	    #final_view_pkt.show()
        sendp(pkts, iface=self.cpu_interface, verbose=True)
        
    ##################################### Send View Change Packets ############################
    def is_returned(self, p):
        if not (Ether in p and p[Ether].type in (TYPE_VIEW_CHANGE, TYPE_VIEW_ACK)):
            return False
        if TriggerViewChange in p:
	    t = p[TriggerViewChange]
            return self.switch_id == self.view_change_leader
    	if FinalizeViewChange in p:
            f = p[FinalizeViewChange]
            return self.switch_id == self.view_change_leader and f.current_ring_view == self.view
        return False

    def handle_pkt(self, ether_pkt):
        if ether_pkt[Ether].type == TYPE_VIEW_CHANGE:
	    print("[sniffer %d] GOT A TriggerView PACKET!" % self.switch_id)
            startView = ether_pkt[TriggerViewChange]
            startView.show()
            with self.arr_lock:
                self.received_start_view.append(startView)
                self.last_trigger_reply = time.time()
                self.last_finalize_reply = time.time()
        elif ether_pkt[Ether].type == TYPE_VIEW_ACK:
	    print("[sniffer %d] GOT A FinalizeView PACKET!" % self.switch_id)
            finalView = ether_pkt[FinalizeViewChange]
            finalView.show()
            with self.arr_lock:
	    	self.received_finalize_view.append(finalView)
                self.last_trigger_reply = time.time()
                self.last_finalize_reply = time.time()

    def collect_responses(self):
        sniff(iface = self.cpu_interface, L2socket=conf.L2socket, lfilter=self.is_returned, timeout=self.exp_duration, prn = lambda x: self.handle_pkt(x))
        #sniff(iface=self.cpu_interface, L2socket=conf.L2socket, timeout=self.exp_duration, prn = lambda x: self.handle_pkt(x))
        print("Sniffing is over!")

    ##################################### Helper Functions for View Change ############################
    def prep_new_view(self, view_pkts):
    	print("AT PREP NEW VIEW")
	print(view_pkts)
        highest_quorum_seq_no = None
        members = []
        ring_idx = 0
        for p in view_pkts:
	    if p[TriggerViewChange].current_ring_view < self.prev_view:
	    	continue
            highest_seen_seq_no = ((p[TriggerViewChange].highest_seen_upper_seq_no & 0xFFFFFFFF) << 32) | (p[TriggerViewChange].highest_seen_lower_seq_no & 0xFFFFFFFF)
            if highest_quorum_seq_no is None or highest_seen_seq_no < highest_quorum_seq_no:
                highest_quorum_seq_no = highest_seen_seq_no
	
	if highest_quorum_seq_no == None: # This means we have NO MEMBERS from the current ring??? 
	    return (0, members)

	#members = sorted([pkt[TriggerViewChange].view_member_switch_id for pkt in view_pkts]) <-- No sorting in this case cause of weird topology
	responders = set(p[TriggerViewChange].view_member_switch_id for p in view_pkts)
	print(responders)
	members = [s for s in self.active_switches if s in responders]
	print(members)
	return (highest_quorum_seq_no, members)

    #################### View Change Leader #################################
    def run_view_change(self, bfrt_info, target, pkt_it, duration, pipe_id, propose_new_view, prev_view, timeout_trigger, external_trigger):
        # Timeout has been surpassed, start the view change process!
        self.prev_view = prev_view
	self.view = propose_new_view
	
        # Send StartView packet 
	if timeout_trigger:
            self.send_start_view_change(0, 0, prev_view)
            self.trigger_sent = time.time()
        sleep_time = .001
        total_time_waited = 0
        list_of_start_view_pkts = []
        time.sleep(self.view_timeout)
        self.trigger_wait_end = time.time()
	with self.arr_lock:
	    list_of_start_view_pkts = self.received_start_view
            if len(list_of_start_view_pkts) < self.view_member_threshold:
                error = "DID NOT GET ENOUGH MEMBERS TO START THE VIEW"
                self.logger.warn(error)
                return (0, 0)
            (highest_seq_no, members) = self.prep_new_view(list_of_start_view_pkts)
	if len(members) < self.view_member_threshold:
            error = "DID NOT GET ENOUGH MEMBERS TO START THE VIEW"
            self.logger.warn(error)
            return (0, 0)
        self.send_view_change_ack(highest_seq_no, members)
        self.finalize_sent = time.time()
        total_time_waited = 0
        list_of_finalize_view_pkts = []
        time.sleep(self.view_timeout)
        self.finalize_wait_end = time.time()
	with self.arr_lock:
            list_of_finalize_view_pkts = self.received_finalize_view
            if len(list_of_finalize_view_pkts) < self.view_member_threshold:
                error = "DID NOT GET ENOUGH MEMBERS TO FINALIZE THE VIEW"
                self.logger.warn(error)
                return (0,0)
	    self.received_start_view = []
            self.received_finalize_view = []
        return (self.view, highest_seq_no)
