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
    def __init__(self, view_num, view_member_threshold, switch_id, defaultDstAddr, defaultSrcAddr, interface, cpu_port, timeout, duration, logger, active_switches):
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

        # Logger
        self.logger = logger

        self.arr_lock = threading.Lock()

    #def __exit__(self):
    
    ##################################### Helper functions (TODO deduplicate) ############################
    def get_register_number(self, bfrt_info, target, register_name, idx):
        reg_table = bfrt_info.table_get(register_name)
	table_resp = reg_table.entry_get(
	    target,
	    [reg_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	reg_data, _ = next(table_resp)
	reg_data_dict = reg_data.to_dict()
        register_entry_name = register_name + ".f1"
        return reg_data_dict[register_entry_name][idx]
   

    ##################################### Send View Change Packets ############################
    def inject_view_change_trigger(self):
        pkt = Ether(dst=self.dstAddr, src=self.srcAddr, type=TYPE_VIEW_INDUCE)/ \
              InduceViewChange(drop_cntrl=1)/ \
              Canary(magic=MAGIC_VALUE)
        self.logger.info("TRIGGER PACKET WE ARE SENDING:")
	pkt.show()
        sendp(pkt, iface=self.cpu_interface, verbose=True)

    def send_start_view_change(self, switch_highest_seq_no):
        self.prev_view = self.view
        self.view = self.view + 1
        its = 0
        for i in range(0, len(self.active_switches)):
            pkt = Ether(dst=self.dstAddr, src=self.srcAddr, type=TYPE_VIEW_CHANGE)/ \
                  TriggerViewChange(leader_switch_id=self.switch_id, 
                                  current_ring_view=self.prev_view, 
                                  view_member_switch_id=self.active_switches[i],
                                  highest_seen_seq_no=switch_highest_seq_no,
                                  hops=0)/ \
                  Canary(magic=MAGIC_VALUE)
            #self.logger.info("START VIEW PACKET WE ARE SENDING:") # TODO change naming of highest_seen_seq_no!
	    #pkt.show()
            sendp(pkt, iface=self.cpu_interface, verbose=True)
            its += 1
        #print("============================TOTAL NUMBER OF TRIGGERS SENT IS: {}".format(its))

    def send_view_change_ack(self, highest_quorum_seq_no, members):
        for i in range(0, len(members)):
            final_view_pkt = Ether(dst=self.dstAddr, src=self.srcAddr, type=TYPE_VIEW_ACK)/ \
                             FinalizeViewChange(leader_switch_id=self.switch_id,
                                                view_member_switch_id=members[i],
                                                current_ring_view=self.view,
                                                next_switch_id=members[(i+1)%len(members)],
                                                highest_quorum_seq_no=highest_quorum_seq_no,
                                                hops=0)/ \
                             Canary(magic=MAGIC_VALUE)
            #self.logger.info("FINAL VIEW PACKET WE ARE SENDING:")
	    #final_view_pkt.show()
            sendp(final_view_pkt, iface=self.cpu_interface, verbose=True)
        
    ##################################### Send View Change Packets ############################
    def is_returned(self, p):
        return (Ether in p and (p[Ether].type == TYPE_VIEW_CHANGE or p[Ether].type == TYPE_VIEW_ACK)
                and (TriggerViewChange in p or FinalizeViewChange in p))

    def handle_pkt(self, ether_pkt):
        if ether_pkt[Ether].type == TYPE_VIEW_CHANGE:
            #print("GOT A TriggerView PACKET!")
            startView = ether_pkt[TriggerViewChange]
            #startView.show()
            #self.logger.info("TriggerView pkt: leader={} member={} seq_no={}".format(startView.leader_switch_id, startView.view_member_switch_id, startView.highest_seen_seq_no))
            with self.arr_lock:
                self.received_start_view.append(startView)
        elif ether_pkt[Ether].type == TYPE_VIEW_ACK:
            #print("GOT A FinalizeView PACKET!")
            finalView = ether_pkt[FinalizeViewChange]
            #finalView.show()
            #self.logger.info("FinalizeView pkt: next_switch={} member={} start_seq_no={} view={}".format(finalView.next_switch_id, finalView.view_member_switch_id, finalView.highest_quorum_seq_no, finalView.current_ring_view))
            self.received_finalize_view.append(finalView)

    def collect_responses(self):
        sniff(iface = self.cpu_interface, L2socket=conf.L2socket, lfilter=self.is_returned, timeout=self.exp_duration, prn = lambda x: self.handle_pkt(x))
        #sniff(iface=self.cpu_interface, L2socket=conf.L2socket, timeout=self.exp_duration, prn = lambda x: self.handle_pkt(x))
        print("Sniffing is over!")

    ##################################### Helper Functions for View Change ############################
    def prep_new_view(self, view_pkts):
        highest_quorum_seq_no = 0
        members = []
        ring_idx = 0
        for pkt in view_pkts:
            # TODO? Get the sequence number seen by ALL switches in the new quorum from the last view

            # Get the highest quorum sequence number
            if highest_quorum_seq_no == 0:
                highest_quorum_seq_no = pkt[TriggerViewChange].highest_seen_seq_no
            elif pkt[TriggerViewChange].highest_seen_seq_no < highest_quorum_seq_no:
                highest_quorum_seq_no = pkt[TriggerViewChange].highest_seen_seq_no

            # Construct members of the new ring
            members.append(pkt[TriggerViewChange].view_member_switch_id)
        return (highest_quorum_seq_no, members)

    #################### View Change Leader #################################
    def run_view_change(self, bfrt_info, target, pkt_it, duration, pipe_id):
        # Timeout has been surpassed, start the view change process!
        
        # Start dataplane response sniffing thread
        #print("========================================MADE IT HERE!!!!!")
        register_name = "MyIngress{}.map_from_gsn".format(pipe_id)
        highest_quorum_seq_no = self.get_register_number(bfrt_info, target, register_name, pkt_it)
        #print("======================================= Packet iteration: {}, Highest quorum sequence number: {}".format(pkt_it, highest_quorum_seq_no))

        # Send StartView packet 
        self.send_start_view_change(highest_quorum_seq_no)
        #sleep_time = float("5e-9")
        sleep_time = .0000000005
        total_time_waited = 0
        start_time = time.time()
        list_of_start_view_pkts = []
        while (time.time() - start_time) < self.view_timeout:
            with self.arr_lock:
                list_of_start_view_pkts = self.received_start_view
                if  len(self.received_start_view) < self.view_member_threshold:
                    #print("Only got {}, waiting for {} StartView packets".format(len(self.received_start_view), self.view_member_threshold))
                    continue
                else:
                    break
            total_time_waited += sleep_time
            time.sleep(sleep_time)
                    
        
        if len(list_of_start_view_pkts) < self.view_member_threshold:
            error = "DID NOT GET ENOUGH MEMBERS TO START THE VIEW"
            self.logger.warn(error)
            raise ValueError(error) # TODO DO BETTER!
        #else:
            #print("IT TOOK {}s to get START VIEW QUORUM".format(total_time_waited))
            #print("=============================AND WE GOT {} number of members".format(len(list_of_start_view_pkts)))
        self.received_start_view = []

        (highest_seq_no, members) = self.prep_new_view(list_of_start_view_pkts)
        self.send_view_change_ack(highest_seq_no, members)

        start_time = time.time()
        total_time_waited = 0
        list_of_finalize_view_pkts = []
        while (time.time() - start_time) < self.view_timeout:
            list_of_finalize_view_pkts = self.received_finalize_view
            if  len(self.received_finalize_view) < self.view_member_threshold:
                #print("Only got {}, waiting for {} FinalView packets".format(len(self.received_finalize_view), self.view_member_threshold))
                time.sleep(sleep_time)
                continue
            else:
                break

        if len(list_of_finalize_view_pkts) < self.view_member_threshold:
            error = "DID NOT GET ENOUGH MEMBERS TO FINALIZE THE VIEW"
            self.logger.warn(error)
            raise ValueError(error)
        #else:
            #print("IT TOOK {}s to get FINALIZE VIEW QUORUM".format(total_time_waited))
            #print("=============================AND WE GOT {} number of members".format(len(list_of_finalize_view_pkts)))
        self.active_switches = members # TODO: Check semantics but I think this is sound
        self.received_finalize_view = []
        return (self.view, highest_seq_no)
