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
from scapy.all import *
import time
import sys
import copy
import random
import os
import yaml
import json
from multiprocessing import Process
#from headers import *
import ptf
import ptf.dataplane as dataplane
from ptf import config
import ptf.testutils as testutils
from bfruntime_client_base_tests import BfRuntimeTest
import pltfm_pm_rpc as pltfm_pm
import bfrt_grpc.client as gc
import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2
from ptf.thriftutils import *


p4_program_name = "pktgen" ## TODO: Add as entry in the YAML

#loopback_port=68
cpu_pcie_port=192 # dunno what the ptf test script is doing
RCV_SIZE_DEFAULT = 4096
ETH_P_ALL = 0x03
RCV_TIMEOUT = 10000
END_EXPERIMENT = False
g_timer_app_id = 1

# Some useful defines
TYPE_IP= 0x800
TCP_PROTOCOL = 0x6
UDP_PROTOCOL = 0x11
TYPE_APPEND = 0x0860
TYPE_APPEND_WAIT = 0x0862
TYPE_APPEND_RESP = 0x0861
TYPE_READ = 0x0870
TYPE_READ_RESP = 0x0871
TYPE_TAIL = 0x0840
TYPE_SUB = 0x0850
TYPE_SUB_RESP = 0x0851
TYPE_CONTROL = 0x0820; 
TYPE_CONTROL_CHECK = 0x0880; 
TYPE_HELLO = 0x0888;
TYPE_MULTICAST = 0x0890


class PktgenTimerHeader(Packet):
    fields_desc = [
        BitField("_pad0", 0, 3),      # 3 bits
        BitField("pipe_id", 0, 2),    # 2 bits
        BitField("app_id", 0, 3),     # 3 bits (Total 8 bits / 1 byte)
        ByteField("_pad1", 0),        # 8 bits / 1 byte
        ShortField("batch_id", 0),    # 16 bits / 2 bytes
        ShortField("packet_id", 0)]    # 16 bits / 2 bytes

class RingType(Packet):
    fields_desc = [ XShortField("type", 0x0),
                    XShortField("exp_type", 1),
                    BitField("start_ts", 0, 48),
                    BitField("end_ts", 0, 48),
                    BitField("g_idx", 0, 32),
                    XShortField("raw_elapsed_time", 0),
                    BitField("raw_duration", 0, 32)]

bind_layers(PktgenTimerHeader, Ether)
bind_layers(Ether, IP, type=TYPE_IP)
bind_layers(IP, UDP)
bind_layers(UDP, RingType)

def ValueCheck(self, field, data_dict, expect_value):
    value = data_dict[field]
    if (value != expect_value):
        logger.info("Error: data %d, expect %d", value, expect_value)
        # assert(0)

def verify_multiple_packets(test, port, pkts=[], pkt_lens=[], device_number=0, tmo=None, slack=0):
    # tmo: time out; slack: nonezero-allow getting slack number packets less than expected.
    rx_pkt_status = [False] * len(pkts)
    if tmo is None:
        tmo = ptf.ptfutils.default_negative_timeout
    rx_pkts = 0
    while rx_pkts < len(pkts):
        (rcv_device, rcv_port, rcv_pkt, pkt_time) = testutils.dp_poll(
            test,
            device_number=device_number,
            port_number=port,
            timeout=tmo)
        if not rcv_pkt:
            if slack:
                test.assertTrue((slack > (len(pkts) - rx_pkts)),
                                "Timeout:Port[%d]:Got:[%d]:Allowed slack[%d]:Left[%d]\n" % (
                                port, rx_pkts, slack, len(pkts) - rx_pkts))
                return
            else:
                logger.info("No more packets but still expecting", len(pkts) - rx_pkts)
                for i, a_pkt in enumerate(pkts):
                    # print rx_pkt_status[i] #can be used for future debug when test case cannot pass.
                    # print format_packet(a_pkt) #can be used for future debug when test case cannot pass.
                    if not rx_pkt_status[i]:
                        logger.error("%s", str(a_pkt))
                test.assertTrue(False, "Timeout:Port:[%d]:Got[%d]:Left[%d]\n" % (port, rx_pkts, len(pkts) - rx_pkts))
                return
        rx_pkts = rx_pkts + 1
        found = False
        for i, a_pkt in enumerate(pkts):
            if str(a_pkt) == str(rcv_pkt[:pkt_lens[i]]) and not rx_pkt_status[i]:
                rx_pkt_status[i] = True
                found = True
                break
        if not found:
            test.assertTrue(False, "RxPort:[%u]:Pkt#[%u]:Pkt:%s:Unmatched\n" % (
                port, rx_pkts, ":".join("{:02x}".format(ord(c)) for c in rcv_pkt[:pkt_lens[0]])))



g_num_pipes = int(testutils.test_param_get("num_pipes"))
def port_to_pipe(port):
    local_port = port & 0x7F
    pipe = port >> 7
    return pipe

swports = []
for device, port, ifname in config["interfaces"]:
    pipe = port_to_pipe(port)
    if pipe < g_num_pipes:
        swports.append(port)
        swports.sort()

swports_0 = []
swports_1 = []
# the following method categorizes the ports in ports.json file as belonging to either of the pipes (0, 1, 2, 3)
for port in swports:
    pipe = port_to_pipe(port)
    if pipe == 0:
        swports_0.append(port)
    elif pipe == 1:
        swports_1.append(port)

# L1 Multicast Node Class
class L1Node:
    # BRI
    l1_node_id = 1

    def __init__(self, bfrt_test, target, rid):
        self.bfrt_test = bfrt_test
        self.rid = rid
        self.target = target
        self.xid = None
        self.l2_hdl = None
        self.mgid = None
        self.mbr_ports = []
        self.mbr_lags = []
        self.node_id = L1Node.l1_node_id
        L1Node.l1_node_id = L1Node.l1_node_id + 1
        # Creates node id entry in node table
        self.bfrt_test.node_table.entry_add(
            self.target,
            [self.bfrt_test.node_table.make_key([gc.KeyTuple('$MULTICAST_NODE_ID', self.node_id)])],
            [self.bfrt_test.node_table.make_data([gc.DataTuple('$MULTICAST_RID', self.rid),
                                                  gc.DataTuple('$MULTICAST_LAG_ID', int_arr_val=self.mbr_lags),
                                                  gc.DataTuple('$DEV_PORT', int_arr_val=self.mbr_ports)])])

    def __repr__(self):
        return "L1Node_" + str(hex(self.l1_hdl))

    def __str__(self):
        return str(hex(self.l1_hdl))

    def l1_hdl(self):
        return self.l1_hdl

    def get_rid(self):
        return self.rid

    def get_mbr_ports(self):
        return list(self.mbr_ports)

    def associate(self, mgid, xid):
        self.mgid = mgid
        self.xid = xid
        if xid is None:
            xid = 0
            use_xid = 0
        else:
            use_xid = 1
        self.bfrt_test.mgid_table.entry_mod_inc(
            self.target,
            [self.bfrt_test.mgid_table.make_key([gc.KeyTuple('$MGID', self.mgid)])],
            [self.bfrt_test.mgid_table.make_data([
                gc.DataTuple('$MULTICAST_NODE_ID', int_arr_val=[self.node_id]),
                gc.DataTuple('$MULTICAST_NODE_L1_XID_VALID', bool_arr_val=[use_xid]),
                gc.DataTuple('$MULTICAST_NODE_L1_XID', int_arr_val=[xid])])],
            bfruntime_pb2.TableModIncFlag.MOD_INC_ADD)
        # TODO: Add verification and enable entry get test
        '''
        resp = self.bfrt_test.get_table_entry(
            self.target,
            '$pre.mgid',
            [table.make_key([client.KeyTuple('$MGID', self.mgid)])],
            {"from_hw":False})
        data_dict = next(self.bfrt_test.parseEntryGetResponse(resp))
        '''

    def dissociate(self, test):
        # Not used
        pass

    def is_associated(self, test):
        # Not used
        pass

    def addMbrs(self, port_list, lag_list):
        if port_list is None and lag_list is None:
            return 0
        if port_list is not None:
            self.mbr_ports += port_list
            self.mbr_ports.sort()
        if lag_list is not None:
            for i in lag_list:
                assert i >= 0 and i <= 255
            self.mbr_lags += lag_list
            self.mbr_lags.sort()
        self.bfrt_test.node_table.entry_mod(
            self.target,
            [self.bfrt_test.node_table.make_key([gc.KeyTuple('$MULTICAST_NODE_ID', self.node_id)])],
            [self.bfrt_test.node_table.make_data([gc.DataTuple('$MULTICAST_RID', self.rid),
                                                  gc.DataTuple('$MULTICAST_LAG_ID', int_arr_val=self.mbr_lags),
                                                  gc.DataTuple('$DEV_PORT', int_arr_val=self.mbr_ports)])])

    def getPorts(self, rid, yid, h2):
        global t
        # Start with the individual ports on the L1 and then apply pruning
        port_list = self.get_mbr_ports()
        if self.rid == rid or rid == t.get_yid_tbl().global_rid():
            t.get_yid_tbl().prune_ports(yid, port_list)
        # If any ports are down, replace them with their backup
        # Since the backup table is initialized such that each port backups up
        # itself we can blindly take the backup table contents if the port is
        # down.
        if port_list:
            for x in range(len(port_list)):
                pport = port_list[x]
                pport_idx = portToBitIdx(pport)
                if t.sw_mask[pport_idx] == 1 or t.hw_mask[pport_idx] == 1:
                    port_list[x] = t.get_backup_port(pport)
        # For each LAG on the L1, pick the correct member port
        for lag_id in self.mbr_lags:
            lag = t.get_lag_tbl().getLag(lag_id)
            port = lag.getMbrByHash(h2, rid, self.rid, yid)
            if port is not None:
                port_list.append(port)
        return port_list

    def cleanUp(self, test):
        if self.mgid is not None:
            self.bfrt_test.mgid_table.entry_mod_inc(
                self.target,
                [self.bfrt_test.mgid_table.make_key([gc.KeyTuple('$MGID', self.mgid)])],
                [self.bfrt_test.mgid_table.make_data([gc.DataTuple('$MULTICAST_NODE_ID', int_arr_val=[self.node_id]),
                                                      gc.DataTuple('$MULTICAST_NODE_L1_XID_VALID',
                                                                   bool_arr_val=[0]),
                                                      gc.DataTuple('$MULTICAST_NODE_L1_XID', int_arr_val=[0])])],
                bfruntime_pb2.TableModIncFlag.MOD_INC_DELETE)
            self.mgid = None
        self.bfrt_test.node_table.entry_del(
            self.target,
            [self.bfrt_test.node_table.make_key([client.KeyTuple('$MULTICAST_NODE_ID', self.node_id)])])
        self.node_id = 0

# Multicast Tree
class MCTree:
    # Class for MGID table and cpmplete multicast engine config
    def __init__(self, bfrt_test, target, mgid):
        self.bfrt_test = bfrt_test
        self.target = target
        self.mgid = mgid
        self.bfrt_test.mgid_table.entry_add(
            self.target,
            [self.bfrt_test.mgid_table.make_key([gc.KeyTuple('$MGID', mgid)])])
        self.nodes = []
        self.ecmps = []

    def get_mgid(self):
        return self.mgid

    def get_first_node(self):
        return self.nodes[0]

    def add_node(self, rid, xid, mbr_ports, mbr_lags):
        l1 = L1Node(self.bfrt_test, self.target, rid)
        l1.addMbrs(mbr_ports, mbr_lags)
        l1.associate(self.mgid, xid)
        self.nodes.append(l1)

    def rmv_last_node(self):
        l1 = self.nodes[-1]
        self.nodes = self.nodes[:-1]
        l1.cleanUp(self.bfrt_test)

    def add_ecmp(self, grp, xid):
        grp.associate(self.mgid, xid)
        grp_tup = (grp, xid)
        self.ecmps.append(grp_tup)

    def reprogram(self):
        for l1 in self.nodes:
            l1.dissociate(self.bfrt_test)
        for grp, xid in self.ecmps:
            grp.dissociate(self.mgid)
        for l1 in self.nodes:
            l1.associate(self.test, self.mgid_hdl, l1.xid)
        for grp, xid in self.ecmps:
            grp.associate(self.mgid_hdl, xid)

    def cleanUp(self):
        for l1 in self.nodes:
            l1.cleanUp(self.bfrt_test)
        for grp, _ in self.ecmps:
            grp.dissociate(self.mgid)
        self.bfrt_test.mgid_table.entry_del(
            self.target,
            [self.bfrt_test.mgid_table.make_key([gc.KeyTuple('$MGID', self.mgid)])])
        self.nodes = []
        self.ecmps = []

    def get_ports(self, pkt_rid, pkt_xid, pkt_yid, pkt_hash1=0, pkt_hash2=0):
        port_data = []
        ecmp_data = []
        for l1 in self.nodes:
            if l1.xid is not None and pkt_xid == l1.xid:
                continue
            ports = l1.getPorts(pkt_rid, pkt_yid, pkt_hash2)
            port_data.append((l1.rid, ports))

        for grp, xid in self.ecmps:
            if xid is not None and pkt_xid == xid:
                continue
            l1 = grp.getMbrByHash(pkt_hash1)
            if l1 is not None:
                ports = l1.getPorts(pkt_rid, pkt_yid, pkt_hash2)
                ecmp_data.append((l1.rid, ports))
        return port_data + ecmp_data

    def print_tree(self):
        print("Dev:", self.dev, "MGID:", hex(self.mgid), "Num L1 Nodes:", len(self.nodes), "Num ECMPs:", len(self.ecmps))
        for l1 in self.nodes:
            if l1.xid is not None:
                print("  L1_Hdl:", l1.l1_hdl, "RID:", hex(l1.rid), "XID:", hex(
                    l1.xid), "Ports:", l1.mbr_ports, "LAGs:", l1.mbr_lags)
            else:
                print("  L1_Hdl:", l1.l1_hdl, "RID:", hex(
                    l1.rid), "XID:", l1.xid, "Ports:", l1.mbr_ports, "LAGs:", l1.mbr_lags)
        for grp, xid in self.ecmps:
            if xid is not None:
                print("  ECMP Hdl:", hex(grp), "XID:", hex(xid))
            else:
                print("  ECMP Hdl:", hex(grp))

swports = []
for device, port, ifname in config["interfaces"]:
    swports.append(port)
    swports.sort()

logger = logging.getLogger('Test')
if not len(logger.handlers):
    logger.addHandler(logging.StreamHandler())

class SequencingTest(BfRuntimeTest):
    ###################### GENERAL CONTROL PLANE SETUP ##############################3
    def setUp(self):
        client_id = 0
        BfRuntimeTest.setUp(self, client_id, p4_program_name)
    
    def load_config(self, file_path):
        with open(file_path, 'r') as file:
            try:
                # safe_load prevents execution of arbitrary code in YAML files
                config = yaml.safe_load(file)
                return config
            except yaml.YAMLError as exc:
                print("Error parsing YAML: ", exc)

    
    ###################### PORT SETUP ##############################
    def setup_all_switch_ports(self, target, loop_ports, no_loop_ports, port_speed, port_fec):
        print(loop_ports)
        print(no_loop_ports)
        print(port_speed)
        print(port_fec)
	for i in loop_ports:
	    self.port_setup(target, i, True, port_speed, port_fec)
        for i in no_loop_ports:
	    self.port_setup(target, i, False, port_speed, port_fec)

    def port_setup(self, target, port, use_loopback, port_speed, port_fec):
        logger.info("Test Port cfg table add and read operations")
        logger.info("PortCfgTest: Adding entry for port %d", port)
        if use_loopback:
            self.port_table.entry_add(
                target,
                [self.port_table.make_key([gc.KeyTuple('$DEV_PORT', port)])],
                [self.port_table.make_data([gc.DataTuple('$SPEED', str_val=port_speed),
                                            gc.DataTuple('$FEC', str_val=port_fec),
                                            gc.DataTuple('$TX_MTU', 9000),
                                            gc.DataTuple('$RX_MTU', 9000),
                                            gc.DataTuple('$PORT_ENABLE', bool_val=True),
                                            gc.DataTuple('$LOOPBACK_MODE', str_val="BF_LPBK_MAC_NEAR")])])
        else:
            self.port_table.entry_add(
                target,
                [self.port_table.make_key([gc.KeyTuple('$DEV_PORT', port)])],
                [self.port_table.make_data([gc.DataTuple('$SPEED', str_val=port_speed),
                                            gc.DataTuple('$FEC', str_val=port_fec),
                                            gc.DataTuple('$TX_MTU', 9000),
                                            gc.DataTuple('$RX_MTU', 9000),
                                            gc.DataTuple('$PORT_ENABLE', bool_val=True)])])
					    #gc.DataTuple('$N_LANES', 4)])])
    
    ###################### MATCH ACTION TABLE ##############################
    def setup_client_response_table(self, target, bfrt_info, client_ips): # Entry in client_ips: [IP, MAC, PORT]
        # ipv4_lpm
        table_ipv4 = bfrt_info.table_get("MyIngress.ipv4_lpm")
        table_ipv4.info.key_field_annotation_add("hdr.ipv4.dstAddr", "ipv4")
        table_ipv4.info.data_field_annotation_add("dstAddr", "MyIngress.ipv4_forward", "mac")
        for client_ip_and_port in client_ips:
            table_ipv4.entry_add(
                    target, 
                    [table_ipv4.make_key([gc.KeyTuple('hdr.ipv4.dstAddr', client_ip_and_port[0], prefix_len=32)])],
                    [table_ipv4.make_data(action_name="MyIngress.ipv4_forward", data_field_list_in=[gc.DataTuple(name="dstAddr", val=client_ip_and_port[1]), gc.DataTuple(name="port", val=int(client_ip_and_port[2]))])])

    def setup_circulate_table(self, bfrt_info, recirc_ports):
        # Circulate_table: If meta.circulate is set to 1, send packet to the loopback port
        # Set all pipes to be in different scopes. Also known as Single scope
        table_circulate = bfrt_info.table_get("MyIngress.circulate_table")
        table_circulate.info.key_field_annotation_add("meta.port_idx", "bit<8>")
        for i in range(0, len(recirc_ports)):
            print("Recirc port idx: {} and port {}".format(i, recirc_ports[i]))
            table_circulate.entry_add(
                    self.target, 
                    [table_circulate.make_key([gc.KeyTuple('meta.port_idx', i)])],
                    [table_circulate.make_data(action_name="MyIngress.circulate_port", data_field_list_in=[gc.DataTuple(name="port", val=recirc_ports[i])])])
    
    def setup_wait_table(self, bfrt_info, wait_ports):
        # Circulate_table: If meta.circulate is set to 1, send packet to the loopback port
        # Set all pipes to be in different scopes. Also known as Single scope
        table_wait = bfrt_info.table_get("MyIngress.wait_table")
        table_wait.info.key_field_annotation_add("meta.port_idx", "bit<8>")
        for i in range(0, len(wait_ports)):
            print("Wait port idx: {} and port {}".format(i, wait_ports[i]))
            table_wait.entry_add(
                    self.target, 
                    [table_wait.make_key([gc.KeyTuple('meta.port_idx', i)])],
                    [table_wait.make_data(action_name="MyIngress.wait_port", data_field_list_in=[gc.DataTuple(name="port", val=wait_ports[i])])])

    def setup_recirc_port_table(self, bfrt_info, tot_num_recirc_ports):
        table_recirc = bfrt_info.table_get("MyIngress.num_recirc_port_table")
        table_recirc.default_entry_set(
                self.target, 
                table_recirc.make_data(action_name="MyIngress.get_num_recirc", data_field_list_in=[gc.DataTuple(name="num_recirc_ports", val=tot_num_recirc_ports)]))

    def setup_wait_port_table(self, bfrt_info, tot_num_wait_ports):
        table_wait = bfrt_info.table_get("MyIngress.num_wait_port_table")
        table_wait.default_entry_set(
                self.target, 
                table_wait.make_data(action_name="MyIngress.get_num_wait", data_field_list_in=[gc.DataTuple(name="num_wait_ports", val=tot_num_wait_ports)]))

    def setup_check_dur_table(self, bfrt_info, lower_bound, upper_bound):
        # Cntrl ID -> Send Port
        table_check_dur = bfrt_info.table_get("MyEgress.check_duration")
        table_check_dur.info.key_field_annotation_add("hdr.ring_type.raw_elapsed_time", "bit<16>")

        table_check_dur.entry_add(
            self.target, 
            [table_check_dur.make_key([gc.KeyTuple('hdr.ring_type.raw_elapsed_time', low=lower_bound, high=upper_bound)])],
            [table_check_dur.make_data([], "MyEgress.update_type")])

    ###################### CONTROL PLANE RUNTIME ##############################3
    def receive_pkt_from_tofino(self, bfrt_info, target, interface, duration):
        os.system("taskset -p -c 0 {}".format(os.getpid()))
        # This creates a raw socket exactly like tcpdump
        recv_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(0x0003))
        recv_sock.bind((interface, 0))
        # The Magic Constant: 18 (PACKET_IGNORE_OUTGOING)
        # This prevents the socket from receiving packets sent by the local host
        recv_sock.setsockopt(263, 18, 1)

        print("[*] Tofino Receive Socket Open and Listening...")
        # Block until 1 packet is received
        start_time = time.time()
        while (time.time() - start_time) < duration:
            try:
                raw_data, addr = recv_sock.recvfrom(65535)
                pkttype = addr[2]
                if pkttype == 4:
                    #print("Discarding: This is an OUTGOING packet we just sent.")
                    continue
                ether_pkt = Ether(raw_data)
                pkttimer = PktgenTimerHeader(raw_data)
                if ether_pkt.haslayer(RingType):
                    if ether_pkt[RingType].type == TYPE_SUB:
                        print("RECEIVED a packet with TYPE_SUB")
                        ether_pkt.show()
                        if ether_pkt.haslayer(Subscribe):
                            self.update_stream_subscriber_table(bfrt_info, target, ether_pkt[Subscribe].stream_id, ether_pkt[Subscribe].subscribe_port)
                    elif ether_pkt[RingType].type == TYPE_APPEND:
                        print("RECEIVED a packet with TYPE_APPEND")
                        ether_pkt.show()
                    elif ether_pkt[RingType].type == TYPE_APPEND_RESP:
                        print("RECEIVED a packet with TYPE_APPEND_RESP")
                        ether_pkt.show()
                    elif ether_pkt[RingType].type == TYPE_SUB_RESP:
                        print("RECEIVED a packet with TYPE_SUB_RESP")
                        ether_pkt.show()
                    elif ether_pkt[RingType].type == TYPE_TAIL:
                        print("RECEIVED a packet with TYPE_TAIL")
                        ether_pkt.show()
                    elif ether_pkt.haslayer(Cntrl):
                        print("RECEIVED a packet with TYPE_CNTRL")
                        ether_pkt.show()
	        #elif pkttimer.haslayer(RingType):
		#    if pkttimer[RingType].type == TYPE_APPEND_RESP:
                #    	print("RECEIVED a packet with TYPE_APPEND_RESP")
                #        pkttimer.show()
            except socket.timeout:
	        print("Halted: Waiting for packet to send tofino request")
		continue
    
    def batch_check(self, bfrt_info, target, interface, duration):
        #os.system("taskset -p -c 0 {}".format(os.getpid()))
        # This creates a raw socket exactly like tcpdump
        recv_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(0x0003))
        recv_sock.bind((interface, 0))
        print("[*] Packet Gen Socket Open and Listening...")
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
	    #print(data_dict)
            print("Total Active Batches: {}".format(data_dict['MyIngress.active_batches.f1'][1]))

    def get_tm_queue(self, bfrt_info, target, interface, duration):
        recv_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(0x0003))
        recv_sock.bind((interface, 0))
        print("[*] Packet Gen Socket Open and Listening...")
        # Block until 1 packet is received

        start_time = time.time()
        while (time.time() - start_time) < duration:
	    time.sleep(1)
            # Tofino counters usually return a dictionary with '$COUNTER_SPEC_PKTS'
            tm_queue_tbl = bfrt_info.table_get("MyEgress.queue_cnt")
	    queue_val = tm_queue_tbl.entry_get(
	        target,
	        [tm_queue_tbl.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	        {"from_hw": True}
	    )
	    data, _ = next(queue_val)
	    data_dict = data.to_dict()
            print("Queue Activities: {}".format(data_dict["MyEgress.queue_cnt.f1"][1]))

    def process_pktgen(self, bfrt_info, target, interface, in_cntrl):
        # TODO
        #os.system("taskset -p -c 0 {}".format(os.getpid()))
        # This creates a raw socket exactly like tcpdump
        recv_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(0x0003))
        recv_sock.bind((interface, 0))
        learn_filter = bfrt_info.learn_get("digest_append")
        durations = []
        print("[*] Packet Gen Socket Open and Listening...")
        # Block until 1 packet is received

        start_time = time.time()
        duration = 10 # PARAMETERIZE TODO
	time.sleep(duration)
	# 1. Get a reference to the counter table
	counter_table = bfrt_info.table_get("MyIngress.tot_packet_counter")
	
	# 3. Request the entry at index 0
	# 'from_hw=True' ensures you get the latest count from the ASIC registers, 
	# not a cached software value.
	resp = counter_table.entry_get(
	    target,
	    [counter_table.make_key([gc.KeyTuple('$COUNTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	
	# 4. Parse the response
	data, _ = next(resp)
	data_dict = data.to_dict()
	
	# Tofino counters usually return a dictionary with '$COUNTER_SPEC_PKTS'
	pkts = data_dict['$COUNTER_SPEC_PKTS']
	print("Total Packets: {}".format(pkts))
        lat_table = bfrt_info.table_get("pipe.MyIngress.latency")
	lat_resp = lat_table.entry_get(
	    target,
	    [lat_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	data, _ = next(lat_resp)
	data_dict = data.to_dict()
	print(data_dict)
        
        ##### CNTRL PACKET
        cntrl_table = bfrt_info.table_get("pipe.MyIngress.cntrl_packet_counter")
	
	# 3. Request the entry at index 0
	# 'from_hw=True' ensures you get the latest count from the ASIC registers, 
	# not a cached software value.
	resp = cntrl_table.entry_get(
	    target,
	    [cntrl_table.make_key([gc.KeyTuple('$COUNTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	
	# 4. Parse the response
	data, _ = next(resp)
	data_dict = data.to_dict()
	
	# Tofino counters usually return a dictionary with '$COUNTER_SPEC_PKTS'
	pkts = data_dict['$COUNTER_SPEC_PKTS']
	print("Total Control Packets: {}".format(pkts))
        cntrl_lat_table = bfrt_info.table_get("pipe.MyIngress.ring_trip_time")
	cntrl_lat_resp = cntrl_lat_table.entry_get(
	    target,
	    [cntrl_lat_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	lat_data, _ = next(cntrl_lat_resp)
	data_dict = lat_data.to_dict()
	print(data_dict)
         
        ##### RECIRC PACKET
        recirc_table = bfrt_info.table_get("pipe.MyIngress.recirc_packet_counter")
	
	# 3. Request the entry at index 0
	# 'from_hw=True' ensures you get the latest count from the ASIC registers, 
	# not a cached software value.
	resp = recirc_table.entry_get(
	    target,
	    [recirc_table.make_key([gc.KeyTuple('$COUNTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	
	# 4. Parse the response
	data, _ = next(resp)
	data_dict = data.to_dict()
	
	# Tofino counters usually return a dictionary with '$COUNTER_SPEC_PKTS'
	pkts = data_dict['$COUNTER_SPEC_PKTS']
	print("Total Recirc Packets: {}".format(pkts))
        recirc_lat_table = bfrt_info.table_get("pipe.MyIngress.recirc_time")
	recirc_lat_resp = recirc_lat_table.entry_get(
	    target,
	    [recirc_lat_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	data, _ = next(recirc_lat_resp)
	data_dict = data.to_dict()
	print(data_dict)
        
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
    
    def setup_timer_pkt_gen(self, bfrt_info, target, pkt_gen_app_id, dstAddr, srcAddr, ip_addr, payload_size, in_cntrl, cpu_interface, duration, nsperpkt, num_pgen_ports, pipe_id, exp_type):
        logger.info("=============== Testing Packet Generator trigger by Timer ===============")
        pktgen_app_cfg_table = bfrt_info.table_get("$PKTGEN_APPLICATION_CFG")
        pktgen_pkt_buffer_table = bfrt_info.table_get("$PKTGEN_PKT_BUFFER")
        pktgen_port_cfg_table = bfrt_info.table_get("$PKTGEN_PORT_CFG")

        # timer pktgen app_id = 1 one shot 0
        app_id = 0 #pkt_gen_app_id
        nonce = 33
        payload = "P" * payload_size
        packed_ip = socket.inet_aton(ip_addr)
        ip_int = struct.unpack("!I", packed_ip)[0]
	p = Ether(dst=dstAddr, src=srcAddr, type=TYPE_IP)/ \
            IP(dst=ip_addr)/ \
	    UDP(dport=1234, sport=5678)/ \
	    RingType(type=TYPE_APPEND,exp_type=exp_type,start_ts=0,end_ts=0,g_idx=0,raw_elapsed_time=0)/ \
            Raw(load=payload)
        p.show()
        raw_p = bytes(p)
        pktlen = len(raw_p) #92 bytes: ethernet + ip + udp + ring_type + append
        print("Length of the packet being sent is: {}".format(pktlen))

        pgen_pipe_id = pipe_id
        for i in range(0, num_pgen_ports):
            src_port = self.pgen_port(pgen_pipe_id, i)
            # B x P to emulate 100G of client traffic
            p_count = 1 #132 #99 # packets per batch
            b_count = 1 # batch number
            buff_offset = 144 + (i * ((pktlen + 15) // 16) * 16) 
            #buff_offset = 144 + (i * pktlen)  # generated packets' payload will be taken from the offset in buffer
            out_port = 192 #swports[0]
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
                data = pktgen_app_cfg_table.make_data([gc.DataTuple('timer_nanosec', nsperpkt), #1000000), #10000), #3333), #1000
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
                                                      '$PKTGEN_TRIGGER_TIMER_ONE_SHOT') # PERIODIC
                pktgen_app_cfg_table.entry_add(
                    target,
                    [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', i)])],
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
                    [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', i)])],
                    [pktgen_app_cfg_table.make_data([gc.DataTuple('app_enable', bool_val=True)],
                                                     '$PKTGEN_TRIGGER_TIMER_PERIODIC')]
                )
                
            except gc.BfruntimeRpcException as e:
                print(e)
                raise e
            finally:
                pass
    
    def update_registers(self, bfrt_info):
	counter_table = bfrt_info.table_get("MyIngress.tot_packet_counter")
        counter_table.attribute_entry_scope_set(self.target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        low_lat_table = bfrt_info.table_get("MyIngress.latency_lower")
        low_lat_table.attribute_entry_scope_set(self.target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        high_lat_table = bfrt_info.table_get("MyIngress.latency_higher")
        high_lat_table.attribute_entry_scope_set(self.target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        cntrl_table = bfrt_info.table_get("MyIngress.cntrl_packet_counter")
        cntrl_table.attribute_entry_scope_set(self.target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        cntrl_lat_table = bfrt_info.table_get("MyIngress.ring_trip_time")
        cntrl_lat_table.attribute_entry_scope_set(self.target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

    def get_final_pktgen_stats(self, bfrt_info, target, duration, nsperpkt, num_recircs, payload_size):
        # 1. Get a reference to the counter table
        pkts = 0
        pkts_tput = 0
        counter_table = bfrt_info.table_get("MyIngress.tot_packet_counter")
        # 3. Request the entry at index 0
	# 'from_hw=True' ensures you get the latest count from the ASIC registers, 
	# not a cached software value.
	resp = counter_table.entry_get(
	    self.target,
	    [counter_table.make_key([gc.KeyTuple('$COUNTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	
	# 4. Parse the response
	data, _ = next(resp)
	data_dict = data.to_dict()
	
	# Tofino counters usually return a dictionary with '$COUNTER_SPEC_PKTS'
	pkts = data_dict['$COUNTER_SPEC_PKTS']
        pkts_tput = pkts/float(duration)
	print("======================Total Append Packets: {}".format(pkts))
	print("======================Append Throughput from PIPE ZERO: {}".format(pkts/float(duration)))
        #low_lat_table = bfrt_info.table_get("MyIngress.latency_lower")
	#low_lat_resp = low_lat_table.entry_get(
	#    target,
	#    [low_lat_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	#    {"from_hw": True}
	#)
	#low_data, _ = next(low_lat_resp)
	#low_data_dict = low_data.to_dict()
        #high_lat_table = bfrt_info.table_get("MyIngress.latency_higher")
	#high_lat_resp = high_lat_table.entry_get(
	#    target,
	#    [high_lat_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	#    {"from_hw": True}
	#)
	#high_data, _ = next(high_lat_resp)
	#high_data_dict = high_data.to_dict()
        #low_lat = low_data_dict["MyIngress.latency_lower.f1"][0]
        #high_lat = high_data_dict["MyIngress.latency_higher.f1"][0]
        #total_latency_ns = (high_lat << 32) + low_lat
        #print("High Latency: {}ns and Low Latency: {}ns".format(high_lat, low_lat))
        #print("Aggregate Latency: {}ns".format(total_latency_ns))
        
        raw_duration_table = bfrt_info.table_get("MyIngress.raw_duration")
	raw_duration_resp = raw_duration_table.entry_get(
	    target,
	    [raw_duration_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	raw_dur_data, _ = next(raw_duration_resp)
	raw_dur_data_dict = raw_dur_data.to_dict()
        raw_dur_lat = raw_dur_data_dict["MyIngress.raw_duration.f1"][0]
        print("Raw duration: {}ns".format(raw_dur_lat))

        raw_et_table = bfrt_info.table_get("MyIngress.raw_elapsed_time")
	raw_et_resp = raw_et_table.entry_get(
	    target,
	    [raw_et_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	raw_et_data, _ = next(raw_et_resp)
	raw_et_data_dict = raw_et_data.to_dict()
        raw_et_lat = raw_et_data_dict["MyIngress.raw_elapsed_time.f1"][0]
        print("Raw elapsed time: {}ns".format(raw_et_lat))
        total_latency_ns = 0

        raw_duration_table = bfrt_info.table_get("MyEgress.start_ts")
	raw_duration_resp = raw_duration_table.entry_get(
	    target,
	    [raw_duration_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	raw_dur_data, _ = next(raw_duration_resp)
	raw_dur_data_dict = raw_dur_data.to_dict()
        raw_dur_lat = raw_dur_data_dict["MyEgress.start_ts.f1"][0]
        print("Start ts: {}ns".format(raw_dur_lat))

        raw_et_table = bfrt_info.table_get("MyEgress.end_ts")
	raw_et_resp = raw_et_table.entry_get(
	    target,
	    [raw_et_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	raw_et_data, _ = next(raw_et_resp)
	raw_et_data_dict = raw_et_data.to_dict()
        raw_et_lat = raw_et_data_dict["MyEgress.end_ts.f1"][0]
        print("End time: {}ns".format(raw_et_lat))
        total_latency_ns = 0


        avg_lat = 0
        if pkts != 0:
	    avg_lat = (total_latency_ns/float(pkts))/float(1000)
            print("======================Average latency: {} microseconds".format((total_latency_ns/float(pkts))/float(1000)))
        
        cntrl_pkts = 0
        cntrl_agg_lat = 0
        target_dest = "/root/pipeAll_{}_nsperpkt.json".format(nsperpkt)
        self.write_experiment_telemetry(target_dest, pkts, pkts_tput, total_latency_ns, avg_lat, cntrl_pkts, cntrl_agg_lat, duration, nsperpkt, num_recircs, payload_size)
        

    def write_experiment_telemetry(self, file_path, total_pkts, throughput, total_lat, avg_lat, total_cntrl, cntrl_lat, duration, nsperpkt, num_recircs, payload_size):
        """
        Serializes benchmarking telemetry safely into a standardized JSON payload structure.
        """
        # 1. Map data directly into a standard Python dictionary layout
        telemetry_payload = {
            "total_number_of_packets": int(total_pkts),
            "throughput_pps": float(throughput),
            "total_latency_ns": float(total_lat),
            "average_latency_us": float(avg_lat),
            "total_number_of_control_packets": int(total_cntrl),
            "control_packet_latency_ns": float(cntrl_lat),
            "nsperpkt": int(nsperpkt),
            "duration": int(duration),
            "total_loopback": int(num_recircs),
            "payload_size": int(payload_size)
        }
        
        # 2. Open and write out using a safe with context block
        try:
            with open(file_path, 'w') as json_file:
                # indent=4 formats the output nicely for human-readable debugging
                json.dump(telemetry_payload, json_file, indent=4)
            print("[INFO] Telemetry footprint written successfully to {}".format(file_path))
        except IOError as e:
            print("[ERROR] Failed writing metrics to destination layout: {}".format(e))
    
    
    ###################### TESTING FUNCTIONS ##############################3
    def runTest(self):
	filepath = "/tmp/switch_config.yaml"
        print(filepath)
	data = self.load_config(filepath)
	os.system("taskset -p -c 1 {}".format(os.getpid()))
	dev_number = data['device_number']

	# Device-wide startup (once per physical switch -- these resources are shared
	# across both pipes, NOT duplicated per pipe: PRE/multicast lives in the Traffic
	# Manager, not in a specific pipe, and there is only one bfrt_info session for
	# this whole ASIC).
	bfrt_info = self.interface.bfrt_info_get(p4_program_name)

        self.port_table = bfrt_info.table_get("$PORT")
        self.port_hdl_info_table = bfrt_info.table_get("$PORT_HDL_INFO")
        self.port_fp_idx_info_table = bfrt_info.table_get("$PORT_FP_IDX_INFO")
        self.port_str_info_table = bfrt_info.table_get("$PORT_STR_INFO")
	self.dataplane = ptf.dataplane_instance
        self.dataplane.flush()
        
        # Get all PRE table objects
        self.mgid_table = bfrt_info.table_get("$pre.mgid")
        self.node_table = bfrt_info.table_get("$pre.node")
        self.ecmp_table = bfrt_info.table_get("$pre.ecmp")
        self.lag_table = bfrt_info.table_get("$pre.lag")
        self.prune_table = bfrt_info.table_get("$pre.prune")
        self.mirror_cfg_table = bfrt_info.table_get("$mirror.cfg")
        
        # Setup some of the multicast tables (device-wide, shared across both pipes)
        self.global_group_id = 1
        self.sid = 1
        self.stream_sub_dict = {}

	# Parameters
	listen_ip = "0.0.0.0" # Listen on all interfaces
        listen_port = 5005
        CONST_MAC_DST = "00:90:fb:70:65:71"
        CONST_MAC_SRC = "00:25:90:53:e6:00"
        CONST_IP = "10.229.49.9"

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
        wait_time = data['wait_time']
        # Range of time to wait before forwarding packet to storage section
        lower_time_bound = data['lower_time_bound']
        upper_time_bound = data['upper_time_bound']
        experiment_type = data['experiment_type']
	
        warmup = 5
        cooldown = 5
        buffer_time = 10
        total_time = duration + warmup + cooldown + buffer_time

        run_setup = True
        use_pktgen = True

	# NEW: per-pipe sections. Each entry describes everything that is genuinely
	# scoped to ONE pipe: which devport is used for cntrl forwarding, which ports
	# are held in loopback, which front-panel ports belong to that pipe, how many
	# recirc ports are active, and whether this pipe is the one that kicks off the
	# ring's first control packet.
	pipes_cfg = data['pipes']
        experiment_type = data['experiment_type']
        print("Experiment type: {}".format(experiment_type))

	tm_threads = []
        self.target = gc.Target(device_id=0, pipe_id=0xffff)
        self.target0 = gc.Target(device_id=0, pipe_id=0x00)
        self.target1 = gc.Target(device_id=0, pipe_id=0x01)
        #self.update_registers(bfrt_info)

        loop_ports = set()
        no_loop_ports = set()
        pipe_cfg = pipes_cfg[0]
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

	self.setup_all_switch_ports(self.target, loop_ports, no_loop_ports, port_speed, port_fec)

	loopback_ports = pipe_cfg['loopback_ports']
	wait_ports = pipe_cfg['wait_ports']
	ports_to_ring_members = pipe_cfg['ports_to_ring_members']
	tot_num_recirc_ports = pipe_cfg['total_recirc_ports']
	list_of_switch_ports = pipe_cfg['switch_ports']
        pipe_id = pipe_cfg['pipe_id']


	print("ports to ring members: {}".format(ports_to_ring_members))

	####################### SETUP MATCH-ACTION TABLES (this pipe) ##########################
	print("Number of loopback ports: {}".format(len(loopback_ports)))
	self.setup_recirc_port_table(bfrt_info, len(loopback_ports))
	self.setup_circulate_table(bfrt_info, loopback_ports)

        self.setup_wait_port_table(bfrt_info, len(wait_ports))
	self.setup_wait_table(bfrt_info, wait_ports)
	
        self.setup_check_dur_table(bfrt_info, lower_time_bound, upper_time_bound)

        time.sleep(wait_time)
        self.setup_timer_pkt_gen(bfrt_info, self.target0, 1, CONST_MAC_DST, CONST_MAC_SRC, CONST_IP, payload_size, in_cntrl, cpu_interface, duration, nsperpkt, 1, pipe_cfg['pipe_id'], experiment_type)

	## Both pipes' generators are now running concurrently -- wait once for the
	## shared experiment duration rather than once per pipe.
	time.sleep(duration)

	tot_num_recirc_ports = pipe_cfg['total_recirc_ports']
        print("Processing pipe {} info!".format(pipe_id))
        self.get_final_pktgen_stats(bfrt_info, self.target, duration, nsperpkt, tot_num_recirc_ports, payload_size)
