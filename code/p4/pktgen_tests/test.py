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


p4_program_name = "sequencing_only"

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

class Cntrl(Packet):
    fields_desc = [ BitField("global_seq_no", 0, 32),
                    BitField("ring_view", 0, 16),
                    BitField("pkt_id", 0, 16)]

class Cntrl_Check(Packet):
    fields_desc = [ IntField("switch_global_seq_no", 0)]

class Hello(Packet):
    fields_desc = [ BitField("hello", 0, 32)]

class RingType(Packet):
    fields_desc = [ XShortField("type", 0x0),
                    BitField("num_entries", 1, 16),
                    BitField("shard_id", 0, 32),
                    BitField("switch_to_process", 1, 32)]
class Append(Packet):
    fields_desc = [ BitField("nonce", 0, 32),
                    BitField("payload_size", 0, 16),
                    BitField("stream_id", 0, 32),
                    BitField("g_idx", 0, 32), # TODO: 64
                    BitField("cntrl_pkt_it", 0, 16), # TODO: 64
                    BitField("client_ip", 0, 32),
                    ShortField("recv_port", 0),
                    BitField("start_ts", 0, 48)]
class Read(Packet):
    fields_desc = [ BitField("nonce", 0, 32),
                    BitField("payload_size", 0, 32),
                    BitField("stream_id", 0, 32),
                    BitField("g_idx", 0, 32),
                    BitField("ring_view", 0, 16),
                    ShortField("recv_port", 0),
                    BitField("client_ip", 0, 32),
                    BitField("start_ts", 0, 48)]
class Subscribe(Packet):
    fields_desc = [ BitField("g_idx", 0, 32),
                    BitField("stream_id", 0, 32),
                    BitField("subscribe", 0, 32),
                    BitField("client_ip", 0, 32),
                    ShortField("recv_port", 0),
                    ShortField("subscribe_port", 0)]
class Tail(Packet):
    fields_desc = [ BitField("nonce", 0, 32),
                    ShortField("hops", 0),
                    BitField("tail_seq_no", 0, 32),
                    BitField("client_ip", 0, 32),
                    ShortField("recv_port", 0)]

bind_layers(PktgenTimerHeader, Ether)
bind_layers(Ether, IP, type=TYPE_IP)
bind_layers(IP, UDP)
bind_layers(UDP, RingType)
bind_layers(RingType, Append, type=TYPE_APPEND)
bind_layers(RingType, Append, type=TYPE_APPEND_RESP)
bind_layers(RingType, Append, type=TYPE_SUB_RESP)
bind_layers(RingType, Read, type=TYPE_READ)
bind_layers(RingType, Read, type=TYPE_READ_RESP)
bind_layers(RingType, Subscribe, type=TYPE_SUB)
bind_layers(RingType, Tail, type=TYPE_TAIL)
bind_layers(Ether, Cntrl, type=TYPE_CONTROL) # TODO: Make ringtype

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
    def setup_switch_ports(self, target, switch_ports, loopback_ports, control_port, port_speed, port_fec, cntrl_port_loopback):
        print(switch_ports)
	for i in switch_ports:
	    self.port_setup(target, i, False, port_speed, port_fec)
        for recirc_port in loopback_ports:
	    self.port_setup(target, recirc_port, True, port_speed, port_fec)
	if cntrl_port_loopback:
	    self.port_setup(target, control_port, True, port_speed, port_fec)
	else:
	    self.port_setup(target, control_port, False, port_speed, port_fec)

    def setup_all_switch_ports(self, target, loop_ports, no_loop_ports, port_speed, port_fec):
        print(loop_ports)
        print(no_loop_ports)
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
                [self.port_table.make_data([gc.DataTuple('$SPEED', str_val=port_speed), # TODO parameterize!
                                            gc.DataTuple('$FEC', str_val=port_fec),
                                            gc.DataTuple('$TX_MTU', 9000),
                                            gc.DataTuple('$RX_MTU', 9000),
                                            gc.DataTuple('$PORT_ENABLE', bool_val=True),
                                            gc.DataTuple('$LOOPBACK_MODE', str_val="BF_LPBK_MAC_NEAR")])])
        else:
            print(port)
            print(port_speed)
            print(port_fec)
            self.port_table.entry_add(
                target,
                [self.port_table.make_key([gc.KeyTuple('$DEV_PORT', port)])],
                [self.port_table.make_data([gc.DataTuple('$PORT_ENABLE', bool_val=True),
                                            gc.DataTuple('$SPEED', str_val=port_speed),
                                            gc.DataTuple('$TX_MTU', 9000),
                                            gc.DataTuple('$RX_MTU', 9000),
                                            gc.DataTuple('$FEC', str_val=port_fec)])])
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

    def setup_circulate_table(self, bfrt_info, recirc_ports, pipe_idx):
        # Circulate_table: If meta.circulate is set to 1, send packet to the loopback port
        # Set all pipes to be in different scopes. Also known as Single scope
        table_circulate = bfrt_info.table_get("MyIngress{}.circulate_table".format(pipe_idx))
        table_circulate.attribute_entry_scope_set(self.target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        table_circulate.info.key_field_annotation_add("meta.port_idx", "bit<8>")
        if pipe_idx == 0:
            for i in range(0, len(recirc_ports)):
                print("Recirc port idx: {} and port {}".format(i, recirc_ports[i]))
                table_circulate.entry_add(
                        self.target0, 
                        [table_circulate.make_key([gc.KeyTuple('meta.port_idx', i)])],
                        [table_circulate.make_data(action_name="MyIngress{}.circulate_port".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=recirc_ports[i])])])
        else:
            for i in range(0, len(recirc_ports)):
                print("Recirc port idx: {} and port {}".format(i, recirc_ports[i]))
                table_circulate.entry_add(
                        self.target1, 
                        [table_circulate.make_key([gc.KeyTuple('meta.port_idx', i)])],
                        [table_circulate.make_data(action_name="MyIngress{}.circulate_port".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=recirc_ports[i])])])
    
    def setup_recirc_port_table(self, bfrt_info, tot_num_recirc_ports,pipe_idx):
        table_recirc = bfrt_info.table_get("MyIngress{}.num_recirc_port_table".format(pipe_idx))
        table_recirc.attribute_entry_scope_set(self.target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)
        if pipe_idx == 0:
            table_recirc.default_entry_set(
                    self.target0, 
                    table_recirc.make_data(action_name="MyIngress{}.get_num_recirc".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="num_recirc_ports", val=tot_num_recirc_ports)]))
        else:
            table_recirc.default_entry_set(
                    self.target1, 
                    table_recirc.make_data(action_name="MyIngress{}.get_num_recirc".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="num_recirc_ports", val=tot_num_recirc_ports)]))

    def setup_ack_const(self, target, bfrt_info, ack_threshold):
        table_ack = bfrt_info.table_get("MyIngress.get_ack_threshold")
        table_ack.entry_add(
                target, 
                [table_ack.make_data(action_name="MyIngress.ack_threshold", data_field_list_in=[gc.DataTuple(name="threshold", val=ack_threshold)])])

    def setup_num_shard_const(self, target, bfrt_info, num_shards):
        table_shard = bfrt_info.table_get("MyIngress.get_num_shards")
        table_shard.entry_add(
                target, 
                [table_shard.make_data(action_name="MyIngress.num_shards", data_field_list_in=[gc.DataTuple(name="shards", val=num_shards)])])


    def setup_shard_size_const(self, target, bfrt_info, shard_size):
        table_shard = bfrt_info.table_get("MyIngress.get_shard_size")
        table_shard.entry_add(
                target, 
                [table_shard.make_data(action_name="MyIngress.shard_size", data_field_list_in=[gc.DataTuple(name="size", val=shard_size)])])

    def setup_shard_multicast_groups(self, target, bfrt_info, shard_to_port_gp):
        # Map shard IDs to their respective multicast group (each group has a collection of ports to storage server)
        table_shard = bfrt_info.table_get("MyIngress.get_shard_port")
        table_shard.info.key_field_annotation_add("hdr.ring_type.shard_id", "bit<32>")
        print(shard_to_port_gp)
        for key, dev_ports in shard_to_port_gp.items():
            rid = 0x321 + self.global_group_id
            xid = 0x432 + self.global_group_id
            mbr_lags = []
            print("RID: ", rid, " XID: ", xid, " GPID: ", self.global_group_id, " DEV: ", dev_ports)
            mc = MCTree(self, target, self.global_group_id)
            mc.add_node(rid, xid, dev_ports, mbr_lags)
            self.mgid_table.entry_add(target, [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])
            table_shard.entry_add(
                target, 
                [table_shard.make_key([gc.KeyTuple('hdr.ring_type.shard_id', key)])],
                [table_shard.make_data(action_name="MyIngress.shard_port", data_field_list_in=[gc.DataTuple(name="group_id", val=self.global_group_id)])])
            self.global_group_id += 1
    
    def setup_switch_check(self, target, bfrt_info, size_of_ring, ports_to_ring_members):
        # Check switch routing
        table_process = bfrt_info.table_get("MyIngress.check_switch_routing")
        table_process.info.key_field_annotation_add("hdr.ring_type.switch_to_process", "bit<32>")
        print("Ports to ring members:")
        print(ports_to_ring_members)
        for i in range(0, size_of_ring):
            if ports_to_ring_members[i] == 0:
                continue; # This is the index of the current switch
            switch_id = i
            table_process.entry_add(
                target, 
                [table_process.make_key([gc.KeyTuple('hdr.ring_type.switch_to_process', switch_id)])],
                [table_process.make_data(action_name="MyIngress.route_next_switch", data_field_list_in=[gc.DataTuple(name="port", val=ports_to_ring_members[i])])])

    def setup_cntrl_table(self, bfrt_info, in_cntrl, out_cntrl, cntrl_port, pipe_idx):
        # Cntrl ID -> Send Port
        table_cntrl = bfrt_info.table_get("MyIngress{}.cntrl_id_to_ip".format(pipe_idx))
        table_cntrl.attribute_entry_scope_set(self.target,
                predefined_pipe_scope=True,
                predefined_pipe_scope_val=bfruntime_pb2.Mode.SINGLE)

        table_cntrl.info.key_field_annotation_add("hdr.cntrl.pkt_id", "bit<32>")
        if pipe_idx == 1:
             table_cntrl.entry_add(
                     self.target1, 
                     [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.pkt_id', in_cntrl)])],
                     [table_cntrl.make_data(action_name="MyIngress{}.cntrl_forward".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=cntrl_port), gc.DataTuple(name="pkt_id", val=out_cntrl)])])
        else:
             table_cntrl.entry_add(
                     self.target0, 
                     [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.pkt_id', in_cntrl)])],
                     [table_cntrl.make_data(action_name="MyIngress{}.cntrl_forward".format(pipe_idx), data_field_list_in=[gc.DataTuple(name="port", val=cntrl_port), gc.DataTuple(name="pkt_id", val=out_cntrl)])])



    def setup_view_check(self, target, bfrt_info): # Is this all that needs to be done for cntrl packet view change?
        # Ring View
        table_view = bfrt_info.table_get("MyIngress.check_cntrl_view")
        table_view.info.key_field_annotation_add("hdr.cntrl.ring_view", "bit<16>")
        table_view.info.data_field_annotation_add("view", "MyIngress.update_cntrl_view", "bit<16>")
        upper_end = self.view - 1
        updated_view = self.view
        print(upper_end)
        print(updated_view)
        print("View checked!")
        table_view.entry_add(
                target, 
                [table_view.make_key([gc.KeyTuple('hdr.cntrl.ring_view', low=1, high=upper_end)])],
                [table_view.make_data(action_name="MyIngress.update_cntrl_view", data_field_list_in=[gc.DataTuple(name="view", val=updated_view)])])

    def setup_tail_table(self, target, bfrt_info, size_of_ring, cntrl_port):
        # Tail
        table_tail = bfrt_info.table_get("MyIngress.process_tail")
        table_tail.info.key_field_annotation_add("hdr.tail.hops", "bit<32>")
        table_tail.entry_add(
                target, 
                [table_tail.make_key([gc.KeyTuple('hdr.tail.hops', low=0, high=(size_of_ring-1))])], # size_of_ring - 1?
                [table_tail.make_data(action_name="MyIngress.forward_tail", data_field_list_in=[gc.DataTuple(name="port", val=cntrl_port)])])

    def setup_subscribe_routing_table(self, target, bfrt_info, ports_to_ring_members, cpu_port):
        # Subscription 
        table_sub = bfrt_info.table_get("MyIngress.submit_subscription")
        table_sub.info.key_field_annotation_add("hdr.sub.subscribe", "bit<32>")

        # Broadcast subscription packet to all switches
        for i in range(0, len(ports_to_ring_members)):
            if ports_to_ring_members[i] == 0:
                ports_to_ring_members[i] = cpu_port
        self.mgid_table = bfrt_info.table_get("$pre.mgid")
        rid = 0x321 + self.global_group_id
        xid = 0x432 + self.global_group_id
        mbr_lags = []
        print("RID: ", rid, " XID: ", xid, " GPID: ", self.global_group_id, " DEV: ", ports_to_ring_members)
        self.general_sub_mc = MCTree(self, target, self.global_group_id)
        self.general_sub_group = self.global_group_id
        self.general_sub_mc.add_node(rid, xid, ports_to_ring_members, mbr_lags)
        self.mgid_table.entry_add(target, [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])
        table_sub.entry_add(
                target, 
                [table_sub.make_key([gc.KeyTuple('hdr.sub.subscribe', 1)])],
                [table_sub.make_data(action_name="MyIngress.send_subscription_to_all", data_field_list_in=[gc.DataTuple(name="group_id", val=self.global_group_id)])])
        self.global_group_id += 1

        # Only send to local control plane
        table_sub.entry_add(
                target, 
                [table_sub.make_key([gc.KeyTuple('hdr.sub.subscribe', 0)])],
                [table_sub.make_data(action_name="MyIngress.send_subscription_to_self", data_field_list_in=[gc.DataTuple(name="port", val=cpu_port)])])
    
    # Acknowledgement tables (primarily handle subscription responses - for both streams & non-streams)
    def setup_subscriber_acks_table(self, target, bfrt_info):
        # Subscriber acks
        table_acks = bfrt_info.table_get("MyIngress.route_subscriber_acks")
        table_acks.info.key_field_annotation_add("hdr.append.stream_id", "bit<32>")

    def update_stream_subscriber_table(self, bfrt_info, target, stream_id, subscriber_port):
        print(stream_id)
        print(subscriber_port)
        print(self.stream_sub_dict)
        if stream_id not in self.stream_sub_dict.keys():
            table_acks = bfrt_info.table_get("MyIngress.route_subscriber_acks")
            table_acks.info.key_field_annotation_add("hdr.append.stream_id", "bit<32>")

            # Subscription group (non-stream & stream)
            stream_sub_mc = MCTree(self, target, self.global_group_id)
            stream_group_id = self.global_group_id
            if stream_id == 0:
                self.general_sub_group = stream_group_id
            rid = 0x321 + stream_group_id
            xid = 0x432 + stream_group_id
            stream_sub_mc.add_node(rid, xid, [subscriber_port], [])
            self.stream_sub_dict[stream_id] = stream_sub_mc
            self.mgid_table.entry_add(target, [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])

            # Update mirror table with the multicast group information4
            mirror_session_bfrt_key = self.mirror_cfg_table.make_key([gc.KeyTuple('$sid', self.sid)])
            if stream_id == 0:
                mirror_session_bfrt_data = self.mirror_cfg_table.make_data([
                    gc.DataTuple('$direction', str_val="INGRESS"),
                    gc.DataTuple('$session_enable', bool_val=True),
                    gc.DataTuple('$mcast_grp_a', self.general_sub_group),
                    gc.DataTuple('$mcast_grp_a_valid', bool_val=True),
                    gc.DataTuple('$ucast_egress_port_valid', bool_val=False),
                ], "$normal")
                self.mirror_cfg_table.entry_add(target, [ mirror_session_bfrt_key ], [ mirror_session_bfrt_data ])
            else:
                mirror_session_bfrt_data = self.mirror_cfg_table.make_data([
                    gc.DataTuple('$direction', str_val="INGRESS"),
                    gc.DataTuple('$session_enable', bool_val=True),
                    gc.DataTuple('$mcast_grp_a', self.general_sub_group),
                    gc.DataTuple('$mcast_grp_a_valid', bool_val=True),
                    gc.DataTuple('$mcast_grp_b', stream_group_id),
                    gc.DataTuple('$mcast_grp_b_valid', bool_val=True),
                    gc.DataTuple('$ucast_egress_port_valid', bool_val=False),
                ], "$normal")
                self.mirror_cfg_table.entry_add(target, [ mirror_session_bfrt_key ], [ mirror_session_bfrt_data ])

            # Table acks 
            table_acks.entry_add(
                    target, 
                    [table_acks.make_key([gc.KeyTuple('hdr.append.stream_id', stream_id)])],
                    [table_acks.make_data(action_name="MyIngress.forward_to_subscribers", data_field_list_in=[gc.DataTuple(name="mirror_session_id", val=self.sid)])])
            self.global_group_id += 1
            self.sid += 1
        else:
            self.stream_sub_dict[stream_id].get_first_node().addMbrs([subscriber_port], [])

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
        
	#while (time.time() - start_time) < duration:
        #    digest = None
        #    try:
        #        digest = self.interface.digest_get()
        #    except RuntimeError:
        #        continue
        #    # Get this batch of digests
        #    #print("New Digest Batch!")
        #    data_list = learn_filter.make_data_list(digest)
        #    total_pkts += len(data_list)
        #    #for data in data_list:
        #        #print(data) #.dict()["duration"])
        #        #total_pkts += 1
        #        #print(data["end_ts"].__str__()) #.dict()["duration"])
        #        #print(data["end_ts"].__type()) #.dict()["duration"])
        #        #print(long(data["end_ts"].__str__())) #.dict()["duration"])
        #        #raw_end = data["end_ts"].val
        #        #end_val = struct.unpack('!Q', raw_end)[0]
        #        #durations.append(end_val) # - long(data["start_ts"]))
        #    #self.send_test_cntrl_packet(interface,dstAddr,srcAddr,ipAddr,in_cntrl, 0)
        #print(total_pkts)
        ##duration_avg = sum(durations) / len(durations)
        ##print(duration_avg)
    
    
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

    def setup_timer_pkt_gen(self, bfrt_info, target, pkt_gen_app_id, dstAddr, srcAddr, ip_addr, payload_size, in_cntrl, cpu_interface, duration, nsperpkt, num_recircs, num_pgen_ports, pipe_id):
        logger.info("=============== Testing Packet Generator trigger by Timer ===============")
        pktgen_app_cfg_table = bfrt_info.table_get("$PKTGEN_APPLICATION_CFG")
        pktgen_pkt_buffer_table = bfrt_info.table_get("$PKTGEN_PKT_BUFFER")
        pktgen_port_cfg_table = bfrt_info.table_get("$PKTGEN_PORT_CFG")


        # timer pktgen app_id = 1 one shot 0
        app_id = 0 #pkt_gen_app_id

        # Assuming not including the pkt gen header
        # Assuming in bytes

        # build expected generated packets
        nonce = 33
        payload = "P" * payload_size
        packed_ip = socket.inet_aton(ip_addr)
        ip_int = struct.unpack("!I", packed_ip)[0]
	p = Ether(dst=dstAddr, src=srcAddr, type=TYPE_IP)/ \
            IP(dst=ip_addr)/ \
	    UDP(dport=1234, sport=5678)/ \
	    RingType(type=TYPE_APPEND, num_entries=1, shard_id=0,switch_to_process=1)/ \
            Append(nonce=nonce,payload_size=payload_size,stream_id=0,g_idx=0,cntrl_pkt_it=0,client_ip=ip_int,recv_port=192,start_ts=0)/ \
            Raw(load=payload)
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
                                                      '$PKTGEN_TRIGGER_TIMER_PERIODIC') # ONE_SHOT
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
                
                # Use the per-app counters to wait for all packets to be generated.
                #start_time = time.time()
                #while (time.time() - start_time) < duration:
                #    # verify pktgen related counters
                #    resp = pktgen_app_cfg_table.entry_get(
                #        target,
                #        [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', g_timer_app_id)])],
                #        {"from_hw": True},
                #        pktgen_app_cfg_table.make_data([gc.DataTuple('batch_counter'),
                #                                        gc.DataTuple('pkt_counter'),
                #                                        gc.DataTuple('trigger_counter')],
                #                                       '$PKTGEN_TRIGGER_TIMER_PERIODIC', get=True)
                #    )
                #    data_dict = next(resp)[0].to_dict()
                #    tri_value = data_dict["trigger_counter"]
                #    if tri_value != 1:
                #        logger.info("Triggered %d times", tri_value)
                #        # Wait for packets to be generated
                #        time.sleep(2)
                #        continue
                #    batch_value = data_dict["batch_counter"]
                #    if batch_value != b_count:
                #        logger.info("Generated %d of %d batches", batch_value, b_count)
                #        # Wait for packets to be generated
                #        time.sleep(2)
                #        continue
                #    pkt_value = data_dict["pkt_counter"]
                #    if pkt_value != b_count * p_count:
                #        logger.info("Generated %d of %d packets", pkt_value, b_count * p_count)
                #        # Wait for packets to be generated
                #        time.sleep(2)
                #        continue
                #    break
                # Verify generated packets
                # verify_multiple_packets(self, out_port, pkt_lst, pkt_len, tmo=5)
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

    def get_final_pktgen_stats(self, bfrt_info, target, duration, nsperpkt, num_recircs, pipe_id, ingress_num, num_pipelines, payload_size):
        # 1. Get a reference to the counter table
        pkts = 0
        pkts_tput = 0
        print("Here is the actual pipe id: {}".format(pipe_id))
        if pipe_id == 0:
            counter_table = bfrt_info.table_get("MyIngress{}.tot_packet_counter".format(ingress_num))
            # 3. Request the entry at index 0
	    # 'from_hw=True' ensures you get the latest count from the ASIC registers, 
	    # not a cached software value.
	    resp = counter_table.entry_get(
	        self.target, #TODO pay attention to this
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
        else:
            counter_table = bfrt_info.table_get("MyIngress{}.tot_packet_counter_pipe_one".format(ingress_num))
	    # 3. Request the entry at index 0
	    # 'from_hw=True' ensures you get the latest count from the ASIC registers, 
	    # not a cached software value.
	    resp = counter_table.entry_get(
	        self.target, #TODO pay attention to this
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
	    print("======================Append Throughput from PIPE ONE: {}".format(pkts/float(duration)))

        low_lat_table = bfrt_info.table_get("MyIngress{}.latency_lower".format(ingress_num))
	low_lat_resp = low_lat_table.entry_get(
	    target,
	    [low_lat_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	low_data, _ = next(low_lat_resp)
	low_data_dict = low_data.to_dict()
        high_lat_table = bfrt_info.table_get("MyIngress{}.latency_higher".format(ingress_num))
	high_lat_resp = high_lat_table.entry_get(
	    target,
	    [high_lat_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	    {"from_hw": True}
	)
	high_data, _ = next(high_lat_resp)
	high_data_dict = high_data.to_dict()
        low_lat = low_data_dict["MyIngress{}.latency_lower.f1".format(ingress_num)][0]
        high_lat = high_data_dict["MyIngress{}.latency_higher.f1".format(ingress_num)][0]
        total_latency_ns = (high_lat << 32) + low_lat
        print("High Latency: {}ns and Low Latency: {}ns".format(high_lat, low_lat))
        print("Aggregate Latency: {}ns".format(total_latency_ns))
        avg_lat = 0
        if pkts != 0:
	    avg_lat = (total_latency_ns/float(pkts))/float(1000)
            print("======================Average latency: {} microseconds".format((total_latency_ns/float(pkts))/float(1000)))
        
        ##### CNTRL PACKET
        #cntrl_table = bfrt_info.table_get("MyIngress{}.cntrl_packet_counter".format(ingress_num))
	#
	## 3. Request the entry at index 0
	## 'from_hw=True' ensures you get the latest count from the ASIC registers, 
	## not a cached software value.
	#resp = cntrl_table.entry_get(
	#    target,
	#    [cntrl_table.make_key([gc.KeyTuple('$COUNTER_INDEX', 0)])],
	#    {"from_hw": True}
	#)
	#
	## 4. Parse the response
	#data, _ = next(resp)
	#data_dict = data.to_dict()
	#
	## Tofino counters usually return a dictionary with '$COUNTER_SPEC_PKTS'
	#cntrl_pkts = data_dict['$COUNTER_SPEC_PKTS']
	#print("Total Control Packets: {}".format(cntrl_pkts))

        #cntrl_lat_table = bfrt_info.table_get("MyIngress{}.ring_trip_time".format(ingress_num))
	#cntrl_lat_resp = cntrl_lat_table.entry_get(
	#    target,
	#    [cntrl_lat_table.make_key([gc.KeyTuple('$REGISTER_INDEX', 0)])],
	#    {"from_hw": True}
	#)
	#lat_data, _ = next(cntrl_lat_resp)
	#data_dict = lat_data.to_dict()
        #cntrl_agg_lat = data_dict["MyIngress{}.ring_trip_time.f1".format(ingress_num)][0]
        #print("Control Packet Aggregate latency: {}".format(cntrl_agg_lat))
        cntrl_pkts = 0
        cntrl_agg_lat = 0
        target_dest = "/root/pipe{}_{}_nsperpkt.json".format(pipe_id, nsperpkt)
        self.write_experiment_telemetry(target_dest, pkts, pkts_tput, total_latency_ns, avg_lat, cntrl_pkts, cntrl_agg_lat, duration, nsperpkt, num_recircs, num_pipelines, payload_size)
        

    def write_experiment_telemetry(self, file_path, total_pkts, throughput, total_lat, avg_lat, total_cntrl, cntrl_lat, duration, nsperpkt, num_recircs, num_pipelines, payload_size):
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
            "num_pipelines": int(num_pipelines),
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
    def test_connection(self, dstAddr, srcAddr, ip_addr, interface):
        print(dstAddr)
        print(srcAddr)
        print(ip_addr)
        print("SENDING THIS PACKET")
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_IP)/ \
              IP(dst=ip_addr)/ \
              UDP(dport=1234, sport=5678)/ \
	      RingType(type=TYPE_IP, num_entries=1, shard_id=1)
        sendp(pkt, iface=interface, verbose=True)
        print("Sent packet!")
    
    def send_test_multicast_packet(self, interface, dstAddr, srcAddr, ip_addr):
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_IP)/ \
              IP(dst=ip_addr)/ \
	      UDP(dport=1234, sport=5678)/ \
	      RingType(type=TYPE_MULTICAST, num_entries=1, shard_id=1)
        payload= Raw(load=b"Hello world!")
        pkt = pkt/payload
	pkt.show()
        pkt_buffer = bytes(pkt)
        # Send socket
        sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW)
        sock.bind((interface, 0))
        sock.send(pkt_buffer)

    def send_test_ack_packet(self, interface, dstAddr, srcAddr, ip_addr, seq_no):
        packed_ip = socket.inet_aton(ip_addr)
        ip_int = struct.unpack("!I", packed_ip)[0]
        nonce = 0
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_IP)/ \
              IP(dst=ip_addr)/ \
	      UDP(dport=1234, sport=5678)/ \
	      RingType(type=TYPE_APPEND_RESP, num_entries=1, shard_id=1,switch_to_process=1)/ \
              Append(nonce=nonce,payload_size=100,stream_id=0,g_idx=seq_no,cntrl_pkt_it=1,client_ip=ip_int,recv_port=192,start_ts=3333)
        payload= Raw(load=b"Hello world!")
        pkt = pkt/payload
        #print("ACK PACKET WE ARE SENDING:")
	#pkt.show()
        sendp(pkt, iface=interface, verbose=True)
    
    def send_test_append_packet(self, interface, dstAddr, srcAddr, ip_addr):
        packed_ip = socket.inet_aton(ip_addr)
        ip_int = struct.unpack("!I", packed_ip)[0]
        nonce = 1
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_IP)/ \
              IP(dst=ip_addr)/ \
	      UDP(dport=1234, sport=5678)/ \
	      RingType(type=TYPE_APPEND, num_entries=1, shard_id=1,switch_to_process=1)/ \
              Append(nonce=nonce,payload_size=12,stream_id=0,g_idx=0,cntrl_pkt_it=0,client_ip=ip_int,recv_port=192,start_ts=0)
        payload= Raw(load=b"Hello world!")
        pkt = pkt/payload
        #pkt.show()
        pkt_buffer = bytes(pkt)
        sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW)
        sock.bind((interface, 0))
        sock.send(pkt_buffer)

    def send_test_cntrl_packet(self, interface, dstAddr, srcAddr, in_cntrl, seq_no):
        #print("Send cntrl packet!")
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_CONTROL)/ \
              Cntrl(global_seq_no=seq_no, ring_view=1, pkt_id=in_cntrl)
        print("CONTROL PACKET WE ARE SENDING:")
	pkt.show()
        sendp(pkt, iface=interface, verbose=True)

    def run_jump_sniff(self, external_interface, listen_ip, listen_port, tofino_interface, dstAddr, srcAddr, ip_addr):
	os.system("taskset -p -c 2 {}".format(os.getpid()))
        raw_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(0x0003))
	raw_sock.bind((external_interface, 0))
        print("Listener thread started on {listen_ip}:{listen_port}")
        nonce = 1
        tofino_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW)
        tofino_sock.bind((tofino_interface, 0))
        while True:
	    try:
                raw_data, addr = raw_sock.recvfrom(65535)
                ip_header = raw_data[14:34]
                iph = struct.unpack('!BBHHHBBH4s4s', ip_header)
                src_ip = socket.inet_ntoa(iph[8])
                dst_ip = socket.inet_ntoa(iph[9])
                protocol = iph[6] # 17 for UDP
                if dst_ip == "10.229.49.9" and protocol == 17:
                    u_header = raw_data[34:42]
                    udph = struct.unpack('!HHHH', u_header)
                    dest_port = udph[1]
	            if dest_port == 5005: # TODO TODO 
	        	pkt = Ether(raw_data)
                        tofino_sock.send(raw_data)
	    except socket.timeout:
	        print("Halted: Waiting for packet to send tofino request")
		continue

    def run_client_no_sniff(self, interface, dstAddr, srcAddr, ip_addr, nonce):
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_APPEND)/ \
              IP(dst=ip_addr)/ \
	      UDP(dport=1234, sport=5678)/ \
	      RingType(type=TYPE_APPEND, num_entries=1)/ \
              Append(cid=0, nonce=nonce, g_idx=0, batch_size=0, shard_id=0, cntrl_pkt_it=0)
        payload= Raw(load=b"Hello world!")
        pkt = pkt/payload
	pkt.show()
        pkt_buffer = bytes(pkt)
        # Send socket
        sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW)
        sock.bind((interface, 0))
        pkt[Append].nonce = nonce 
        sock.send(pkt_buffer)

    
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

	tm_threads = []
        self.target = gc.Target(device_id=0, pipe_id=0xffff)
        self.target0 = gc.Target(device_id=0, pipe_id=0x00)
        self.target1 = gc.Target(device_id=0, pipe_id=0x01)
        #self.update_registers(bfrt_info)

        loop_ports = set()
        no_loop_ports = set()
        for pipe_cfg in pipes_cfg:
            for port in pipe_cfg['loopback_ports']:
                loop_ports.add(port)
            for port in pipe_cfg['ports_to_ring_members']:
                no_loop_ports.add(port)
            for port in pipe_cfg['switch_ports']:
                no_loop_ports.add(port)
            if pipe_cfg['cntrl_port_is_loopback'] == True:
                loop_ports.add(pipe_cfg['cntrl_port'])
            else:
                no_loop_ports.add(pipe_cfg['cntrl_port'])


	#self.setup_switch_ports(self.target, list_of_switch_ports, loopback_ports, cntrl_port, port_speed, port_fec, is_cntrl_pkt_loopback)
	self.setup_all_switch_ports(self.target, loop_ports, no_loop_ports, port_speed, port_fec)

	for pipe_cfg in pipes_cfg:
	    loopback_ports = pipe_cfg['loopback_ports']
	    ports_to_ring_members = pipe_cfg['ports_to_ring_members']
	    tot_num_recirc_ports = pipe_cfg['total_recirc_ports']
	    list_of_switch_ports = pipe_cfg['switch_ports']
	    cntrl_port = pipe_cfg['cntrl_port']
            is_cntrl_pkt_loopback = pipe_cfg['cntrl_port_is_loopback']
            pipe_id = pipe_cfg['pipe_id']


	    print("Pipe {} - ports to ring members: {}".format(pipe_id, ports_to_ring_members))

	    if run_setup:
	        # Initialize this pipe's front-panel/loopback ports
	        #self.setup_switch_ports(self.target, list_of_switch_ports, loopback_ports, cntrl_port, port_speed, port_fec, is_cntrl_pkt_loopback)

	        ####################### SETUP MATCH-ACTION TABLES (this pipe) ##########################
	        print("Pipe {} - Number of loopback ports: {}".format(pipe_id , len(loopback_ports)))
	        print("Pipe {} - Control port: {}, with in port {} and out port {}".format(pipe_id, cntrl_port, in_cntrl, out_cntrl))
	        self.setup_recirc_port_table(bfrt_info, len(loopback_ports), pipe_id)
	        self.setup_circulate_table(bfrt_info, loopback_ports, pipe_id)
	        self.setup_cntrl_table(bfrt_info, in_cntrl, out_cntrl, cntrl_port, pipe_id)

	    #tm_q_thread = threading.Thread(target=self.get_tm_queue, args=(bfrt_info, self.target, cpu_interface, total_time))
	    #tm_q_thread.start()
	    #tm_threads.append(tm_q_thread)

        if switch_send_cntrl:
	    print("Sending control packet!")
	    self.send_test_cntrl_packet(cpu_interface, CONST_MAC_DST, CONST_MAC_SRC, in_cntrl, 0)

	time.sleep(wait_time)
        for pipe_cfg in pipes_cfg:
            if pipe_cfg['pipe_id'] == 0 and pipe_cfg['active_pipe']: #TODO numpgen_ports should not be 2?
                self.setup_timer_pkt_gen(bfrt_info, self.target0, 1, CONST_MAC_DST, CONST_MAC_SRC, CONST_IP, payload_size, in_cntrl, cpu_interface, duration, nsperpkt, tot_num_recirc_ports, 2, pipe_cfg['pipe_id'])
            elif pipe_cfg['pipe_id'] == 1 and pipe_cfg['active_pipe']:
                self.setup_timer_pkt_gen(bfrt_info, self.target1, 1, CONST_MAC_DST, CONST_MAC_SRC, CONST_IP, payload_size, in_cntrl, cpu_interface, duration, nsperpkt, tot_num_recirc_ports, 2, pipe_cfg['pipe_id'])

	## Both pipes' generators are now running concurrently -- wait once for the
	## shared experiment duration rather than once per pipe.
	time.sleep(duration)
        num_pipelines = len(pipes_cfg)

	for pipe_cfg in pipes_cfg:
	    pipe_id = pipe_cfg['pipe_id']
	    tot_num_recirc_ports = pipe_cfg['total_recirc_ports']
            print("Processing pipe {} info!".format(pipe_id))
            if pipe_id == 0:
                self.get_final_pktgen_stats(bfrt_info, self.target0, duration, nsperpkt, tot_num_recirc_ports, pipe_id, 0, num_pipelines, payload_size)
            else:
                self.get_final_pktgen_stats(bfrt_info, self.target1, duration, nsperpkt, tot_num_recirc_ports, pipe_id, 1, num_pipelines, payload_size)

        # Tofino Listener join
        print("Waiting to join threads!")
	#for t in tm_threads:
	#    t.join()
