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
    def setup_switch_ports(self, target, switch_ports, loopback_port, control_port, port_speed, port_fec, size_of_ring, pktgen_loopback_port):
        print(switch_ports)
	for i in switch_ports:
		self.port_setup(target, i, False, port_speed, port_fec)
	self.port_setup(target, loopback_port, True, port_speed, port_fec)
	#self.port_setup(target, pktgen_loopback_port, True, port_speed, port_fec)
	if size_of_ring == 1:
	    self.port_setup(target, control_port, True, port_speed, port_fec)
	else:
	    self.port_setup(target, control_port, False, port_speed, port_fec)


    def port_setup(self, target, port, use_loopback, port_speed, port_fec):
        logger.info("Test Port cfg table add and read operations")
        logger.info("PortCfgTest: Adding entry for port %d", port)
        if use_loopback:
            self.port_table.entry_add(
                target,
                [self.port_table.make_key([gc.KeyTuple('$DEV_PORT', port)])],
                [self.port_table.make_data([gc.DataTuple('$SPEED', str_val=port_speed), # TODO parameterize!
                                            gc.DataTuple('$FEC', str_val=port_fec),
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

    def setup_circulate_table(self, target, bfrt_info, loopback_port):
        # Circulate_table: If meta.circulate is set to 1, send packet to the loopback port
        meta_circulate = 1
        table_circulate = bfrt_info.table_get("MyIngress.circulate_table")
        table_circulate.info.key_field_annotation_add("meta.circulate", "bit<32>")
        table_circulate.entry_add(
                target, 
                [table_circulate.make_key([gc.KeyTuple('meta.circulate', meta_circulate)])],
                [table_circulate.make_data(action_name="MyIngress.circulate_port", data_field_list_in=[gc.DataTuple(name="port", val=loopback_port)])])

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

    def setup_cntrl_table(self, target, bfrt_info, in_cntrl, out_cntrl, cntrl_port):
        # Cntrl ID -> Send Port
        table_cntrl = bfrt_info.table_get("MyIngress.cntrl_id_to_ip")
        table_cntrl.info.key_field_annotation_add("hdr.cntrl.pkt_id", "bit<32>")
        table_cntrl.entry_add(
                target, 
                [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.pkt_id', in_cntrl)])],
                [table_cntrl.make_data(action_name="MyIngress.cntrl_forward", data_field_list_in=[gc.DataTuple(name="port", val=cntrl_port), gc.DataTuple(name="pkt_id", val=out_cntrl)])])

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

    #def delete_tables(self, target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl, meta_circulate): # TODO
    #    # Reset the tables to make multiple runs possible
    #    table_ipv4 = bfrt_info.table_get("MyIngress.ipv4_lpm")
    #    table_ipv4.info.key_field_annotation_add("hdr.ipv4.dstAddr", "ipv4")
    #    table_ipv4.info.data_field_annotation_add("dstAddr", "MyIngress.ipv4_forward", "mac")
    #    table_ipv4.entry_del(
    #            target, 
    #            [table_ipv4.make_key([gc.KeyTuple('hdr.ipv4.dstAddr', ip_addr, prefix_len=32)])])

    #    # Reset control table
    #    table_cntrl = bfrt_info.table_get("MyIngress.cntrl_id_to_ip")
    #    table_cntrl.info.key_field_annotation_add("hdr.cntrl.pkt_id", "int<32>")
    #    table_cntrl.entry_del(
    #            target, 
    #            [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.pkt_id', in_cntrl)])])
    #    
    #    # Reset circulate table
    #    table_circulate = bfrt_info.table_get("MyIngress.circulate_table")
    #    table_circulate.info.key_field_annotation_add("meta.circulate", "bit<32>")
    #    table_circulate.entry_del(
    #            target,
    #           [table_circulate.make_key([gc.KeyTuple('meta.circulate', meta_circulate)])])
    #     # Disable the application.
    #     logger.info("disable pktgen")
    #     pktgen_app_cfg_table.entry_mod(
    #         target,
    #         [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', g_timer_app_id)])],
    #         [pktgen_app_cfg_table.make_data([gc.DataTuple('app_enable', bool_val=False)],
    #                                         '$PKTGEN_TRIGGER_TIMER_ONE_SHOT')])
    #     # Disable packet generation on the port
    #     pktgen_port_cfg_table.entry_mod(
    #         target,
    #         [pktgen_port_cfg_table.make_key([gc.KeyTuple('dev_port', src_port)])],
    #         [pktgen_port_cfg_table.make_data([gc.DataTuple('pktgen_enable', bool_val=False)])])
    #     CleanupTimerTable(self, target)

       
    ###################### CONTROL PLANE RUNTIME ##############################3
    def receive_pkt_from_tofino(self, bfrt_info, target, interface): # TODO
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
        duration = 20 # PARAMETERIZE TODO
        while True: #(time.time() - start_time) < duration:
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
    
    def process_pktgen(self, bfrt_info, target, interface, dstAddr, srcAddr, ipAddr, in_cntrl):
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
	counter_table = bfrt_info.table_get("pipe.MyIngress.tot_packet_counter")
	
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
    def pgen_port(self, pipe_id):
        """
        Given a pipe return a port in that pipe which is usable for packet
        generation.  Note that Tofino allows ports 68-71 in each pipe to be used for
        packet generation while Tofino2 allows ports 0-7.  This example will use
        either port 68 or port 6 in a pipe depending on chip type.
        """
        pipe_local_port = 68
        return self.make_port(pipe_id, pipe_local_port)
    
    def make_port(self, pipe, local_port):
        return (pipe << 7) | local_port

    def setup_timer_pkt_gen(self, bfrt_info, target, pkt_gen_app_id, dstAddr, srcAddr, ip_addr, payload_size, in_cntrl, cpu_interface):
        logger.info("=============== Testing Packet Generator trigger by Timer ===============")
        pktgen_app_cfg_table = bfrt_info.table_get("$PKTGEN_APPLICATION_CFG")
        pktgen_pkt_buffer_table = bfrt_info.table_get("$PKTGEN_PKT_BUFFER")
        pktgen_port_cfg_table = bfrt_info.table_get("$PKTGEN_PORT_CFG")


        # timer pktgen app_id = 1 one shot 0
        app_id = 0 #pkt_gen_app_id

        # Assuming not including the pkt gen header
        # Assuming in bytes


        pgen_pipe_id = 1
        src_port = self.pgen_port(pgen_pipe_id)
        # B x P to emulate 100G of client traffic
        p_count = 20 # packets per batch
        b_count = 1 # batch number
        buff_offset = 144  # generated packets' payload will be taken from the offset in buffer
        out_port = 192 #swports[0]
        nonce = 33
        payload = "P" * payload_size

        packed_ip = socket.inet_aton(ip_addr)
        ip_int = struct.unpack("!I", packed_ip)[0]
       
        # build expected generated packets
	p = Ether(dst=dstAddr, src=srcAddr, type=TYPE_IP)/ \
            IP(dst=ip_addr)/ \
	    UDP(dport=1234, sport=5678)/ \
	    RingType(type=TYPE_APPEND, num_entries=1, shard_id=0,switch_to_process=1)/ \
            Append(nonce=nonce,payload_size=payload_size,stream_id=0,g_idx=0,cntrl_pkt_it=0,client_ip=ip_int,recv_port=192,start_ts=0)/ \
            Raw(load=payload)
        raw_p = bytes(p)
        pktlen = len(raw_p) #92 bytes: ethernet + ip + udp + ring_type + append
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
            data = pktgen_app_cfg_table.make_data([gc.DataTuple('timer_nanosec', 10000), #3333), #1000
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
                [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', g_timer_app_id)])],
                [data])
            logger.info("configure packet buffer")
            pktgen_pkt_buffer_table.entry_add(
                target,
                [pktgen_pkt_buffer_table.make_key([gc.KeyTuple('pkt_buffer_offset', buff_offset),
                                                   gc.KeyTuple('pkt_buffer_size', pktlen)])],
                [pktgen_pkt_buffer_table.make_data([gc.DataTuple('buffer', raw_p)])])
            #resp = pktgen_pkt_buffer_table.entry_get(
            #    target,
            #    [pktgen_pkt_buffer_table.make_key([gc.KeyTuple('pkt_buffer_offset', buff_offset),
            #                                       gc.KeyTuple('pkt_buffer_size', (pktlen - 6))])],
            #    {"from_hw": False})
            #data_dict = next(resp)[0] .to_dict()
            #print()
            #data_dict
            logger.info("enable pktgen")
            pktgen_app_cfg_table.entry_mod(
                target,
                [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', g_timer_app_id)])],
                [pktgen_app_cfg_table.make_data([gc.DataTuple('app_enable', bool_val=True)],
                                                 '$PKTGEN_TRIGGER_TIMER_PERIODIC')]
            )
            
            # Use the per-app counters to wait for all packets to be generated.
            start_time = time.time()
            duration = 5 # PARAMETERIZE TODO
            while (time.time() - start_time) < duration:
                # verify pktgen related counters
                resp = pktgen_app_cfg_table.entry_get(
                    target,
                    [pktgen_app_cfg_table.make_key([gc.KeyTuple('app_id', g_timer_app_id)])],
                    {"from_hw": True},
                    pktgen_app_cfg_table.make_data([gc.DataTuple('batch_counter'),
                                                    gc.DataTuple('pkt_counter'),
                                                    gc.DataTuple('trigger_counter')],
                                                   '$PKTGEN_TRIGGER_TIMER_PERIODIC', get=True)
                )
                data_dict = next(resp)[0].to_dict()
                tri_value = data_dict["trigger_counter"]
                if tri_value != 1:
                    logger.info("Triggered %d times", tri_value)
                    # Wait for packets to be generated
                    time.sleep(2)
                    continue
                batch_value = data_dict["batch_counter"]
                if batch_value != b_count:
                    logger.info("Generated %d of %d batches", batch_value, b_count)
                    # Wait for packets to be generated
                    time.sleep(2)
                    continue
                pkt_value = data_dict["pkt_counter"]
                if pkt_value != b_count * p_count:
                    logger.info("Generated %d of %d packets", pkt_value, b_count * p_count)
                    # Wait for packets to be generated
                    time.sleep(2)
                    continue
                break
            # 1. Get a reference to the counter table
	    counter_table = bfrt_info.table_get("pipe.MyIngress.tot_packet_counter")
	    
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
            print(data_dict)
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
        
            # Verify generated packets
            # verify_multiple_packets(self, out_port, pkt_lst, pkt_len, tmo=5)
        except gc.BfruntimeRpcException as e:
            print(e)
            raise e
        finally:
            pass


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
    
    def test_subscribe_one_switch(self, interface, dstAddr, srcAddr, ip_addr):
        print("Testing subscribe!!")
        packed_ip = socket.inet_aton(ip_addr)
        ip_int = struct.unpack("!I", packed_ip)[0]
        
        # First, send subscribe request
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_IP)/ \
              IP(dst=ip_addr)/ \
	      UDP(dport=1234, sport=5678)/ \
	      RingType(type=TYPE_SUB, num_entries=1, shard_id=1,switch_to_process=0)/ \
              Subscribe(g_idx=0,stream_id=0,subscribe=0,client_ip=ip_int,recv_port=192,subscribe_port=192)
        payload= Raw(load=b"Hello world!")
        pkt = pkt/payload
        print("SUBSCRIBE PACKET WE ARE SENDING:")
	#pkt.show()
        sendp(pkt, iface=interface, verbose=True)

        # Then, send two acks
        g_idx = 0
        self.send_test_ack_packet(interface,dstAddr,srcAddr,ip_addr,g_idx)
        self.send_test_ack_packet(interface,dstAddr,srcAddr,ip_addr,g_idx)

    def test_tail(self, interface, dstAddr, srcAddr, ip_addr, in_cntrl): # TODO
        print("Running getTail mini-test!")
        # First, send the cntrl packet once (NO loop)
        g_idx = 0
        self.send_test_cntrl_packet(interface,dstAddr,srcAddr,ip_addr,in_cntrl, g_idx)
        # Second, send an append request packet once
        self.send_test_append_packet(interface,dstAddr,srcAddr,ip_addr)
        # Third, send the cntrl packet again (NO loop)
        # Fourth, send append response packet once
        #self.send_test_ack_packet(interface,dstAddr,srcAddr,ip_addr,g_idx)
        self.send_test_append_packet(interface,dstAddr,srcAddr,ip_addr)
        self.send_test_append_packet(interface,dstAddr,srcAddr,ip_addr)
        #self.send_test_ack_packet(interface,dstAddr,srcAddr,ip_addr,g_idx)
        # Fifth, send a tail request packet
        time.sleep(3)
        self.send_test_tail_packet(interface, dstAddr, srcAddr, ip_addr)
          
    def send_test_tail_packet(self, interface, dstAddr, srcAddr, ip_addr):
        packed_ip = socket.inet_aton(ip_addr)
        ip_int = struct.unpack("!I", packed_ip)[0]
        nonce = 1
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_IP)/ \
              IP(dst=ip_addr)/ \
	      UDP(dport=1234, sport=5678)/ \
	      RingType(type=TYPE_TAIL, num_entries=1, shard_id=1,switch_to_process=0)/ \
              Tail(nonce=nonce,hops=0,tail_seq_no=0,client_ip=ip_int,recv_port=192)
        payload= Raw(load=b"Hello world!")
        pkt = pkt/payload
	#pkt.show()
        pkt_buffer = bytes(pkt)
        sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW)
        sock.bind((interface, 0))
        sock.send(pkt_buffer)

     
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

    def send_test_cntrl_packet(self, interface, dstAddr, srcAddr, ip_addr, in_cntrl, seq_no):
        #print("Send cntrl packet!")
        packed_ip = socket.inet_aton(ip_addr)
        ip_int = struct.unpack("!I", packed_ip)[0]
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_CONTROL)/ \
              Cntrl(global_seq_no=seq_no, ring_view=1, pkt_id=in_cntrl)
        #print("CONTROL PACKET WE ARE SENDING:")
	#pkt.show()
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
	filepath = "/root/switch_config.yaml"
        print(filepath)
	data = self.load_config(filepath)
	os.system("taskset -p -c 1 {}".format(os.getpid()))
	dev_number = data['device_number']
       
	# Device startup 
	target = gc.Target(device_id=0, pipe_id=0xffff)
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
        
        # Setup some of the multicast tables
        self.global_group_id = 1
        self.sid = 1
        self.stream_sub_dict = {}

	# Parameters
	listen_ip = "0.0.0.0" # Listen on all interfaces
        listen_port = 5005
	dstAddr = data['mac_dst_addr']
	srcAddr = data['mac_src_addr']
	ipAddr = data['dummy_ip_addr']
	port_speed = data['port_speed']
	port_fec = data['port_fec']
	loopback_port = data['loopback_port']
        pktgen_loopback_port = 169
	cpu_interface = data['cpu_interface']
	external_interface = data['external_interface']
	list_of_switch_ports = data['switch_ports']
	cntrl_port = data['cntrl_port']
	cpu_port = data['cpu_port']
	in_cntrl = data['in_cntrl']
	out_cntrl = data['out_cntrl']
	meta_circulate = data['meta_circulate']
        send_cntrl = data['send_cntrl_pkt']
	size_of_ring = data['size_of_ring']
	target_mac = data['client_mac']
	target_ip = data['client_ip']
	storage_port = 30003 # TODO
	client_base_port = data['client_recv_port']
	client_num_threads = data['num_client_threads']
	client_ips = data['all_client_ips_and_ports']
	switch_mac = data['switch_mac']
        use_stor = data['use_stor']
        cli_base_d_port = data['cli_d_port']
        num_client_threads = data['num_client_threads']
        serv_d_port = data['serv_d_port']
        ipv4_table_vals = data['ipv4_table_entries']
        num_shards = data['num_shards'] # NEW
        shard_size = data['shard_size'] # NEW
        self.view = data['start_view']
        ports_to_ring_members = data['ports_to_ring_members']
        print(ports_to_ring_members)
        cntrl_timeout = data['cntrl_timeout']
        ack_threshold = data['ack_threshold'] # NEW
        payload_size = data['payload_size'] # NEW
        shard_to_port_gp = {} 
        for entry in data['all_shards']: # Entry = [Shard ID, ...ports]
            shard_to_port_gp[entry[0]] = entry[1:]
        
        run_setup = True
        use_pktgen = True

	self.setup_switch_ports(target, list_of_switch_ports, loopback_port, cntrl_port, port_speed, port_fec, size_of_ring, pktgen_loopback_port)
        #self.setup_cntrl_table(target, bfrt_info, in_cntrl, out_cntrl, cntrl_port)
        self.send_test_cntrl_packet(cpu_interface,dstAddr,srcAddr,ipAddr,in_cntrl,0)

        #if run_setup:
        #    # Initialize all 5, 21, 3, 19, 23, 7 ports (332244)
	#    self.setup_switch_ports(target, list_of_switch_ports, loopback_port, cntrl_port, port_speed, port_fec, size_of_ring, pktgen_loopback_port)

        #    ####################### SETUP MATCH-ACTION TABLES ##########################
        #    global_group_id = 1
        #    # if use_pktgen:
        #    #     self.setup_circulate_table(target, bfrt_info, pktgen_loopback_port)
        #        ##self.setup_ack_const(target, bfrt_info, 1)
        #        ##self.setup_num_shard_const(self, target, bfrt_info, 1)
        #        ##self.setup_shard_size_const(self, target, bfrt_info, 1)
        #    #else:
        #    #self.setup_circulate_table(target, bfrt_info, loopback_port)
        #    #self.setup_ack_const(target, bfrt_info, ack_threshold)
        #    #self.setup_num_shard_const(self, target, bfrt_info, num_shards)
        #    #self.setup_shard_size_const(self, target, bfrt_info, shard_size)
        #    
        #    # ipv4_lpm
        #    self.setup_client_response_table(target, bfrt_info, client_ips) 
        #    # Shard port
        #    self.setup_shard_multicast_groups(target, bfrt_info, shard_to_port_gp)
        #    # Check switch routing
        #    #self.setup_switch_check(target, bfrt_info, size_of_ring, ports_to_ring_members)
        #    # Cntrl ID -> IP
        #    self.setup_cntrl_table(target, bfrt_info, in_cntrl, out_cntrl, cntrl_port)
        #    # Ring View
        #    #self.setup_view_check(target, bfrt_info) TODO
        #    # Tail
        #    self.setup_tail_table(target, bfrt_info, size_of_ring, cntrl_port)
        #    # Subscription 
        #    self.setup_subscribe_routing_table(target, bfrt_info, ports_to_ring_members, cpu_port)
        #    # Acknowledgement tables (primarily handle subscription responses)
        #    self.setup_subscriber_acks_table(target, bfrt_info)
        #
        ######################### SETUP DATA PLANE LISTENER
        #recv_thread = threading.Thread(target=self.receive_pkt_from_tofino, args=(bfrt_info,target,cpu_interface,))
        #recv_thread.start()
        #time.sleep(2)
        #
        #        # ============================================== UNIT TESTS ================================================= #
        ## Test connection SIMPLE
        ##self.test_connection(dstAddr,srcAddr,ipAddr,cpu_interface)

        ## Multicast Test - DONE
        ## print("BEGIN MULTICAST TEST")
        ##self.send_multicast_packet(cpu_interface,dest_addr,dest_addr,ipv4_sent)

        ## Simple Ack test - DONE
        ##g_idx = 0
        ##self.send_test_ack_packet(cpu_interface,dstAddr,srcAddr,ipAddr,g_idx)
        ##self.send_test_ack_packet(cpu_interface,dstAddr,srcAddr,ipAddr,g_idx)

        ## Subscribe test - DONE
        ##self.test_subscribe_one_switch(cpu_interface, dstAddr, srcAddr, ipAddr)

        ## Tail test - DONE
        #if use_pktgen:
        #    self.send_test_cntrl_packet(cpu_interface,dstAddr,srcAddr,ipAddr,in_cntrl,0)
        #    self.setup_timer_pkt_gen(bfrt_info, target, 1, dstAddr, srcAddr, ipAddr, payload_size, in_cntrl, cpu_interface)
	#    #pktgen_thread = threading.Thread(target=self.process_pktgen, args=(bfrt_info, target, cpu_interface,dstAddr,srcAddr,ipAddr,in_cntrl))
        #    #pktgen_thread.start()
        #    #pktgen_thread.join()
	#else:
        #    self.test_tail(cpu_interface, dstAddr, srcAddr, ipAddr, in_cntrl)

        ######## TEST 1: End-to-end sequencing test ######
        ## Send one append
        ##self.send_test_append_packet(cpu_interface,dstAddr,srcAddr,ipAddr)
        ## Send cntrl packet (just once)
        ##self.send_test_cntrl_packet(cpu_interface,dstAddr,srcAddr,ipAddr)
        ## Ack Test
        ##self.send_test_ack_packet(cpu_interface,dstAddr,srcAddr,ipAddr)
        ##self.send_test_ack_packet(cpu_interface,dstAddr,srcAddr,ipAddr)
        #
        ## Append test
        #
        ## Read test
        #
        ## Tail test
        #
        ## Subscribe test

        ######## TEST 2: Packet generation test ######

        ## Tofino Listener join
        #print("Waiting to join threads!")
        #recv_thread.join()

