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

loopback_port=68
cpu_pcie_port=192 # dunno what the ptf test script is doing
RCV_SIZE_DEFAULT = 4096
ETH_P_ALL = 0x03
RCV_TIMEOUT = 10000
END_EXPERIMENT = False

# Some useful defines
TYPE_IP= 0x800
TCP_PROTOCOL = 0x6
UDP_PROTOCOL = 0x11
TYPE_APPEND = 0x0860
TYPE_APPEND_RESP = 0x0861
TYPE_TAIL = 0x0840
TYPE_CONTROL = 0x0820; 
TYPE_CONTROL_CHECK = 0x0880; 
TYPE_HELLO = 0x0888;
TYPE_MULTICAST = 0x0890

class Cntrl(Packet):
    fields_desc = [ IntField("global_seq_no", 0),
                    BitField("ring_view", 0, 32),
                    BitField("pkt_id", 0, 32)]

class Cntrl_Check(Packet):
    fields_desc = [ IntField("switch_global_seq_no", 0)]

class Hello(Packet):
    fields_desc = [ BitField("hello", 0, 32)]

class RingType(Packet):
    fields_desc = [ XShortField("type", 0x0),
                    BitField("num_entries", 1, 16),
                    IntField("shard_id", 0),
                    IntField("cid", 0),
                    IntField("switch_to_process", 1)]
class Append(Packet):
    fields_desc = [ IntField("nonce", 0),
                    IntField("payload_size", 0),
                    IntField("stream_id", 0),
                    IntField("g_idx", 0),
                    IntField("batch_size", 1),
                    IntField("ring_view", 0),
                    IntField("status", 0),
                    IntField("cntrl_pkt_it", 0),
                    IntField("thread_id", 0),
                    IntField("client_ip", 0),
                    ShortField("recv_port", 0),
                    IntField("timestamp", 0),
                    IntField("ack_cnt", 0)]
class Read(Packet):
    fields_desc = [ IntField("nonce", 0),
                    IntField("payload_size", 0),
                    IntField("stream_id", 0),
                    IntField("g_idx", 0),
                    IntField("ring_view", 0),
                    IntField("status", 0),
                    IntField("thread_id", 0),
                    IntField("client_ip", 0),
                    ShortField("recv_port", 0),
                    IntField("timestamp", 0),
                    IntField("circs", 0)]
class Subscribe(Packet):
    fields_desc = [ IntField("g_idx", 0),
                    IntField("stream_id", 0),
                    IntField("subscribe", 0),
                    IntField("client_ip", 0),
                    ShortField("recv_port", 0)]
class Tail(Packet):
    fields_desc = [ IntField("nonce", 0),
                    IntField("hops", 0),
                    IntField("tail_seq_no", 0),
                    IntField("client_ip", 0),
                    ShortField("recv_port", 0)]
bind_layers(Ether, IP)
bind_layers(IP, UDP)
bind_layers(UDP, RingType)
bind_layers(RingType, Append)
bind_layers(RingType, Read)
bind_layers(RingType, Subscribe)
bind_layers(RingType, Tail)
bind_layers(Ether, Cntrl)

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

    def get_mgid():
        return self.mgid

    def get_first_node():
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
    def __init__(self):
        self.view = 0
        self.general_sub_mc = MCTree()
        self.stream_sub_dict = {}
        self.global_group_id = 0

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
    def setup_switch_ports(self, target, switch_ports, loopback_port, control_port, port_speed, port_fec, size_of_ring):
        print(switch_ports)
	for i in switch_ports:
		self.port_setup(target, i, False, port_speed, port_fec)
	self.port_setup(target, loopback_port, True, port_speed, port_fec)
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
    def setup_client_response_table(target, bfrt_info, client_ips): # Entry in client_ips: [IP, MAC, PORT]
        # ipv4_lpm
        table_ipv4 = bfrt_info.table_get("MyIngress.ipv4_lpm")
        table_ipv4.info.key_field_annotation_add("hdr.ipv4.dstAddr", "ipv4")
        table_ipv4.info.data_field_annotation_add("dstAddr", "MyIngress.ipv4_forward", "mac")
        for client_ip_and_port in client_ips:
            table_ipv4.entry_add(
                    target, 
                    [table_ipv4.make_key([gc.KeyTuple('hdr.ipv4.dstAddr', client_ip_and_port[0], prefix_len=32)])],
                    [table_ipv4.make_data(action_name="MyIngress.ipv4_forward", data_field_list_in=[gc.DataTuple(name="dstAddr", val=client_ip_and_port[1]), gc.DataTuple(name="port", val=int(client_ip_and_port[2]))])])

    def setup_circulate_table(target, bfrt_info, loopback_port):
        # Circulate_table: If meta.circulate is set to 1, send packet to the loopback port
        meta_circulate = 1
        table_circulate = bfrt_info.table_get("MyIngress.circulate_table")
        table_circulate.info.key_field_annotation_add("meta.circulate", "bit<32>")
        table_circulate.entry_add(
                target, 
                [table_circulate.make_key([gc.KeyTuple('meta.circulate', meta_circulate)])],
                [table_circulate.make_data(action_name="MyIngress.circulate_port", data_field_list_in=[gc.DataTuple(name="port", val=loopback_port)])])

    def setup_shard_multicast_groups(target, bfrt_info, shard_to_port_gp, global_group_id):
        # Map shard IDs to their respective multicast group (each group has a collection of ports to storage server)
        table_shard = bfrt_info.table_get("MyIngress.get_shard_port")
        table_shard.info.key_field_annotation_add("hdr.ring_type.shard_id", "bit<32>")
        self.mgid_table = bfrt_info.table_get("$pre.mgid")
        print(shard_ports)
        for key, dev_ports in shard_ports.items():
            rid = 0x321 + global_group_id
            xid = 0x432 + global_group_id
            mbr_lags = []
            print("RID: ", rid, " XID: ", xid, " GPID: ", global_group_id, " DEV: ", dev_ports)
            mc = MCTree(self, target, global_group_id)
            mc.add_node(rid, xid, dev_ports, mbr_lags)
            self.mgid_table.entry_add(target, [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])
            table_shard.entry_add(
                target, 
                [table_shard.make_key([gc.KeyTuple('hdr.ring_type.shard_id', key)])],
                [table_shard.make_data(action_name="MyIngress.shard_port", data_field_list_in=[gc.DataTuple(name="group_id", val=global_group_id)])])
            global_group_id += 1
    
    def setup_switch_check(target, bfrt_info, size_of_ring, ports_to_ring_members):
        # Check switch routing
        table_process = bfrt_info.table_get("MyIngress.check_switch_routing")
        table_process.info.key_field_annotation_add("hdr.ring_type.switch_to_process", "bit<32>")
        for i in range(0, size_of_ring):
            if ports_to_ring_members[i] == 0:
                continue; # This is the index of the current switch
            switch_id = i + 1
            table_process.entry_add(
                target, 
                [table_process.make_key([gc.KeyTuple('hdr.ring_type.switch_to_process', switch_id)])],
                [table_process.make_data(action_name="MyIngress.route_next_switch", data_field_list_in=[gc.DataTuple(name="port", val=ports_to_ring_members[i])])])

    def setup_cntrl_table(target, bfrt_info, in_cntrl, out_cntrl, cntrl_port):
        # Cntrl ID -> Send Port
        table_cntrl = bfrt_info.table_get("MyIngress.cntrl_id_to_ip")
        table_cntrl.info.key_field_annotation_add("hdr.cntrl.pkt_id", "bit<32>")
        table_cntrl.entry_add(
                target, 
                [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.pkt_id', in_cntrl)])],
                [table_cntrl.make_data(action_name="MyIngress.cntrl_forward", data_field_list_in=[gc.DataTuple(name="port", val=cntrl_port), gc.DataTuple(name="pkt_id", val=out_cntrl)])])

    def setup_view_check(target, bfrt_info): # Is this all that needs to be done for cntrl packet view change?
        # Ring View
        table_view = bfrt_info.table_get("MyIngress.check_cntrl_view")
        table_view.info.key_field_annotation_add("hdr.cntrl.ring_view", "bit<16>")
        table_view.entry_add(
                target, 
                [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.ring_view', low=1, high=(self.view-1))])],
                [table_cntrl.make_data(action_name="MyIngress.update_cntrl_view", data_field_list_in=[gc.DataTuple(name="view", val=self.view)])])

    def setup_tail_table(target, bfrt_info, size_of_ring, cntrl_port):
        # Tail
        table_tail = bfrt_info.table_get("MyIngress.process_tail")
        table_tail.info.key_field_annotation_add("hdr.tail.hops", "bit<32>")
        table_tail.entry_add(
                target, 
                [table_tail.make_key([gc.KeyTuple('hdr.tail.hops', low=1, high=(size_of_ring-1))])],
                [table_tail.make_data(action_name="MyIngress.forward_tail", data_field_list_in=[gc.DataTuple(name="port", val=cntrl_port)])])

    def setup_subscribe_routing_table(target, bfrt_info, ports_to_ring_members, cpu_port):
        # Subscription 
        table_sub = bfrt_info.table_get("MyIngress.submit_subscription")
        table_sub.info.key_field_annotation_add("hdr.sub.subscribe", "bit<32>")

        # Broadcast subscription packet to all switches
        for i in range(0, len(ports_to_ring_members):
            if ports_to_ring_members[i] == 0:
                ports_to_ring_members[i] = cpu_port
        self.mgid_table = bfrt_info.table_get("$pre.mgid")
        rid = 0x321 + self.global_group_id
        xid = 0x432 + self.global_group_id
        mbr_lags = []
        print("RID: ", rid, " XID: ", xid, " GPID: ", global_group_id, " DEV: ", dev_ports)
        mc = MCTree(self, target, self.global_group_id)
        mc.add_node(rid, xid, dev_ports, mbr_lags)
        self.mgid_table.entry_add(target, [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])
        table_sub.entry_add(
                target, 
                [table_sub.make_key([gc.KeyTuple('hdr.sub.subscribe', 1)])],
                [table_sub.make_data(action_name="MyIngress.send_subscription_to_all", data_field_list_in=[gc.DataTuple(name="group_id", val=self.global_group_id)])])
        self.general_sub_group_id = self.global_group_id
        self.global_group_id += 1

        # Only send to local control plane
        table_sub.entry_add(
                target, 
                [table_sub.make_key([gc.KeyTuple('hdr.sub.subscribe', 0)])],
                [table_sub.make_data(action_name="MyIngress.send_subscription_to_self", data_field_list_in=[gc.DataTuple(name="port", val=cpu_port)])])

    # Acknowledgement tables (primarily handle subscription responses - for both streams & non-streams)
    def setup_subscriber_acks_table(target, bfrt_info):
        # Subscriber acks
        table_acks = bfrt_info.table_get("MyIngress.route_subscriber_ack")
        table_acks.info.key_field_annotation_add("hdr.append.stream_id", "bit<32>")

        # Subscription group (non-stream & stream)
        self.mgid_table = bfrt_info.table_get("$pre.mgid")
        rid = 0x321 + self.global_group_id
        xid = 0x432 + self.global_group_id
        mbr_lags = []
        dev_ports = []
        print("RID: ", rid, " XID: ", xid, " GPID: ", global_group_id, " DEV: ", dev_ports)
        self.general_sub_mc = MCTree(self, target, self.global_group_id)
        self.general_sub_mc.add_node(rid, xid, dev_ports, mbr_lags)
        self.mgid_table.entry_add(target, [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])
        self.general_sub_group = self.global_group_id
        self.global_group_id += 1
       
        # Table acks - no stream
        table_acks.entry_add(
                target, 
                [table_acks.make_key([gc.KeyTuple('hdr.append.stream_id', 0)])],
                [table_acks.make_data(action_name="MyIngress.forward_to_subscribers_no_stream", data_field_list_in=[gc.DataTuple(name="general_sub_gid", val=general_group)])])


    def update_subscriber_table(subscriber_port):
        # Subscription group (non-stream & stream)
        self.general_sub_mc.get_first_node().addMbrs([subscriber_port], [])

    def update_stream_subscriber_table(stream_id, subscriber_port):
        if stream_id in self.stream_sub_dict.keys():
            # Subscription group (non-stream & stream)
            stream_sub_mc = MCTree(self, target, self.global_group_id)
            stream_group_id = self.global_group_id
            rid = 0x321 + stream_group_id
            xid = 0x432 + stream_group_id
            stream_sub_mc.add_node(rid, xid, [subscriber_port], [])
            self.stream_sub_dict[stream_id] = stream_sub_mc
            self.mgid_table.entry_add(target, [self.mgid_table.make_key([gc.KeyTuple('$MGID', (rid & 0xFFFF))])])
             
            # Table acks 
            table_acks.entry_add(
                    target, 
                    [table_acks.make_key([gc.KeyTuple('hdr.append.stream_id', stream_id)])],
                    [table_acks.make_data(action_name="MyIngress.forward_to_subscribers", data_field_list_in=[gc.DataTuple(name="general_sub_gid", val=self.general_sub_group), gc.DataTuple(name="stream_gid", val=stream_group_id)])])
            stream_global_id += 1
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
       
    ###################### CONTROL PLANE RUNTIME ##############################3
    def receive_pkt_from_tofino(self, interface, tofinoSrcAddr, target_ip, target_port, target_mac, switch_mac, use_stor): # TODO
        os.system("taskset -p -c 0 {}".format(os.getpid()))
        # This creates a raw socket exactly like tcpdump
        recv_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(0x0003))
        recv_sock.bind((interface, 0))
        print("[*] Tofino Receive Socket Open and Listening...")
        # Block until 1 packet is received
        while True:
            try:
                raw_data, addr = recv_sock.recvfrom(65535)
                pkt = Ether(raw_data)
                
                #eth_header = raw_data[:14]
                #eth = struct.unpack('!6s6sH', eth_header)
                #eth_type = eth[2]
	        ## 2. Parse IP Header (Next 20 bytes)
                #ip_header = raw_data[14:34]
                #iph = struct.unpack('!BBHHHBBH4s4s', ip_header)
                #
                #src_ip = socket.inet_ntoa(iph[8])
                #dst_ip = socket.inet_ntoa(iph[9])
                #protocol = iph[6] # 17 for UDP

                ## UDP Header starts at index 34
                #u_header = raw_data[34:42]
                #udph = struct.unpack('!HHHH', u_header)
                #src_port = udph[0]
                #dest_port = udph[1]
                #PAYLOAD_OFFSET = 42
                #payload = raw_data[PAYLOAD_OFFSET:]
                
                if pkt.haslayer(RingType):
                    if pkt[RingType].type == TYPE_SUB:
                        if pkt[Subscribe].stream_id == 0:
                            # TODO: Get the subscriber port??
                            self.update_subscriber_table(pkt[Subscribe].subscribe_port)
                        else:
                            self.update_stream_subscriber_table(pkt[Subscribe].stream_id, pkt[Subscribe].subscribe_port)
                    elif pkt[RingType].type == TYPE_MULTICAST:
                        # hello
            except socket.timeout:
	        print("Halted: Waiting for packet to send tofino request")
		continue
         
        
    ###################### TESTING FUNCTIONS ##############################3
    def test_connection(self, dstAddr, srcAddr, ip_addr, interface):
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_IP)/ \
              IP(dst=ip_addr)
        sendp(pkt, iface=interface, verbose=True)
        print("Sent packet!")
    
    def send_test_multicast_packet(self, interface, dstAddr, srcAddr, ip_addr):
        print("In start thread!")
        print(srcAddr)
        print(interface)
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_MULTICAST)/ \
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

    def send_ack_packet(self, interface, dstAddr, srcAddr, ip_addr): # TODO
        print("In start thread!")
        print(srcAddr)
        print(interface)
        nonce = 1
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_MULTICAST)/ \
              IP(dst=ip_addr)/ \
	      UDP(dport=1234, sport=5678)/ \
	      RingType(type=TYPE_MULTICAST, num_entries=1, shard_id=1)/ \
              Append(nonce=nonce,payload_size=100,stream_id=1,g_idx=1,batch_size=0,ring_view=1,status=3,cntrl_pkt_it=0,thread_id=0,client_ip=100,recv_port=192,timestamp=3333,ack_cnt=0)
        payload= Raw(load=b"Hello world!")
        pkt = pkt/payload
	pkt.show()
        pkt_buffer = bytes(pkt)
        # Send socket
        sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW)
        sock.bind((interface, 0))
        sock.send(pkt_buffer)

    
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
              Append(cid=0, nonce=nonce, g_idx=0, batch_size=0, shard_id=0, ring_view=0, status=1, cntrl_pkt_it=0)
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

	# Parameters
	listen_ip = "0.0.0.0" # Listen on all interfaces
        listen_port = 5005
	dstAddr = data['mac_dst_addr']
	srcAddr = data['mac_src_addr']
	ipAddr = data['dummy_ip_addr']
	port_speed = data['port_speed']
	port_fec = data['port_fec']
	loopback_port = data['loopback_port']
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
	switch_mac = data['switch_mac']
        use_stor = data['use_stor']
        cli_base_d_port = data['cli_d_port']
        num_client_threads = data['num_client_threads']
        serv_d_port = data['serv_d_port']
        ipv4_table_vals = data['ipv4_table_entries']
        ack_threshold = data['ack_threshold']
        ports_to_ring_members = data['ports_to_ring_members']
        self.view = data['start_view']
        ports_to_ring_members = data['ports_to_ring_members']
        client_ips = data['all_client_ips_and_ports']
        cntrl_timeout = data['cntrl_timeout']
        shard_to_port_gp = {} 
        for entry in data['all_shards']: # Entry = [Shard ID, ...ports]
            shard_to_port_gp[entry[0]] = entry[1:]

        # Initialize all 5, 21, 3, 19, 23, 7 ports (332244)
	self.setup_switch_ports(target, list_of_switch_ports, loopback_port, cntrl_port, port_speed, port_fec, size_of_ring)

                # ==================================== UNIT TESTS ======================================= #
        ####################### SETUP MATCH-ACTION TABLES
        global_group_id = 1
        # ipv4_lpm
        self.setup_client_response_table(target, bfrt_info, client_ips) 
        # Circulate_table
        self.setup_circulate_table(target, bfrt_info, loopback_port)
        # Shard port
        self.setup_shard_multicast_groups(target, bfrt_info, shard_to_port_gp, global_group_id)
        # Check switch routing
        self.setup_switch_check(target, bfrt_info, size_of_ring, ports_to_ring_members)
        # Cntrl ID -> IP
        self.setup_cntrl_table(target, bfrt_info, in_cntrl, out_cntrl, cntrl_port)
        # Ring View
        self.setup_view_check(target, bfrt_info, start_view)
        # Tail
        self.setup_tail_table(target, bfrt_info, size_of_ring, cntrl_port)
        # Subscription 
        self.setup_subscribe_routing_table(target, bfrt_info, ports_to_ring_members, global_group_id)
        # Acknowledgement tables (primarily handle subscription responses)
        self.setup_subscriber_table(target, bfrt_info, global_group_id) 
        self.setup_stream_subscriber_table(target, bfrt_info, global_group_id)


        ######################## SETUP DATA PLANE LISTENER
        recv_thread = threading.Thread(target=self.recv_tofino, args=(cpu_interface,match_addr,))
        recv_thread.start()
        time.sleep(2)

        # Multicast Test - DONE
        # print("BEGIN MULTICAST TEST")
        #self.send_multicast_packet(cpu_interface,dest_addr,dest_addr,ipv4_sent)

        # Ack Test
        self.send_ack_packet(cpu_interface,dest_addr,dest_addr,ipv4_table_vals[0])
        self.send_ack_packet(cpu_interface,dest_addr,dest_addr,ipv4_table_vals[0])
        
        # Append test
        
        # Read test
        
        # Tail test
        
        # Subscribe test


        # Tofino Listener join
        recv_thread.join()
