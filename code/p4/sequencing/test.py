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
from headers import *

import ptf.dataplane as dataplane
from ptf import config
import ptf.testutils as testutils
from bfruntime_client_base_tests import BfRuntimeTest
import pltfm_pm_rpc
import bfrt_grpc.client as gc
from ptf.thriftutils import *


p4_program_name = "sequencing_only"

loopback_port=68
cpu_pcie_port=192 # dunno what the ptf test script is doing
RCV_SIZE_DEFAULT = 4096
ETH_P_ALL = 0x03
RCV_TIMEOUT = 10000

# Some useful defines
IP_ETHERTYPE = 0x800
TCP_PROTOCOL = 0x6
UDP_PROTOCOL = 0x11
TYPE_APPEND = 0x0860
TYPE_TAIL = 0x0840
TYPE_CONTROL = 0x0820; 
TYPE_CONTROL_CHECK = 0x0880; 
TYPE_HELLO = 0x0888;

class Cntrl(Packet):
    fields_desc = [ IntField("global_seq_no", 0),
                    BitField("ring_view", 0, 32),
                    BitField("pkt_id", 0, 32)]

class Cntrl_Check(Packet):
    fields_desc = [ IntField("switch_global_seq_no", 0)]

class Hello(Packet):
    fields_desc = [ BitField("hello", 0, 32)]

class Append(Packet):
    fields_desc = [ BitField("cid", 0, 32),
                    BitField("nonce", "", 32),
                    IntField("g_idx", 0),
                    IntField("batch_size", 1),
                    BitField("shard_id", 0, 32),
                    BitField("ring_view", 0, 32),
                    BitField("status", 0, 32),
                    IntField("cntrl_pkt_it", 0)]
 
class Tail(Packet):
    fields_desc = [ BitField("cid", 0, 32),
                    BitField("nonce", "", 32),
                    IntField("hops", 0),
                    IntField("tail_seq_no", 0)]

bind_layers(Ether, IP)
bind_layers(IP, Tail)
bind_layers(IP, Append)
bind_layers(IP, Cntrl)
bind_layers(IP, Cntrl_Check)

swports = []
for device, port, ifname in config["interfaces"]:
    #print("Port: ", port)
    #print("Interface: ", ifname)
    swports.append(port)
    swports.sort()

logger = logging.getLogger('Test')
if not len(logger.handlers):
    logger.addHandler(logging.StreamHandler())

class SequencingTest(BfRuntimeTest):
    def setUp(self):
        client_id = 0
        BfRuntimeTest.setUp(self, client_id, p4_program_name)

    def initialize_all_ports(self, device, ps, fec):
        for i in range(0, 130):
            pltfm_port_pm_enable(self, device, dev_port)
            pltfm_port_pm_add(self, device, dev_port, ps, fec)


    def create_cpu_socket(self, interface_name, device_number, port_number):
        """
        @param interface_name The name of the physical interface like eth1
        """
        # Set interface up
        cpu_socket = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, 0)
        #afpacket.enable_auxdata(cpu_socket)
        #socket.setsockopt(SOL_PACKET, PACKET_AUXDATA, 1)
        cpu_socket.bind((interface_name, ETH_P_ALL))
        #netutils.set_promisc(cpu_socket, interface_name)
        #cpu_socket.settimeout(self.RCV_TIMEOUT)
        #recv_size = config.get("socket_recv_size", self.RCV_SIZE_DEFAULT)
        return cpu_socket

    def delete_socket(self, cpu_socket):
        if cpu_socket:
            cpu_socket.close()

    # Need to add more tables
    def initialize_tables(self, target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl):
        # Set default output port
        table_ipv4 = bfrt_info.table_get("MyIngress.ipv4_lpm")
        table_ipv4.info.key_field_annotation_add("hdr.ipv4.dstAddr", "ipv4")
        table_ipv4.info.data_field_annotation_add("dstAddr", "MyIngress.ipv4_forward", "mac")
        table_ipv4.entry_add(
                target, 
                [table_ipv4.make_key([gc.KeyTuple('hdr.ipv4.dstAddr', ip_addr, prefix_len=32)])],
                [table_ipv4.make_data(action_name="MyIngress.ipv4_forward", data_field_list_in=[gc.DataTuple(name="dstAddr", val=dstAddr), gc.DataTuple(name="port", val=recv_port)])])

        #table_cntrl = bfrt_info.table_get("MyIngress.cntrl_id_to_ip")
        #table_cntrl.info.key_field_annotation_add("hdr.cntrl.id", "int<32>")
        #table_cntrl.entry_add(
        #        target, 
        #        [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.id', in_cntrl)])],
        #        [table_cntrl.make_data(action_name="MyIngress.cntrl_forward", data_field_list_in=[gc.DataTuple(name="port", val=loopback_port), gc.DataTuple(name="id", val=out_cntrl)])])

    def delete_tables(self, target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl):
        # Reset the tables to make multiple runs possible
        table_ipv4 = bfrt_info.table_get("MyIngress.ipv4_lpm")
        table_ipv4.info.key_field_annotation_add("hdr.ipv4.dstAddr", "ipv4")
        table_ipv4.info.data_field_annotation_add("dstAddr", "MyIngress.ipv4_forward", "mac")
        table_ipv4.entry_del(
                target, 
                [table_ipv4.make_key([gc.KeyTuple('hdr.ipv4.dstAddr', ip_addr, prefix_len=32)])])

        #table_cntrl = bfrt_info.table_get("MyIngress.cntrl_id_to_ip")
        #table_cntrl.info.key_field_annotation_add("hdr.cntrl.id", "int<32>")
        #table_cntrl.entry_del(
        #        target, 
        #        [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.id', in_cntrl)])])
    
    def run_sniff(self, interface, tofinoSrcAddr):
        idle_timeout=5
        #sniff(iface=interface, prn=self.handle_packet, count=10, timeout=10)
        # This creates a raw socket exactly like tcpdump
        L2sock = L2ListenSocket(iface=interface)
        print("[*] L2 Socket Open and Listening...")
            
        # Block until 1 packet is received
        while True:
            ready = select.select([L2sock], [], [], idle_timeout)
            if ready[0]:
                pkt = L2sock.recv(1024) 
                if pkt and pkt[Ether].src == tofinoSrcAddr:
                    print("[*] Captured via L2Socket!")
                    print(testutils.format_packet(pkt))
            else:
                print("Progam timeout out!")
                break
        L2sock.close()
        
    def handle_packet(self, recv_pkt):
        print("!!!!!!!!!!!!!!!!!!!!!!Received from Tofino!")
        if recv_pkt[Ether].src == "11:11:11:11:11:11":
            print(testutils.format_packet(recv_pkt))
        #print("!!!!!!!!!!!! SHOW PACKET")
        #recv_pkt.show()
        logger.info("Test finished!")

    def runTest(self):
        target = gc.Target(device_id=0, pipe_id=0xffff)
        
        # Get bfrt_info and set it as part of the test
        bfrt_info = self.interface.bfrt_info_get(p4_program_name)

        # Set default output port
        ip_addr='100.99.98.97'
        interface = "enp5s0"
        dev_number = 0
        port_number = 192
        addr = socket.gethostbyname('100.99.98.97')
        dstAddr='11:11:11:11:11:11'
        srcAddr='22:22:22:22:22:22'
        send_port = cpu_pcie_port # swports[0]
        recv_port= cpu_pcie_port # swports[1]
        in_cntrl=1
        out_cntrl=1
        
        # Starting sniffing thread
        self.delete_tables(target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl)
        #cpu_socket = self.create_cpu_socket(interface, dev_number, port_number)
        self.initialize_tables(target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl)
        
        # ipkt = testutils.simple_control_packet(eth_dst=dstAddr, eth_src=srcAddr)
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_HELLO)/ \
              IP(dst=ip_addr)/ \
              Hello(hello=0)
        payload= Raw(load=b"Hello world!")
        pkt = pkt/payload
        print(testutils.format_packet(pkt))
        #pkt = pkt/codecs.decode("".join(["%02x"%(x%256) for x in range(pktlen - len(pkt))]), "hex")
        
        sniffer_thread = threading.Thread(target=self.run_sniff, args=(interface,dstAddr,))
        sniffer_thread.start()
        time.sleep(2)

        sendp(pkt, iface=interface, verbose=True) 
        # ipkt = testutils.simple_hello_world_packet(eth_dst=dstAddr,
        #                                    eth_src=srcAddr,
        #                                    ip_src='1.2.3.4',
        #                                    ip_dst=ip_addr,
        #                                    ip_id=101,
        #                                    ip_ttl=64,
        #                                    hello=0)
        #cpu_socket.send(bytes(ipkt))
        #testutils.send_packet(self, send_port, ipkt) # K it can't find loopback port because it's not tied to an interface?

        #print("Src port: ", send_port, " and dest port: ", recv_port)


        #ipkt = testutils.simple_control_check_packet(eth_dst='11:11:11:11:11:11',
        #                                   eth_src='22:22:22:22:22:22')
        #testutils.send_packet(self, swports[0], ipkt)
        #print(testutils.format_packet(ipkt))

        #logger.info("Waiting for a reply...")
        #(rcv_dev, rcv_port, rcv_pkt, pkt_time) = \
        #    testutils.dp_poll(self, 0, recv_port, timeout=2)
        #logger.info("Received packet of size {:>15}".format(str(len(ipkt.__class__(rcv_pkt)))))
        ##logger.info("Sent packet of size {:>15}".format(str(len(ipkt))))
        #hexdump(rcv_pkt)
        #sys.stdout.flush()
        #print(testutils.format_packet(rcv_pkt))
        #expected_ipkt = testutils.simple_hello_world_packet(eth_dst='11:11:11:11:11:11',
        #                                   eth_src='11:11:11:11:11:11',
        #                                   ip_src='1.2.3.4',
        #                                   ip_dst='100.99.98.97',
        #                                   ip_id=101,
        #                                   ip_ttl=63,
        #                                   hello=1)
        #
        #print(testutils.format_packet(expected_ipkt))
        #print(ipkt.show())
        #print(ipkt.__class__(rcv_pkt).show2())
        #print("Payload of IP: ", ipkt.__class__(rcv_pkt)['Hello'].payload.summary())
        #received_packet = Ether(rcv_pkt)
        #if IP in received_packet:
        #    logger.info("The IP ttl is: {:>15}".format(str(received_packet[IP].ttl)))
        #if Raw in received_packet:
        #    print("The IP ttl is: ", received_packet[Raw].load.decode())
        """
        nrcv = ipkt.__class__(rcv_pkt)

        # Parse the payload and extract the timestamps
        # import pdb; pdb.set_trace()
        ts_ingress_mac, ts_ingress_global, \
            ts_enqueue, ts_dequeue_delta, \
            ts_egress_global, ts_egress_tx = \
            struct.unpack("!QQIIQQxxxxxxxxxxxxxxxxxx", nrcv.load)

        ns = 1000000000.0
        logger.info("Timestamps")
        logger.info("  raw values in ns:")
        logger.info("    ingress mac                   : {:>15}".format(ts_ingress_mac))
        logger.info("    ingress global                : {:>15}".format(ts_ingress_global))
        logger.info("    traffic manager enqueue       : {:>15}".format(ts_enqueue))
        logger.info("    traffic manager dequeue delta : {:>15}".format(ts_dequeue_delta))
        logger.info("    egress global                 : {:>15}".format(ts_egress_global))
        logger.info("    egress tx (no value in model) : {:>15}".format(ts_egress_tx))
        logger.info("  values in s:")
        logger.info("    ingress mac                   : {:>15.9f}".format(ts_ingress_mac / ns))
        logger.info("    ingress global                : {:>15.9f}".format(ts_ingress_global / ns))
        logger.info("    traffic manager enqueue       : {:>15.9f}".format(ts_enqueue / ns))
        logger.info("    traffic manager dequeue delta : {:>15.9f}".format(ts_dequeue_delta / ns))
        logger.info("    egress global                 : {:>15.9f}".format(ts_egress_global / ns))
        logger.info("    egress tx (no value in model) : {:>15.9f}".format(ts_egress_tx))
        logger.info("Please note that the timestamps are using the internal time " +
                    "of the model/chip. They are not synchronized with the global time. "
                    "Furthermore, the traffic manager timestamps in the model do not " +
                    "accurately reflect the packet processing. Correct values are shown " +
                    "by the hardware implementation.")
        """
