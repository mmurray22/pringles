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
END_EXPERIMENT = False

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
bind_layers(IP, Append)
bind_layers(Ether, Cntrl)
#bind_layers(IP, Cntrl_Check)

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

    # Need to add more tables
    def initialize_tables(self, target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl, meta_circulate):
        # Set default output port
        table_ipv4 = bfrt_info.table_get("MyIngress.ipv4_lpm")
        table_ipv4.info.key_field_annotation_add("hdr.ipv4.dstAddr", "ipv4")
        table_ipv4.info.data_field_annotation_add("dstAddr", "MyIngress.ipv4_forward", "mac")
        table_ipv4.entry_add(
                target, 
                [table_ipv4.make_key([gc.KeyTuple('hdr.ipv4.dstAddr', ip_addr, prefix_len=32)])],
                [table_ipv4.make_data(action_name="MyIngress.ipv4_forward", data_field_list_in=[gc.DataTuple(name="dstAddr", val=dstAddr), gc.DataTuple(name="port", val=recv_port)])])

        # Set control table
        table_cntrl = bfrt_info.table_get("MyIngress.cntrl_id_to_ip")
        table_cntrl.info.key_field_annotation_add("hdr.cntrl.pkt_id", "bit<32>")
        table_cntrl.entry_add(
                target, 
                [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.pkt_id', in_cntrl)])],
                [table_cntrl.make_data(action_name="MyIngress.cntrl_forward", data_field_list_in=[gc.DataTuple(name="port", val=loopback_port), gc.DataTuple(name="pkt_id", val=out_cntrl)])])

        # Set circulate table
        table_circulate = bfrt_info.table_get("MyIngress.circulate_table")
        #table_circulate.info.key_field_annotation_add("meta.circulate", "int<32>")
        table_circulate.entry_add(
                target, 
                [table_circulate.make_key([gc.KeyTuple('meta.circulate', meta_circulate)])],
                [table_circulate.make_data(action_name="MyIngress.circulate_port", data_field_list_in=[gc.DataTuple(name="port", val=loopback_port)])])

    def delete_tables(self, target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl, meta_circulate):
        # Reset the tables to make multiple runs possible
        table_ipv4 = bfrt_info.table_get("MyIngress.ipv4_lpm")
        table_ipv4.info.key_field_annotation_add("hdr.ipv4.dstAddr", "ipv4")
        table_ipv4.info.data_field_annotation_add("dstAddr", "MyIngress.ipv4_forward", "mac")
        table_ipv4.entry_del(
                target, 
                [table_ipv4.make_key([gc.KeyTuple('hdr.ipv4.dstAddr', ip_addr, prefix_len=32)])])
        # Reset control table
        table_cntrl = bfrt_info.table_get("MyIngress.cntrl_id_to_ip")
        table_cntrl.info.key_field_annotation_add("hdr.cntrl.pkt_id", "int<32>")
        table_cntrl.entry_del(
                target, 
                [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.pkt_id', in_cntrl)])])
    
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

        # Reset circulate table
        table_circulate = bfrt_info.table_get("MyIngress.circulate_table")
        table_circulate.info.key_field_annotation_add("meta.circulate", "bit<32>")
        table_circulate.entry_del(
                target) #, 
                #[table_circulate.make_key([gc.KeyTuple('meta.circulate', meta_circulate)])])
    
    
    def run_tofino_sniff(self, interface, tofinoSrcAddr):
        idle_timeout=10
        #sniff(iface=interface, prn=self.handle_packet, count=10, timeout=10)
        # This creates a raw socket exactly like tcpdump
        L2sock = L2ListenSocket(iface=interface)
        print("[*] L2 Socket Open and Listening...")
        num_packets = 0

        # Block until 1 packet is received
        g_idx = 0
        while True:
            ready = select.select([L2sock], [], [], idle_timeout)
            if ready[0]:
                pkt = L2sock.recv(1024) 
                if pkt and pkt[Ether].src == tofinoSrcAddr:

                    #print("[*] Captured via L2Socket!")
                    #print(testutils.format_packet(pkt))
                    #pkt.show()
                    if pkt[Append]:
                        num_packets += 1
                        #print("The packet ", pkt[Append].nonce, " has index ", pkt[Append].g_idx, " with status ", pkt[Append].status)
                        g_idx = pkt[Append].g_idx

            else:
                print("Progam timeout out! Total number of packets: ", num_packets, " and highest g idx: ", g_idx)
                break
        L2sock.close()
     
    def run_client_sniff(self, interface, udp_sport):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        listener.bind(('', 1234))
        listener.listen(5)
        client_soc, address = listener.accept()
        #print(f"Connection established from: {address[0]} on port {address[1]}")
        while True:
            data = client_soc.recv(1024).decode()
            if len(data) > 0:
                print(data)
                #run_client_no_sniff()
        client_soc.close()
        listener.close()
        #idle_timeout=5
        #traffic_type='UDP'
        ##sniff(iface=interface, prn=self.handle_packet, count=10, timeout=10)
        ## This creates a raw socket exactly like tcpdump
        #L2sock = L2ListenSocket(iface=interface)
        #print("[*] L2 Socket for the client is Open and Listening...")
        #
        ## Block until 1 packet is received
        #while True:
        #    ready = select.select([L2sock], [], [], idle_timeout)
        #    if ready[0]:
        #        pkt = L2sock.recv(1024) 
        #        if pkt and pkt.getlayer(TCP) and pkt[TCP].dport == 1234:
        #                    print("[*] Captured incoming UDP packets via L2Socket!")
        #                    print("Ready to forward append packet to the ASIC!")
        #                    # TODO - actually extract append packet from the python script
        #                    #sendp(pkt, iface=interface, verbose=True) 
        #                    print(testutils.format_packet(pkt))
        #    else:
        #        print("Progam timeout out!")
        #        break
        #L2sock.close()


    def run_client_no_sniff(self, interface, dstAddr, srcAddr, ip_addr, send_timeout):
        nonce = 1
        pkt = Ether(dst=dstAddr, src=srcAddr, type=TYPE_APPEND)/ \
              IP(dst=ip_addr)/ \
              Append(cid=0, nonce=nonce, g_idx=0, batch_size=0, shard_id=0, ring_view=0, status=1, cntrl_pkt_it=0)
        payload= Raw(load=b"Hello world!")
        pkt = pkt/payload
        pkt_buffer = bytes(pkt)
        
        # Send socket
        sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW)
        sock.bind((interface, 0))

        tend = time.time() + send_timeout
        while time.time() < tend:
            pkt[Append].nonce = nonce    
            sock.send(pkt_buffer)
            #sendp(pkt, iface=interface, verbose=True)
            nonce += 1
        print("Sent this many packets: ", nonce)
    
    def runTest(self):
        target = gc.Target(device_id=0, pipe_id=0xffff)
        
        # Get bfrt_info and set it as part of the test
        bfrt_info = self.interface.bfrt_info_get(p4_program_name)

        # Set defaults [TODO: Should be in script]
        ip_addr='100.99.98.97'
        interface = "enp5s0"
        cpuTofinoInterface = "enp5s0"
        clientInterface = "ma1"
        dev_number = 0
        port_number = 192
        addr = socket.gethostbyname('100.99.98.97')
        dstAddr='11:11:11:11:11:11'
        srcAddr='22:22:22:22:22:22'
        udp_src_port = 1234
        send_port = cpu_pcie_port
        recv_port= cpu_pcie_port
        in_cntrl=1
        out_cntrl=1
        meta_circulate = 1
        send_timeout = 10

        try:
            # Starting sniffing thread
            #self.initialize_tables(target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl, meta_circulate)
        
            # Create listener socket for control plane
            #tofino_sniffer_thread = threading.Thread(target=self.run_tofino_sniff, args=(cpuTofinoInterface,dstAddr,))
            #tofino_sniffer_thread.start()
            #time.sleep(2)

            # Inject control packet into the dataplane
            #ipkt = testutils.simple_control_packet(eth_dst=dstAddr, eth_src=srcAddr, pkt_id=in_cntrl)
            #sendp(ipkt, iface=interface, verbose=True) 
            #print("Sent control packet!")
            #time.sleep(2)
            
            # Start client thread 
            #self.run_client_no_sniff(interface, dstAddr, srcAddr, ip_addr, send_timeout)
            client_sniffer_thread = threading.Thread(target=self.run_client_sniff, args=(clientInterface,udp_src_port,))
            client_sniffer_thread.start()
        except KeyboardInterrupt:
            print("\nStopped by user.")
        #finally:
            #self.delete_tables(target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl, meta_circulate)
