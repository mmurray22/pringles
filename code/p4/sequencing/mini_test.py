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
from scapy import *
import time
import sys
import copy
import random
from headers import *

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
 
#class Tail(Packet):
#    fields_desc = [ BitField("cid", 0, 32),
#                    BitField("nonce", "", 32),
#                    IntField("hops", 0),
#                    IntField("tail_seq_no", 0)]

bind_layers(Ether, IP)
bind_layers(IP, Append)
bind_layers(Ether, Cntrl)
#bind_layers(IP, Cntrl_Check)

def run_client_sniff(self, interface, udp_sport):
    idle_timeout=5
    traffic_type='UDP'
    #sniff(iface=interface, prn=self.handle_packet, count=10, timeout=10)
    # This creates a raw socket exactly like tcpdump
    L2sock = L2ListenSocket(iface=interface)
    print("[*] L2 Socket for the client is Open and Listening...")
    
    # Block until 1 packet is received
    while True:
        ready = select.select([L2sock], [], [], idle_timeout)
        if ready[0]:
            pkt = L2sock.recv(1024) 
            if pkt and pkt.getlayer(UDP): #and pkt[UDP].sport == udp_sport:
                        print("[*] Captured incoming UDP packets via L2Socket!")
                        print("Ready to forward append packet to the ASIC!")
                        # TODO - actually extract append packet from the python script
                        #sendp(pkt, iface=interface, verbose=True) 
                        print(testutils.format_packet(pkt))
        else:
            print("Progam timeout out!")
            break
    L2sock.close()

def main(self):
    # Set defaults [TODO: Should be in script]
    ip_addr='100.99.98.97'
    clientInterface = "ma1"
    addr = socket.gethostbyname('100.99.98.97')
    dstAddr='11:11:11:11:11:11'
    srcAddr='22:22:22:22:22:22'
    udp_src_port = 1234
    in_cntrl=1
    out_cntrl=1
    meta_circulate = 1
    send_timeout = 2

    try:
        # Start client thread 
        self.run_client_no_sniff(interface, dstAddr, srcAddr, ip_addr, send_timeout)
        client_sniffer_thread = threading.Thread(target=self.run_client_sniff, args=(clientInterface,udp_src_port,))
        client_sniffer_thread.start()
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
