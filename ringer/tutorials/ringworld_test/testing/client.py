#!/usr/bin/env python3
import sys
import time
from scapy.fields import *
from headers import *
import netifaces as ni
setup_dir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(setup_dir + "/dist-systems-group/Scripts/scripts/util/")
from ssh_util import *


NUM_SWITCHES = 4
RECEIVE_SCRIPT = ""
SWITCH_ID = "0"

## Helper functions
def get_if():
    ifs=get_if_list()
    iface=None # "h1-eth0"
    for i in get_if_list():
        if "eth0" in i:
            iface=i
            break;
    if not iface:
        print("Cannot find eth0 interface")
        exit(1)
    return iface

def expand(x):
    yield x
    while x.payload:
        x = x.payload
        yield x

def handle_pkt(pkt):
    if pkt:
        #pkt.show2()
        for l in expand(pkt):
            if l.name=='SeqNoReq':
                print("RECEIVED Local seqno: {}".format(l.local_sequence_no))
            if l.name=='Cntrl':
                print("RECEIVED Control pkt global offset: {}".format(l.last_global_offset))

# Receives local sequence number and global sequence offset
def process_incoming_msgs():
    iface = 'eth0'
    print("Sniffing on {}".format(iface))
    sniff(iface = iface,
          prn = lambda x: handle_pkt(x))

def main():
    # Process Arguments
    args = sys.argv[1:]
    if len(args) == 1:
        SWITCH_ID = args[0]  
        print("SWITCH ID: ", SWITCH_ID)
    print("Interface: ", get_if())
    
    # Start by executing the receive command in the background
    receive_thread = Thread(target = process_incoming_msgs, args = ())
    receive_thread.start()
    print("Starting receiving thread")   
    
    # Send sequence number request
    ip = ni.ifaddresses('eth0')[ni.AF_INET][0]['addr']
    addr = socket.gethostbyname(ip)
    print("Address: ", addr)
    iface = get_if()
    while True:
        try:
            pkt =  Ether(src=get_if_hwaddr(iface), dst='ff:ff:ff:ff:ff:ff')
            pkt = pkt / SeqNoReq() / IP(dst=addr)
            #pkt.show2()
            sendp(pkt, iface='eth0')
            #time.sleep(10)
        except KeyboardInterrupt:
            receive_thread.join() 
            sys.exit()
    return

if __name__ == '__main__':
    main()
