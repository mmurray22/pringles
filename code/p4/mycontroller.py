#!/usr/bin/env python3

#TODO Implement this:  https://github.com/jafingerhut/p4-guide/tree/master/ptf-tests/registeraccess

# CONTROL PLANE FOR PRINGLES: 
# This is the PER SWITCH control plane for pringles. 
# We decide to distribute the control plane on each switch to maintain a 
# view change protocol not dependent on a single point of failure.

import argparse
import os
import sys
import yaml
from time import sleep
from headers import *
import grpc

# Import P4Runtime lib from parent utils dir
# Probably there's a better way of doing this.
sys.path.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)),
                 '../utils/'))
import p4runtime_lib.bmv2
import p4runtime_lib.helper
from p4runtime_lib.error_utils import printGrpcError
from p4runtime_lib.switch import ShutdownAllSwitchConnections
import p4runtime_sh.shell as sh
import p4runtime_shell_utils as shu
import scapy.all as scapy
SWITCH_TO_HOST_PORT = 1
SWITCH_TO_SWITCH_PORT = 2
VIEW = 0

def ipv4Routing(p4info_helper, ingress_sw, dst_eth_addr, dst_ip_addr):
    """
    :param p4info_helper: the P4Info helper
    :param ingress_sw: the ingress switch connection
    :param tunnel_id: the specified tunnel ID
    :param dst_eth_addr: the destination IP to match in the ingress rule
    :param dst_ip_addr: the destination Ethernet address to write in the
                        egress rule
    """
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.ipv4_lpm",
        match_fields={
            "hdr.ipv4.dstAddr": (dst_ip_addr, 32)
        },
        action_name="MyIngress.ipv4_forward",
        action_params={
            "dstAddr": dst_eth_addr,
            "port": port,
        })
    ingress_sw.WriteTableEntry(table_entry)
    print("Installed ingress tunnel rule on %s" % ingress_sw.name)

def addCntrlTable(p4info_helper, ingress_sw, next_switch_id):
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.cntrl_id_to_ip",
        match_fields={
            "hdr.cntrl.id": MY_SWITCH_ID
        },
        action_name="MyIngress.cntrl_forward",
        action_params={
            "port": port,
            "id": next_switch_id,
        })
    sw.WriteTableEntry(table_entry)
    print("Installed ingress ack rule on %s" % sw.name)

def addAckForwardTable(p4info_helper, sw, src_idx, src_ip_addr):
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.ipv4_ack",
        match_fields={
            "hdr.ipv4.srcAddr": (src_ip_addr, 32)
        },
        action_name="MyIngress.forward_ack_pkt",
        action_params={
            "port": port,
        })
    sw.WriteTableEntry(table_entry)
    print("Installed ingress ack rule on %s" % sw.name)


def appendShardIDtoPortTable(p4info_helper, sw, shard_id, mcast_grp_num):
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.get_append_shard_id",
        match_fields={
            "hdr.append.shard_id": (shard_id, 32)
        },
        action_name="MyIngress.multicast_append",
        action_params={
            "mcast_grp_num": mcast_grp_num,
        })
    sw.WriteTableEntry(table_entry)
    print("Installed ingress ID rule on %s" % ingress_sw.name)

def readShardIDtoPortTable(p4info_helper, sw, shard_id, mcast_grp_num):
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.get_read_shard_id",
        match_fields={
            "hdr.read.shard_id": (shard_id, 32)
        },
        action_name="MyIngress.read_request",
        action_params={
            "mcast_grp_num": mcast_grp_num,
        })
    sw.WriteTableEntry(table_entry)
    print("Installed ingress ID rule on %s" % ingress_sw.name)

def readSwitchIDtoPortTable(p4info_helper, sw, switch_id, port):
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.get_read_switch",
        match_fields={
            "hdr.read.switch_id": (switch_id, 64)
        },
        action_name="MyIngress.read_switch",
        action_params={
            "port": port,
        })
    sw.WriteTableEntry(table_entry)
    print("Installed ingress ID rule on %s" % ingress_sw.name)

def processTailTable(p4info_helper, sw, num_nodes, port, dstAddr):
    zero = 0
    one_less = num_nodes - 1
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.process_tail",
        match_fields={
            "hdr.tail.hops": [zero, one_less]
        },
        action_name="MyIngress.update_tail",
        action_params={
            "port": port,
        },
        priority=1)
    sw.WriteTableEntry(table_entry)
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.process_tail",
        match_fields={
            "hdr.tail.hops": [one_less,num_nodes]
        },
        action_name="MyIngress.return_tail",
        action_params={
            "dstAddr": dstAddr,
            "port": port,
        },
        priority=1)
    sw.WriteTableEntry(table_entry)
    print("Installed ingress ID rule on %s" % ingress_sw.name)

def circulateTable(p4info_helper, ingress_sw, port):
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.circulate_table",
        match_fields={
            "meta.default_bit": 1
        },
        action_name="MyIngress.circulate_port",
        action_params={
            "port": port,
        })
    sw.WriteTableEntry(table_entry)
    print("Installed ingress ID rule on %s" % ingress_sw.name)

def addContiguousSeqNoTable(p4info_helper, size_of_idx_arr):
    for i in range(0, size_of_idx_arr):
        start_range = 1 << i
        mask = 1
        for j in range(0, i):
            end_range = end_range << 1
            end_range = end_range | 1
        table_entry = p4info_helper.buildTableEntry(
            table_name="MyIngress.update_contiguous_seq_no",
            match_fields={
                "meta.highest_contiguous_seq_no": [start_range, end_range]
            },
            action_name="MyIngress.new_contiguous_value",
            action_params={
                "offset": i,
            },
            priority=1
        )
    sw.WriteTableEntry(table_entry)
    print("Installed ingress ID rule on %s" % ingress_sw.name)

def checkUnwrittenTable(p4info_helper, ingress_sw, port, dstAddr):
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.check_unwritten",
        match_fields={
            "hdr.read.unwritten": 1
        },
        action_name="MyIngress.return_read",
        action_params={
            "dstAddr": dstAddr,
            "port": port,
        })
    sw.WriteTableEntry(table_entry)
    print("Installed ingress ID rule on %s" % ingress_sw.name)

def handle_pkt(pkt):
    print("Controller got a packet")

    heartbeat_header = HeartbeatHeader(raw(pkt))

    if heartbeat_header.live == 1:
        heartbeat_header.show()
        heartbeat = 1

    sys.stdout.flush()

# Get a notification of every time the control packet is received
def ring_heartbeat(iface):
    sys.stdout.flush()
    sniff(iface = iface,
          prn = lambda x: handle_pkt(x))

def view_change():
    # Step 1: Update view number
    VIEW = VIEW + 1
    # Step 2: Create view change packet
    # Step 3: Wait for quorum/timeout
    # Step 4: 


# Necessary inputs!!!!
# - List of storage servers 
# - Array of switch ips?
# Main function: Initializes the configuration of the switch + handles view changes over time
# This control plane is run LOCALLY on each switch. We do this to avoid needing a centralized entity to be involved in the network setup and future view changes.
# Argument: A single yaml file with the following values:
#   - Switch Array: List of all switch IPs in the system
#   - Storage servers: Map of shard ID to a list of all member storage server IPs
#   - Timeout value for control packet
#   - Membership: Order of active switches in ring
def main(p4info_file_path, bmv2_file_path, yaml_file):
    # 1. Get values out of yaml file
    with open(yaml_file, 'r') as file:
        switch_args = yaml.safe_load(file)
    timeout = switch_args['switch_timeout']
    switch_arr = switch_args['switch_info'] # ['127.0.0.1:5001', '127.0.0.1:5002']
    switch_port_arr = switch_args['switch_port']
    switch_eth_addr = switch_args['switch_info']
    storage_servers = switch_args['storage_server_info'] # ['127.0.0.1:5003', '127.0.0.1:5004']
    storage_eth_addr = switch_args['storage_eth']
    membership = switch_args['members']
    this_switch_id = switch_args['switch_id'] # 2
    this_switch_port = switch_args['switch_port'] # 1
    idx_cache_size = switch_args['switch_idx_cache_size']
    interface = switch_args['veth0']


    # Instantiate a P4Runtime helper from the p4info file
    p4info_helper = p4runtime_lib.helper.P4InfoHelper(p4info_file_path)
    try:
        # Create a switch connection object for s#
        # this is backed by a P4Runtime gRPC connection.
        # Also, dump all P4Runtime messages sent to switch to given txt files.
        this_switch_name = 's' + str(this_switch_id)
        this_log_file = 'logs/' + this_switch_name + '-p4runtime-requests.txt'
        s = p4runtime_lib.bmv2.Bmv2SwitchConnection(
            name=this_switch_name,
            address=switch_arr[this_switch_id],
            device_id=this_switch_id,
            proto_dump_file=this_log_file)
        
        # Send master arbitration update message to establish this controller as
        # master (required by P4Runtime before performing any other write operation)
        s.MasterArbitrationUpdate()
        
        # Install the P4 program on the switches    
        s.SetForwardingPipelineConfig(p4info=p4info_helper.p4info, bmv2_json_file_path=bmv2_file_path)
        print("Installed P4 Program using SetForwardingPipelineConfig on switch")
        
        addCntrlTable(p4info_helper, s, next_switch_id)
        addContiguousSeqNoTable(idx_cache_size)
        circulateTable(p4info_helper, s, this_switch_port)
        for i in range(0, switch_arr):
            readSwitchIDtoPortTable(p4info_helper, sw, i, switch_port_arr[i])

        for i in range(0, storage_servers):
            addAckTable(p4info_helper, s, storage_servers[i], i)
            ipv4Routing(p4info_helper, s, storage_eth_addr[i], storage_server[i])
            checkUnwrittenTable(p4info_helper, s, port, dstAddr)

        for i in range(0, shards):
            appendShardIDtoPortTable(p4info_helper, sw, i, mcast_grp_num) # TODO mcast grp num?
            readShardIDtoPortTable(p4info_helper, sw, i, mcast_grp_num)
    
        processTailTable(p4info_helper, sw, len(membership), port, dstAddr)
    
        ring_heartbeat_thread = Thread(target = ring_heartbeat, args = (iface))
        ring_heartbeat_thread.start()
        print("Starting receiving thread")    

        while True:
            sleep(timeout) # TODO: Add exponential backoff
            if heartbeat == 0:
                view_change()
            elif heartbeat == -1:
                break
            heartbeat = 0

    except KeyboardInterrupt:
        print(" Shutting down.")
    except grpc.RpcError as e:
        printGrpcError(e)

    ShutdownAllSwitchConnections()

# Input:
#   - p4info: 
#   - bmv2-json:
#   - yaml-file: YAML specifying all necessary conditions for the program 
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='P4Runtime Controller')
    parser.add_argument('--p4info', help='p4info proto in text format from p4c',
                        type=str, action="store", required=False,
                        default='./build/log.p4.p4info.txt')
    parser.add_argument('--bmv2-json', help='BMv2 JSON file from p4c',
                        type=str, action="store", required=False,
                        default='./build/log.json')
    parser.add_argument('--yaml-file', help='YAML file for Ringworld',
                        type=str, action="store", required=True,
                        default='')
    args = parser.parse_args()

    if not os.path.exists(args.p4info):
        parser.print_help()
        print("\np4info file not found: %s\nHave you run 'make'?" % args.p4info)
        parser.exit(1)
    if not os.path.exists(args.bmv2_json):
        parser.print_help()
        print("\nBMv2 JSON file not found: %s\nHave you run 'make'?" % args.bmv2_json)
        parser.exit(1)
    if not os.path.exists(args.yaml):
        parser.print_help()
        print("\nYAML file not found: Can't advance")
        parser.exit(1)
    main(args.p4info, args.bmv2_json, args.yaml_file)
