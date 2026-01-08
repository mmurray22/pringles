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
import bfrt_grpc.client as gc
from ptf.thriftutils import *

p4_program_name = "sequencing_only"

loopback_port=68
logger = logging.getLogger('Test')
if not len(logger.handlers):
    logger.addHandler(logging.StreamHandler())

class SequencingTest(BfRuntimeTest):
    def setUp(self):
        client_id = 0
        BfRuntimeTest.setUp(self, client_id, p4_program_name)

    def initialize_tables(self, target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl):
        # Set default output port
        table_ipv4 = bfrt_info.table_get("MyIngress.ipv4_lpm")
        table_ipv4.info.key_field_annotation_add("hdr.ipv4.dstAddr", "ipv4")
        table_ipv4.info.data_field_annotation_add("dstAddr", "MyIngress.ipv4_forward", "mac")
        table_ipv4.entry_add(
                target, 
                [table_ipv4.make_key([gc.KeyTuple('hdr.ipv4.dstAddr', ip_addr, prefix_len=32)])],
                [table_ipv4.make_data(action_name="MyIngress.ipv4_forward", data_field_list_in=[gc.DataTuple(name="dstAddr", val=dstAddr), gc.DataTuple(name="port", val=recv_port)])])

        table_cntrl = bfrt_info.table_get("MyIngress.cntrl_id_to_ip")
        table_cntrl.info.key_field_annotation_add("hdr.cntrl.id", "int<32>")
        table_cntrl.entry_add(
                target, 
                [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.id', in_cntrl)])],
                [table_cntrl.make_data(action_name="MyIngress.cntrl_forward", data_field_list_in=[gc.DataTuple(name="port", val=loopback_port), gc.DataTuple(name="id", val=out_cntrl)])])

    def delete_tables(self, target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl):
        # Reset the tables to make multiple runs possible
        table_ipv4 = bfrt_info.table_get("MyIngress.ipv4_lpm")
        table_ipv4.info.key_field_annotation_add("hdr.ipv4.dstAddr", "ipv4")
        table_ipv4.info.data_field_annotation_add("dstAddr", "MyIngress.ipv4_forward", "mac")
        table_ipv4.entry_del(
                target, 
                [table_ipv4.make_key([gc.KeyTuple('hdr.ipv4.dstAddr', ip_addr, prefix_len=32)])])

        table_cntrl = bfrt_info.table_get("MyIngress.cntrl_id_to_ip")
        table_cntrl.info.key_field_annotation_add("hdr.cntrl.id", "int<32>")
        table_cntrl.entry_del(
                target, 
                [table_cntrl.make_key([gc.KeyTuple('hdr.cntrl.id', in_cntrl)])])

    def runTest(self):
        target = gc.Target(device_id=0, pipe_id=0xffff)
        
        # Get bfrt_info and set it as part of the test
        bfrt_info = self.interface.bfrt_info_get(p4_program_name)

        # Set default output port
        ip_addr='100.99.98.97'
        dstAddr='11:11:11:11:11:11'
        srcAddr='22:22:22:22:22:22'
        recv_port=loopback_port
        in_cntrl=1
        out_cntrl=1

        self.initialize_tables(target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl)

        
        try:
            #addr = socket.gethostbyname('100.99.98.97')
            ipkt = testutils.simple_control_packet(eth_dst=dstAddr, eth_src=srcAddr)
            testutils.send_packet(self, loopback_port, ipkt)
            print(testutils.format_packet(ipkt))

            #ipkt = testutils.simple_control_check_packet(eth_dst='11:11:11:11:11:11',
            #                                   eth_src='22:22:22:22:22:22')
            #testutils.send_packet(self, swports[0], ipkt)
            #print(testutils.format_packet(ipkt))

            #logger.info("Waiting for a reply...")
            #(rcv_dev, rcv_port, rcv_pkt, pkt_time) = \
            #    testutils.dp_poll(self, 0, recv_port, timeout=2)
            #logger.info("Received packet of size {:>15}".format(str(len(ipkt.__class__(rcv_pkt)))))
            #logger.info("Sent packet of size {:>15}".format(str(len(ipkt))))
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
        finally:
            logger.info("Test finished!")
            self.delete_tables(target, bfrt_info, ip_addr, dstAddr, recv_port, in_cntrl, out_cntrl)
