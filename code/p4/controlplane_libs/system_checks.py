#### Keeps track of ack count table TODO
#### Keeps track of global sequence number map TODO

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
from headers import *
import ptf
import ptf.dataplane as dataplane
from ptf import config
import ptf.testutils as testutils
from bfruntime_client_base_tests import BfRuntimeTest
import pltfm_pm_rpc as pltfm_pm
import bfrt_grpc.client as gc
import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2
from ptf.thriftutils import *


class SystemChecks:
    def batch_check(self, bfrt_info, target, interface, duration):
        #os.system("taskset -p -c 0 {}".format(os.getpid()))
        # This creates a raw socket exactly like tcpdump
        recv_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(0x0003))
        recv_sock.bind((interface, 0))
        logger.info("[*] Packet Gen Socket Open and Listening...")
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
            #logger.info(data_dict)
            logger.info("Total Active Batches: {}".format(data_dict['MyIngress.active_batches.f1'][1]))


