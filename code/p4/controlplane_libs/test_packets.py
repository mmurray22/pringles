from scapy.all import *
from headers import *

class TestSendingPackets: 
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
        pkt_buffer = bytes(pkt)
        sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW)
        sock.bind((interface, 0))
        sock.send(pkt_buffer)

        # destination_ip: 10.229.49.9
    # destination_port: 5005
    def run_jump_sniff(self, external_interface, listen_ip, listen_port, tofino_interface, dstAddr, srcAddr, ip_addr, destination_ip, destination_port):
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
                if dst_ip == destination_ip and protocol == 17:
                    u_header = raw_data[34:42]
                    udph = struct.unpack('!HHHH', u_header)
                    dst_port = udph[1]
                    if dst_port == destination_port:
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
        sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW)
        sock.bind((interface, 0))
        pkt[Append].nonce = nonce 
        sock.send(pkt_buffer)

    def receive_pkt_from_tofino(self, bfrt_info, target, interface, duration):
        os.system("taskset -p -c 0 {}".format(os.getpid()))
        # This creates a raw socket exactly like tcpdump
        recv_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(0x0003))
        recv_sock.bind((interface, 0))
        # The Magic Constant: 18 (PACKET_IGNORE_OUTGOING)
        # This prevents the socket from receiving packets sent by the local host
        recv_sock.setsockopt(263, 18, 1)

        logger.info("[*] Tofino Receive Socket Open and Listening...")
        # Block until 1 packet is received
        start_time = time.time()
        while (time.time() - start_time) < duration:
            try:
                raw_data, addr = recv_sock.recvfrom(65535)
                pkttype = addr[2]
                if pkttype == 4:
                    #logger.info("Discarding: This is an OUTGOING packet we just sent.")
                    continue
                ether_pkt = Ether(raw_data)
                pkttimer = PktgenTimerHeader(raw_data)
                if ether_pkt.haslayer(RingType):
                    if ether_pkt[RingType].type == TYPE_SUB:
                        logger.info("RECEIVED a packet with TYPE_SUB")
                        ether_pkt.show()
                        if ether_pkt.haslayer(Subscribe):
                            self.update_stream_subscriber_table(bfrt_info, target, ether_pkt[Subscribe].stream_id, ether_pkt[Subscribe].subscribe_port)
                    elif ether_pkt[RingType].type == TYPE_APPEND:
                        logger.info("RECEIVED a packet with TYPE_APPEND")
                        ether_pkt.show()
                    elif ether_pkt[RingType].type == TYPE_APPEND_RESP:
                        logger.info("RECEIVED a packet with TYPE_APPEND_RESP")
                        ether_pkt.show()
                    elif ether_pkt[RingType].type == TYPE_SUB_RESP:
                        logger.info("RECEIVED a packet with TYPE_SUB_RESP")
                        ether_pkt.show()
                    elif ether_pkt[RingType].type == TYPE_TAIL:
                        logger.info("RECEIVED a packet with TYPE_TAIL")
                        ether_pkt.show()
                    elif ether_pkt.haslayer(Cntrl):
                        logger.info("RECEIVED a packet with TYPE_CNTRL")
                        ether_pkt.show()
            except socket.timeout:
	        logger.info("Halted: Waiting for packet to send tofino request")
		continue
    

