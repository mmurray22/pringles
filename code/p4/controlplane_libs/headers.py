from scapy.all import *

TYPE_IP= 0x800
TCP_PROTOCOL = 0x6
UDP_PROTOCOL = 0x11
TYPE_APPEND = 0x0860
TYPE_APPEND_WAIT = 0x0862
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

class Canary(Packet):
    fields_desc = [ BitField("magic", 165, 8)]

class Hello(Packet):
    fields_desc = [ BitField("hello", 0, 32)]

class RingType(Packet):
    fields_desc = [ BitField("type", 0x0, 16),
                    BitField("num_entries", 1, 16),
                    BitField("shard_id", 0, 16),
                    BitField("switch_to_process", 1, 32),
                    BitField("start_ts", 0, 48),
                    BitField("end_ts", 0, 48),
                    BitField("raw_elapsed_time", 0, 16),
                    BitField("pipe_ts", 0, 32)]
class Append(Packet):
    fields_desc = [ BitField("nonce", 0, 16),
                    BitField("payload_size", 0, 16),
                    BitField("stream_id", 0, 32),
                    BitField("g_idx", 0, 32), # TODO: 64
                    BitField("cntrl_pkt_it", 0, 16), # TODO: 64
                    BitField("client_ip", 0, 32),
                    BitField("recv_port", 0, 16),
                    BitField("exp_type", 0, 16)]
class Read(Packet):
    fields_desc = [ BitField("nonce", 0, 32),
                    BitField("payload_size", 0, 32),
                    BitField("stream_id", 0, 32),
                    BitField("g_idx", 0, 32),
                    BitField("ring_view", 0, 16),
                    ShortField("recv_port", 0),
                    BitField("client_ip", 0, 32)]
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
bind_layers(RingType, Append, type=TYPE_APPEND_WAIT)
bind_layers(RingType, Append, type=TYPE_APPEND_RESP)
bind_layers(RingType, Append, type=TYPE_SUB_RESP)
bind_layers(RingType, Read, type=TYPE_READ)
bind_layers(RingType, Read, type=TYPE_READ_RESP)
bind_layers(RingType, Subscribe, type=TYPE_SUB)
bind_layers(RingType, Tail, type=TYPE_TAIL)
bind_layers(Ether, Cntrl, type=TYPE_CONTROL)
bind_layers(Cntrl, Canary)
bind_layers(Append, Canary)
bind_layers(Read, Canary)
bind_layers(Subscribe, Canary)
bind_layers(Tail, Canary)
