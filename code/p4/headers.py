from scapy.all import *

TYPE_CONTROL = 0x0820
TYPE_APPEND = 0x0860
TYPE_READ = 0x0870
TYPE_TAIL = 0x0840
TYPE_ACK = 0x0850
TYPE_HEARTBEAT = 0x0880

class Cntrl(Packet):
    fields_desc = [ IntField("global_seq_no", 0),
                    BitField("ring_view", 0, 32),
                    BitField("id", 0, 32)]

class Append(Packet):
    fields_desc = [ BitField("cid", 0, 32),
                    BitField("nonce", "", 32),
                    IntField("g_idx", 0),
                    IntField("batch_size", 1),
                    BitField("shard_id", 0, 32),
                    BitField("ring_view", 0, 32),
                    BitField("status", 0, 32),
                    IntField("cntrl_pkt_it", 0)]
 
class Read(Packet):
    fields_desc = [ BitField("cid", 0, 32),
                    BitField("nonce", "", 32),
                    IntField("g_idx", 0),
                    IntField("shard_id", 0),
                    IntField("switch_id", 0),
                    IntField("unwritten", 0),
                    IntField("status", 0)]

class Tail(Packet):
    fields_desc = [ BitField("cid", 0, 32),
                    BitField("nonce", "", 32),
                    IntField("hops", 0),
                    IntField("tail_seq_no", 0)]

class Ack(Packet):
    fields_desc = [ IntField("g_idx", 0),
                    BitField("view", 0, 32),
                    BitField("switch_id", 0, 32),
                    BitField("status", 0, 32)]

class Heartbeat(Packet):
        fields_desc = [BitField("live",1, 32)]


bind_layers(Ether, IP, Heartbeat, type=TYPE_HEARTBEAT)
bind_layers(Ether, IP, Ack, type=TYPE_ACK)
bind_layers(Ether, IP, Read, type=TYPE_READ)
bind_layers(Ether, IP, Tail, type=TYPE_TAIL)
bind_layers(Ether, IP, Append, type=TYPE_APPEND)
bind_layers(Cntrl, IP, Ether, type=TYPE_CNTRL)
