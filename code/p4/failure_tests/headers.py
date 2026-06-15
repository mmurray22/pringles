from scapy.all import *

TYPE_APPEND = 0x0860
TYPE_TAIL = 0x0840
TYPE_CONTROL = 0x0820; 
TYPE_CONTROL_CHECK = 0x0880; 

class Cntrl(Packet):
    fields_desc = [ IntField("global_seq_no", 0),
                    BitField("ring_view", 0, 32),
                    BitField("pkt_id", 0, 32)]

class Cntrl_Check(Packet):
    fields_desc = [ IntField("switch_global_seq_no", 0)]

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
bind_layers(IP, Tail, type=TYPE_TAIL)
bind_layers(IP, Append, type=TYPE_APPEND)
bind_layers(Ether, Cntrl, type=TYPE_CONTROL)
bind_layers(IP, Cntrl_Check, type=TYPE_CONTROL_CHECK)
