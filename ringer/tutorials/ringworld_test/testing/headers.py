from scapy.all import *

TYPE_CNTRL = 0x0820
TYPE_MYTUNNEL = 0x1212
TYPE_IPV4 = 0x0800
TYPE_CLI_SEQ = 0x1414

class MyTunnel(Packet):
    name = "MyTunnel"
    fields_desc = [
        ShortField("pid", 0),
        ShortField("dst_id", 0)
    ]
    def mysummary(self):
        return self.sprintf("pid=%pid%, dst_id=%dst_id%")

class UpdateTail(Packet):
    fields_desc = [ IntField("tail", 0)]


class Cntrl(Packet):
    fields_desc = [ IntField("last_global_offset", 0),
                   IntField("global_seq_no", 0)]

class SeqNoReq(Packet):
    name = "SeqNoReq"
    fields_desc = [ IntField("local_sequence_no", 0)]
 
class StoreEntry(Packet):
    fields_desc = [IntField("log_idx", 0)]

class AckEntry(Packet):
    fields_desc = [IntField("log_idx", 0)]

class SendEntry(Packet):
    fields_desc = [IntField("log_idx", 0)]

class GetEntry(Packet):
    fields_desc = [IntField("log_idx", 0)]

bind_layers(Ether, Cntrl, type=TYPE_CNTRL)
bind_layers(Cntrl, MyTunnel)
bind_layers(MyTunnel, IP, pid=TYPE_IPV4)

bind_layers(Ether, SeqNoReq, type=TYPE_CLI_SEQ)
bind_layers(SeqNoReq, IP)
