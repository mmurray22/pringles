from scapy.all import *

TYPE_LINK_TEST = 0x0711
TYPE_IP= 0x800

class PktgenTimerHeader(Packet):
    fields_desc = [
        BitField("_pad0", 0, 3),      # 3 bits
        BitField("pipe_id", 0, 2),    # 2 bits
        BitField("app_id", 0, 3),     # 3 bits (Total 8 bits / 1 byte)
        ByteField("_pad1", 0),        # 8 bits / 1 byte
        ShortField("batch_id", 0),    # 16 bits / 2 bytes
        ShortField("packet_id", 0)]    # 16 bits / 2 bytes

class LinkTest(Packet):
    fields_desc = [ BitField("start_ts", 0, 48),
                    BitField("end_ts", 0, 48),
                    BitField("hop", 0, 8)]

bind_layers(Ether, LinkTest, type=TYPE_LINK_TEST)
