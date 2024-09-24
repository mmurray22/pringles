#!/usr/bin/env python3

from headers import *


def expand(x):
    yield x
    while x.payload:
        x = x.payload
        yield x

def handle_pkt(pkt):
    if Cntrl in pkt:
        pkt.show2()
     #   for l in expand(pkt):
     #       if l.name=='SeqNo':
     #           print("RECEIVED: Seq no {}".format(l.seq_no))

def main():
    iface = 'eth0'
    print("sniffing on {}".format(iface))
    sniff(iface = iface,
          prn = lambda x: handle_pkt(x))

if __name__ == '__main__':
    main()
