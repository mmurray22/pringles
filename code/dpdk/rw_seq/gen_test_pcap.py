#!/usr/bin/env python3
"""
gen_test_pcap.py - build an input pcap for testing rw_seq with DPDK's pcap device.

It plays the role of everything outside the member under test:
  - client APPEND_PRE packets (what the Tofino packet generator produces),
  - token arrivals from the other ring member(s), whose GSN includes the
    other members' (random) batch sizes,
  - junk the sequencer must ignore (bad canary, other EtherTypes, other
    ring types, truncated packets).

It also writes expected.json with what a correct sequencer must do, which
check_trace.py compares against.

Edge cases covered: rounds with zero appends, a seed just below 2^32 so the
GSN carries into the upper 32-bit word, and a token arriving with a nonzero
temp_local_seq_no (which the switch egress folds into the GSN).
"""
import argparse
import json
import random

from scapy.all import Ether, IP, UDP, Raw, Packet, BitField, bind_layers, wrpcap

TYPE_CONTROL = 0x0820
TYPE_APPEND_PRE = 0x0863
TYPE_READ = 0x0870
MAGIC = 165


# Layer definitions match 64_bit_headers.p4 (and the working parts of headers.py).
class Cntrl(Packet):
    name = "Cntrl"
    fields_desc = [BitField("upper_global_seq_no", 0, 32),
                   BitField("lower_global_seq_no", 0, 32),
                   BitField("temp_local_seq_no", 0, 32),
                   BitField("ring_view", 0, 16)]


class RingType(Packet):
    name = "RingType"
    fields_desc = [BitField("type", 0, 16), BitField("shard_id", 0, 16),
                   BitField("pipe_ts", 0, 16), BitField("start_ts", 0, 16)]


class Append(Packet):
    name = "Append"
    fields_desc = [BitField("stream_id", 0, 32), BitField("upper_g_idx", 0, 32),
                   BitField("lower_g_idx", 0, 32), BitField("local_seq_cntr", 0, 32),
                   BitField("cntrl_pkt_it", 0, 16), BitField("exp_type", 0, 8),
                   BitField("seq_recirc", 0, 8), BitField("start_ts", 0, 32),
                   BitField("payload_size", 0, 16)]


class Canary(Packet):
    name = "Canary"
    fields_desc = [BitField("magic", MAGIC, 8)]


bind_layers(Ether, Cntrl, type=TYPE_CONTROL)
bind_layers(UDP, RingType)
bind_layers(RingType, Append, type=TYPE_APPEND_PRE)
bind_layers(Cntrl, Canary)
bind_layers(Append, Canary)


def token(dst, src, gsn, temp=0, view=1, magic=MAGIC):
    return (Ether(dst=dst, src=src, type=TYPE_CONTROL) /
            Cntrl(upper_global_seq_no=(gsn >> 32) & 0xFFFFFFFF,
                  lower_global_seq_no=gsn & 0xFFFFFFFF,
                  temp_local_seq_no=temp, ring_view=view) /
            Canary(magic=magic))


def append_pre(dst, src, payload, magic=MAGIC, exp_type=2):
    return (Ether(dst=dst, src=src) / IP(src="10.0.0.1", dst="10.0.0.2") /
            UDP(sport=5678, dport=1234) /
            RingType(type=TYPE_APPEND_PRE) /
            Append(exp_type=exp_type, payload_size=payload) /
            Canary(magic=magic) / Raw(b"P" * payload))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="in.pcap")
    ap.add_argument("--expected", default="expected.json")
    ap.add_argument("--rounds", type=int, default=200)
    ap.add_argument("--max-appends", type=int, default=40, help="max appends per round at this member")
    ap.add_argument("--max-other", type=int, default=40, help="max batch per round at the other member(s)")
    ap.add_argument("--seed-gsn", type=int, default=(1 << 32) - 1500, help="initial GSN (default crosses 2^32)")
    ap.add_argument("--payload", type=int, default=33)
    ap.add_argument("--member-mac", default="02:00:00:00:00:01", help="MAC of the member under test")
    ap.add_argument("--peer-mac", default="02:00:00:00:00:02", help="MAC of the previous ring member")
    ap.add_argument("--rng", type=int, default=1)
    a = ap.parse_args()

    rng = random.Random(a.rng)
    pkts = []
    exp_tokens = []          # what the member under test must do per token
    other_ranges = []        # GSN ranges taken by the simulated other member(s)

    g = a.seed_gsn           # GSN carried by the next token we send in
    pending = 0
    for r in range(a.rounds):
        n = 0 if r % 7 == 0 else rng.randint(0, a.max_appends)   # include empty rounds
        for _ in range(n):
            pkts.append(append_pre(a.member_mac, a.peer_mac, a.payload))
            pending += 1
            # Sprinkle junk that must be ignored and must not be admitted.
            x = rng.random()
            if x < 0.02:
                pkts.append(append_pre(a.member_mac, a.peer_mac, a.payload, magic=0))
            elif x < 0.03:
                pkts.append(Ether(dst=a.member_mac, src=a.peer_mac, type=0x88B5) / Raw(b"x" * 46))
            elif x < 0.04:
                p = append_pre(a.member_mac, a.peer_mac, a.payload)
                p[RingType].type = TYPE_READ
                pkts.append(p)
            elif x < 0.05:
                pkts.append(Ether(bytes(append_pre(a.member_mac, a.peer_mac, a.payload))[:50]))

        # One token per round; occasionally carry a nonzero temp_local_seq_no,
        # which the switch egress would have folded into the GSN.
        temp = rng.randint(1, 5) if r % 13 == 5 else 0
        if r % 17 == 3:
            pkts.append(token(a.member_mac, a.peer_mac, g, magic=0))   # bad canary: ignore
        pkts.append(token(a.member_mac, a.peer_mac, g, temp=temp))
        g_in = g + temp
        if temp:
            other_ranges.append([g + 1, g + temp])
        g_out = g_in + pending
        exp_tokens.append({"g_in": g_in, "count": pending, "g_out": g_out})
        pending = 0

        # The other member(s) take a batch before the token comes back.
        other = rng.randint(0, a.max_other)
        if other:
            other_ranges.append([g_out + 1, g_out + other])
        g = g_out + other

    wrpcap(a.out, pkts)
    with open(a.expected, "w") as f:
        json.dump({"seed_gsn": a.seed_gsn, "tokens": exp_tokens, "other_ranges": other_ranges,
                   "member_mac": a.member_mac, "next_mac": a.peer_mac}, f)
    total = sum(t["count"] for t in exp_tokens)
    print(f"wrote {a.out}: {len(pkts)} packets, {a.rounds} tokens, {total} valid appends")
    print(f"wrote {a.expected}")


if __name__ == "__main__":
    main()
