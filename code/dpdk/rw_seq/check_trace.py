#!/usr/bin/env python3
"""
check_trace.py - check rw_seq behaviour against the protocol invariants.

Per member (any trace):
  P1  every token:  g_out == g_in + count
  P2  token GSN never goes backwards: g_in(k+1) >= g_out(k)
  P3  acks are strictly increasing, contiguous record indices 0,1,2,... (each
      admitted append acked once, in admission order)
  P4  every acked GSN was stamped at an EARLIER token visit, lies in that
      token's range [g_in+1, g_out], and is acked at a token whose g_in >= gsn
      (i.e., only after the token went around the whole ring)
  P5  liveness: a batch stamped at visit k is fully acked at the next visit
      whose g_in covers it (no append is held longer than necessary)

Against expected.json from gen_test_pcap.py (optional):
  E1  per-token g_in / count / g_out match exactly
  E2  output pcap: one forwarded token per input token, carrying g_out,
      addressed to the next member, with temp_local_seq_no = 0 and canary intact

Across members (pass several traces, e.g. from a real 2-node ring):
  G1  stamped ranges from all members are pairwise disjoint (no duplicate GSN)
  G2  together they cover seed+1 .. max with no gaps (total order, no holes)
"""
import argparse
import bisect
import json
import sys


def load_trace(path):
    tokens, acks = [], []
    pending_acks = []
    with open(path) as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.strip().split(",")
            if parts[0] == "A":
                pending_acks.append({"member": int(parts[1]), "gsn": int(parts[2]),
                                     "idx": int(parts[3]), "lat_ns": int(parts[4])})
            elif parts[0] == "T":
                t = {"member": int(parts[1]), "g_in": int(parts[2]),
                     "count": int(parts[3]), "g_out": int(parts[4]),
                     "visit": len(tokens)}
                for a in pending_acks:          # acks are logged before their token line
                    a["visit"] = t["visit"]
                    acks.append(a)
                pending_acks = []
                tokens.append(t)
    return tokens, acks


class Checker:
    def __init__(self):
        self.errors = 0

    def fail(self, msg):
        self.errors += 1
        if self.errors <= 20:
            print("  FAIL:", msg)

    def ok(self, name, before):
        print(f"  {'ok  ' if self.errors == before else 'FAIL'} {name}")


def check_member(path, ck):
    tokens, acks = load_trace(path)
    print(f"{path}: {len(tokens)} token visits, {len(acks)} acks")

    e = ck.errors
    for t in tokens:
        if t["g_out"] != t["g_in"] + t["count"]:
            ck.fail(f"P1 visit {t['visit']}: g_out {t['g_out']} != g_in {t['g_in']} + {t['count']}")
    ck.ok("P1 g_out = g_in + count", e)

    e = ck.errors
    for a, b in zip(tokens, tokens[1:]):
        if b["g_in"] < a["g_out"]:
            ck.fail(f"P2 visit {b['visit']}: g_in {b['g_in']} < previous g_out {a['g_out']}")
    ck.ok("P2 token GSN monotone", e)

    e = ck.errors
    for i, a in enumerate(acks):
        if a["idx"] != i:
            ck.fail(f"P3 ack #{i} has record index {a['idx']}")
            break
    for a, b in zip(acks, acks[1:]):
        if b["gsn"] <= a["gsn"]:
            ck.fail(f"P3 acks not increasing: {a['gsn']} then {b['gsn']}")
            break
    ck.ok("P3 each append acked once, in order", e)

    # Map each GSN to the visit that stamped it.
    e = ck.errors
    ranges = [(t["g_in"] + 1, t["g_out"], t["visit"]) for t in tokens if t["count"] > 0]
    ri = 0
    for a in acks:
        while ri < len(ranges) and ranges[ri][1] < a["gsn"]:
            ri += 1
        if ri == len(ranges) or not (ranges[ri][0] <= a["gsn"] <= ranges[ri][1]):
            ck.fail(f"P4 acked GSN {a['gsn']} was never stamped")
            continue
        stamped_visit = ranges[ri][2]
        if a["visit"] <= stamped_visit:
            ck.fail(f"P4 GSN {a['gsn']} acked at visit {a['visit']} but stamped at {stamped_visit}")
        if tokens[a["visit"]]["g_in"] < a["gsn"]:
            ck.fail(f"P4 GSN {a['gsn']} acked before the token covered it (g_in {tokens[a['visit']]['g_in']})")
    ck.ok("P4 acks only after a full ring rotation", e)

    e = ck.errors
    acked = {a["gsn"]: a["visit"] for a in acks}
    # g_in never decreases (P2), so binary search finds the first later visit
    # whose g_in covers a range. (A linear scan per range is quadratic and hangs
    # on large traces.)
    g_ins = [t["g_in"] for t in tokens]
    for lo, hi, v in ranges:
        i = bisect.bisect_left(g_ins, hi, v + 1)
        if i == len(g_ins):
            continue                     # run ended before the token came back
        due = tokens[i]["visit"]
        for gsn in (lo, hi):
            if acked.get(gsn) != due:
                ck.fail(f"P5 GSN {gsn} stamped at visit {v} should be acked at visit {due}, got {acked.get(gsn)}")
    ck.ok("P5 no append held longer than one rotation", e)

    if acks:
        lats = sorted(a["lat_ns"] for a in acks)
        print(f"  latency in trace (ns): p50={lats[len(lats)//2]} max={lats[-1]}")
    return tokens, acks


def check_expected(tokens, exp_path, out_pcap, ck):
    exp = json.load(open(exp_path))
    et = exp["tokens"]
    print(f"{exp_path}: {len(et)} expected token visits")
    e = ck.errors
    if len(tokens) != len(et):
        ck.fail(f"E1 {len(tokens)} visits in trace, expected {len(et)}")
    for t, x in zip(tokens, et):
        if (t["g_in"], t["count"], t["g_out"]) != (x["g_in"], x["count"], x["g_out"]):
            ck.fail(f"E1 visit {t['visit']}: got {(t['g_in'], t['count'], t['g_out'])}, "
                    f"expected {(x['g_in'], x['count'], x['g_out'])}")
    ck.ok("E1 GSNs and batch sizes match expected", e)

    if out_pcap:
        from scapy.all import rdpcap
        e = ck.errors
        outs = [p for p in rdpcap(out_pcap) if bytes(p)[12:14] == b"\x08\x20"]
        if len(outs) != len(et):
            ck.fail(f"E2 {len(outs)} tokens forwarded, expected {len(et)}")
        for i, (p, x) in enumerate(zip(outs, et)):
            raw = bytes(p)
            dst = ":".join(f"{b:02x}" for b in raw[0:6])
            gsn = int.from_bytes(raw[14:18], "big") << 32 | int.from_bytes(raw[18:22], "big")
            temp = int.from_bytes(raw[22:26], "big")
            if gsn != x["g_out"]:
                ck.fail(f"E2 forwarded token {i} carries GSN {gsn}, expected {x['g_out']}")
            if dst != exp["next_mac"].lower():
                ck.fail(f"E2 forwarded token {i} sent to {dst}, expected {exp['next_mac']}")
            if temp != 0 or raw[28] != 165:
                ck.fail(f"E2 forwarded token {i}: temp={temp} canary={raw[28]}")
        ck.ok("E2 forwarded tokens correct", e)

    return exp


def check_global(all_tokens, seed, other_ranges, ck):
    e = ck.errors
    rs = sorted([(t["g_in"] + 1, t["g_out"]) for t in all_tokens if t["count"] > 0] +
                [tuple(r) for r in other_ranges])
    for (a_lo, a_hi), (b_lo, b_hi) in zip(rs, rs[1:]):
        if b_lo <= a_hi:
            ck.fail(f"G1 overlapping GSN ranges [{a_lo},{a_hi}] and [{b_lo},{b_hi}]")
    ck.ok("G1 no GSN assigned twice", e)
    e = ck.errors
    expect = seed + 1
    for lo, hi in rs:
        if lo > expect:
            ck.fail(f"G2 gap: GSNs {expect}..{lo - 1} never assigned")
        expect = max(expect, hi + 1)
    ck.ok("G2 GSNs contiguous from seed+1", e)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("traces", nargs="+", help="trace file(s) written by rw_seq --trace")
    ap.add_argument("--expected", help="expected.json from gen_test_pcap.py (single-trace test)")
    ap.add_argument("--out-pcap", help="output pcap from the pcap device")
    ap.add_argument("--seed", type=int, help="initial GSN for the global check (multi-member runs)")
    a = ap.parse_args()

    ck = Checker()
    all_tokens = []
    for path in a.traces:
        tokens, _ = check_member(path, ck)
        all_tokens += tokens

    if a.expected:
        exp = check_expected(all_tokens, a.expected, a.out_pcap, ck)
        print("global:")
        check_global(all_tokens, exp["seed_gsn"], exp["other_ranges"], ck)
    elif a.seed is not None:
        print("global:")
        check_global(all_tokens, a.seed, [], ck)

    print(f"\n{'PASS' if ck.errors == 0 else 'FAIL'} ({ck.errors} errors)")
    sys.exit(0 if ck.errors == 0 else 1)


if __name__ == "__main__":
    main()
