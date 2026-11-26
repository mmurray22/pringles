#!/usr/bin/env bash
# Two-member ring over a veth pair on one machine (needs >= 3 cores: members on 1 and 2).
# Functional test only: AF_PACKET and a shared host make the latency numbers meaningless.
#
# Each member runs in its own network namespace. DPDK's AF_PACKET driver joins a
# per-namespace "fanout group" whose id is derived from (pid ^ interface index); with
# both members in one namespace the ids can collide, and the kernel rejects the second
# one with "could not set PACKET_FANOUT ... Invalid argument". Separate namespaces
# make that impossible.
#
# Usage: sudo bash run_veth_ring.sh [seconds] [appends_per_sec]
set -euo pipefail
cd "$(dirname "$0")"
DUR=${1:-10}; RATE=${2:-100000}

cleanup() {
    ip netns del rw1 2>/dev/null || true
    ip netns del rw2 2>/dev/null || true
}
cleanup
ip link del vA 2>/dev/null || true   # leftover pair from the old version of this script
trap cleanup EXIT

# Namespaces rw1 and rw2, joined by veth pair vA (in rw1) <-> vB (in rw2).
ip netns add rw1
ip netns add rw2
ip link add vA netns rw1 type veth peer name vB netns rw2
ip -n rw1 link set lo up; ip -n rw1 link set vA up
ip -n rw2 link set lo up; ip -n rw2 link set vB up

rm -f t1.csv t2.csv member2.log

# Member 2 first (it just waits for the token), in the background.
ip netns exec rw2 ./rw_seq --no-huge -m 512 --no-pci -l 2 --file-prefix m2 \
  --log-level=lib.eal:error --vdev 'net_af_packet0,iface=vB' -- \
  --id 2 --my-mac 02:00:00:00:00:02 --next-mac 02:00:00:00:00:01 \
  --gen-rate "$RATE" --duration $((DUR + 2)) --warmup 1 --trace t2.csv > member2.log 2>&1 &
M2=$!
sleep 1
if ! kill -0 "$M2" 2>/dev/null; then
    echo "member 2 failed to start:"; cat member2.log; exit 1
fi

# Member 1 injects the token.
ip netns exec rw1 ./rw_seq --no-huge -m 512 --no-pci -l 1 --file-prefix m1 \
  --log-level=lib.eal:error --vdev 'net_af_packet0,iface=vA' -- \
  --id 1 --my-mac 02:00:00:00:00:01 --next-mac 02:00:00:00:00:02 \
  --inject --inject-delay 0.5 --gen-rate "$RATE" --duration "$DUR" --warmup 1 --trace t1.csv

wait "$M2" || true
echo "--- member 2 ---"; tail -5 member2.log
python3 check_trace.py t1.csv t2.csv --seed 0
