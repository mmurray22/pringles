# NOTE: The code in this repo was generate from my existing P4/python data plane + control plane implementation
# I converted it from switch hardware (Tofino specifically) implementation into a DPDK software-only implementation here
# This generation was done with the assistance of an LLM

# rw_seq: DPDK Ringworld ring member


A software version of one Ringworld ring member (one Tofino pipe), mirroring the
`exp_type = 2`, `ack_threshold = 0` experiment: appends are generated inside the
sequencer, stamped when the token arrives, held until the token has gone around the
whole ring, then acked, with latency measured from admission to ack-ready.

Files:

- `rw_seq.c`: the sequencer (single core, single port, single queue)
- `ringworld_hdrs.h`: wire formats matching `64_bit_headers.p4`
- `gen_test_pcap.py`, `check_trace.py`: test input generator and invariant checker
- `run_pcap_test.sh`: correctness test, no NIC needed
- `run_veth_ring.sh`: two-member ring on one machine over a veth pair

## Build

Needs an installed DPDK (the restricted 23.11 build from setup is fine) and `python3-scapy`.

```bash
cd /path/to/pringles/code/dpdk/rw_seq
make            # links against the shared DPDK libraries
make static     # use this if the shared build can't find the NIC drivers at runtime
```

## Test 1: correctness on any machine (GCP VM)

```bash
sudo ./run_pcap_test.sh
```

Plays a client plus the other ring member from a pcap (including bad canaries,
junk packets, empty rounds, a nonzero `temp_local_seq_no`, and GSNs crossing 2^32),
then checks the trace and forwarded tokens. It should end with `PASS`.

## Test 2: two-member ring on one machine (GCP VM, 4 vCPUs)

```bash
sudo ./run_veth_ring.sh 10 100000     # seconds, appends/s per member
```

Runs members 1 and 2 on cores 1 and 2, connected by a veth pair, and checks the
global invariants (no GSN assigned twice, no gaps). This is a functional test only:
AF_PACKET over veth is slow, so ignore the latency numbers.

## Running on CloudLab (r6615 / d6515)

Ring of two members = two nodes, matching your two-pipe Tofino ring. On each node:

```bash
# experiment NIC: PCI address, MAC, NUMA node
ethtool -i <iface> | grep bus-info
cat /sys/class/net/<iface>/address
cat /sys/class/net/<iface>/device/numa_node
echo 2048 | sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
```

Start member 2 first, then member 1 (which injects the token). Use a core on the
NIC's NUMA node, and always pass `-a` with the experiment NIC's PCI address so DPDK
leaves the control NIC alone:

```bash
# node B
sudo ./rw_seq -l 2 -a <PCI_B> -- --id 2 --next-mac <MAC_A> \
     --gen-rate 1e6 --duration 40 --warmup 5
# node A
sudo ./rw_seq -l 2 -a <PCI_A> -- --id 1 --next-mac <MAC_B> --inject \
     --gen-rate 1e6 --duration 35 --warmup 5
```

Each prints per-second stats (acks/s, average/p50/p99 latency, token rotation time,
queue depths) and a final summary with p50/p90/p99/p99.9/max.

If a member can't transmit the token, it prints a `FATAL: TOKEN LOST` banner, marks
its results invalid, and exits with status 2 (on the switch this would trigger a view
change). Check the exit status in your experiment scripts and rerun such runs. The
other member will show tokens/s dropping to 0; stop it with Ctrl-C.

For a correctness run on real hardware, add `--trace tN.csv` at a low rate (e.g.
`--gen-rate 10000`) and run `python3 check_trace.py t1.csv t2.csv --seed 0`.
Tracing writes a line per append, so leave it off for performance runs.

## Throughput-latency curve (item 6)

Sweep `--gen-rate` on both members (e.g. 1e5, 1e6, 5e6, 1e7, 2e7, ...) and record the
final throughput and latency. The knee is where `pending`/`waiting` start growing
or `drops` becomes nonzero: the single core can't keep up past that point.

## Comparing with the Tofino numbers

- Both measure admission to ack-ready inside the sequencer, with no client network hop.
- Token rotation time is the main term in latency: an append waits for the rest of its
  current batch window plus one full rotation. On the Tofino a hop is a pipe-to-pipe
  transfer; here it is NIC → switch → NIC between hosts. That difference is what the
  comparison shows.
- The Tofino admission meter is not modeled. If the record ring fills, new appends
  are dropped and counted in `drops`.
