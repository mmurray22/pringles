#!/usr/bin/env bash
# Correctness test on any Linux box: no NIC, no hugepages.
# Usage: sudo ./run_pcap_test.sh
set -euo pipefail
cd "$(dirname "$0")"
python3 gen_test_pcap.py --out in.pcap --expected expected.json
rm -f out.pcap trace.csv
./rw_seq --no-huge -m 512 --no-pci -l 0 --log-level=lib.eal:error \
  --vdev 'net_pcap0,rx_pcap=in.pcap,tx_pcap=out.pcap' -- \
  --id 1 --my-mac 02:00:00:00:00:01 --next-mac 02:00:00:00:00:02 \
  --idle-exit 1 --stats 0 --trace trace.csv
python3 check_trace.py trace.csv --expected expected.json --out-pcap out.pcap
