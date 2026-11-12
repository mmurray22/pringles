Steps for running the packet generation experiments:

1) eval $(ssh-agent -s)
2) ssh-add /path/to/your/local/ssh_private_key
3) Create python environment, run ./setup.sh
4) source /bin/activate
5) ./bin/python3 /path/to/orchestration.py

Assumptions about port setup on the switches:
- Port numbers are already fully devport-encoded, not raw local port numbers
- Physical cabling matches the TOML, with no verification
- The intra-chip relay port is assumed to be genuinely free
- The topology is static for the whole run
- Exactly one true origin
- One shared cpu_interface per physical switch, always "enp5s0"

An explanation of the results:

- The maximum throughput that any of the recirculation ports can support 
is its bandwidth of 100 Gbps. Given that each back is 100 bytes not including
the ~20 byte preamble in the Ethernet frame, this gives us an expected ceiling of 104-125M pps
per recirculation port. 
- The packet generator can generate packets at a rate of ~115M pps. 
