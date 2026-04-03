# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Pringles is a high-performance distributed replicated log system ("the log which scales to the datacenter"), written in C++17. It uses raw/UDP sockets for ultra-low latency networking and Protocol Buffers for message serialization, targeting ~415K+ packets/second throughput.

## Build and Run

**Setup dependencies** (first time):
```bash
./setup.sh
```

**Compile:**
```bash
meson setup build && cd build
meson compile
```

**Reconfigure** (e.g., to enable address sanitizer):
```bash
meson setup -Db_sanitize=address   # enable
meson setup -Db_sanitize=none      # disable
```

**Run all tests** (must be in `build/` directory):
```bash
sudo meson test
```

**Run a specific binary:**
```bash
cd build
sudo ./experiments/unit_tests/<executable> /path/to/config.yaml
```

All executables require `sudo` because they use raw sockets.

**Run experiments:**
```bash
cd experiments/scripts
python3 experiment.py config.toml
```

Pringles experiments use `experiments/scripts/pringles.toml` (or similar). Comparison system experiments use `kafka.toml` and `scalog.toml` in the same directory.

## Comparison Experiments

Pringles is benchmarked against Kafka and Scalog (LazyLog is blocked pending RDMA hardware). Each system has a TOML config in `experiments/scripts/` and a corresponding cycle function in `experiment.py`. The active system is selected via `[system] name = "..."` in the TOML.

**Kafka** (`kafka.toml`):
- `kafka_log_local_src`: local path to clone [mmurray22/kafka-log](https://github.com/mmurray22/kafka-log) for `sbt assembly`
- `kafka_log_jar_remote`: remote path where the fat jar is SCP'd on client nodes
- `kafka_dir`: Kafka broker install dir on broker nodes (downloaded remotely from `kafka_download_url`)
- Config is injected at runtime via classpath override: `/tmp/kafka-config/config.json` shadows the bundled config

**Scalog** (`scalog.toml`):
- `scalog_local_src`: local path to clone [chn0318/scalog](https://github.com/chn0318/scalog) for cross-compilation
- `path_discovery/order/data/client`: remote paths where each cross-compiled binary is SCP'd
- Cross-compiled locally with `GOOS=linux GOARCH=amd64 CGO_ENABLED=0`
- TODO: verify `.scalog.yaml` field names and client CLI flag names against the actual repo

**Setup behavior** (`run_comparison_setup = "True"` in TOML): on first run, `setup_kafka_nodes` / `setup_scalog_nodes` clone+build locally and SCP binaries to remote nodes. Safe to leave `"True"` — clone is skipped if the local directory already exists.

**Fair comparison parameters** (normalized across all systems):
- TCP/UDP transport only (no RDMA)
- No client-side batching; server-side batching allowed
- In-memory storage where possible
- `replication_factor = 2` default; `num_shards` configurable per TOML

## Architecture

### Core Components

**`code/src/network.cpp` / `code/include/network.h`** — The central networking library used by all protocol implementations. Handles raw socket and UDP communication, packet batching, epoll-based I/O, and a thread pool for sending. Concurrency is built on moodycamel's `ConcurrentQueue` and Intel TBB. Key constructor parameters: `send_port`, `recv_port`, `socket_type` ("RAW"/"UDP"), `batch_size`, `batch_on`, `batch_timeout`, `num_pkt_type`.

**`code/src/pringles_client.cpp`** — Implements the `LogClient` class (derived from `BaseClient`). Exposes `append`, `read`, `getTail`, `subscribe`, `trim`. Loads config from YAML, supports multi-threaded operation, and routes outgoing messages by packet type.

**`code/src/pringles_storage.cpp`** — Implements the `LogStorage` class (derived from `BaseStorage`). Stores log entries in-memory (`std::map<uint64_t, std::string>`). Supports storage types: `MEM_KV`, `NOSTORE`, `DISK_KV`, `HASHMAP`.

**`code/include/structs.h`** — Custom Ethernet/IP packet headers used for raw socket communication, plus helper constructors/destructors.

**`code/proto/ringclient.proto`** — Protobuf definitions for all message types (`Payload`, `AppendEntry`, `ReadEntry`, `GetTail`, `Subscribe`, `Trim`).

**`code/src/measure.cpp`** — Latency and throughput stats, tracked per nonce/thread/IP, with JSON export.

**`code/src/trace.cpp`** — Reads workload trace files (format: `<operation> <payload>` per line).

### Data Flow

1. Client builds a protobuf `Payload` message
2. `Network` class batches or immediately transmits via raw/UDP socket with custom Ethernet/IP headers
3. Receiver uses epoll to detect incoming packets, parses and routes to per-packet-type queues
4. Application layer processes request, sends response via `Network`

### Adding New Code

When adding new `.h` or `.cpp` files, register them in the appropriate `meson.build`:
- Headers: `code/include/meson.build`
- Sources: `code/src/meson.build`
- New test executables: `experiments/unit_tests/meson.build`

### YAML Configuration

Client and storage nodes are configured via YAML. Key fields:
- `log_level`: 1 = show debug, 5 = critical only (uses spdlog)
- `socket_type`: `"RAW"` or `"UDP"`
- `batch_on`: enable/disable batching
- `batch_size`: number of bytes per batch
- `packet_types`: list of packet type entries, each with a list of destination `ips`
- `sequencer_type`: integer [0, 1]
- `storage_type`: integer [0, 3)

See `experiments/yaml/README.md` for the full field reference and `experiments/yaml/` for example configs.

### Logging

Use `spdlog::debug(...)` for debug-level output and `spdlog::critical(...)` for important messages. Set `log_level: 1` in YAML to see debug output; `log_level: 5` to suppress it.
