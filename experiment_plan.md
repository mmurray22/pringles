# Comparison Systems Experiment Plan

## Goal

Add experiment cycle functions to `experiments/scripts/experiment.py` that benchmark Scalog, LazyLog, and Kafka against Pringles, without requiring a custom switch dataplane. Each new function follows the same SSH/SCP orchestration pattern as `run_experiment_cycle` but replaces the switch/sequencer step with the system-specific startup logic.

All systems must be configured to use the **same controlled parameters** so results are directly comparable. Parameters that cannot yet be determined should be surfaced as explicit TOML fields rather than hardcoded.

## Overview of Existing Infrastructure

`run_experiment_cycle` in `experiment.py` orchestrates experiments via SSH/SCP:
1. Generates YAML configs locally, SCPs them to remote machines
2. SSHes into each machine to start processes (storage servers, switch/sequencer, clients) in the background
3. Waits for the client process to finish
4. SCPs JSON result files back to a local results directory
5. A shared `process_and_aggregate_results` + `plot_results` step runs after all cycles

The key difference for comparison systems: **no custom switch or dataplane process**. The switch startup block (steps 7–8 in the existing cycle) is simply omitted.

All result files must still produce the same JSON schema used by `process_and_aggregate_results`:
```json
{
  "throughput": <float, ops/sec>,
  "avg_latency": <float, ms>,
  "batch_size": <int>
}
```

Each comparison system's client benchmark must write a JSON file with this schema (either natively, or via a thin wrapper script) to `~/` on the client machine so the existing `copy_results_back` function can retrieve it.

---

## Fair Comparison: Normalized Parameters

For results to be meaningful, all systems must be configured to use the same values for every parameter that affects performance. These are divided into **fixed** (same value enforced for all systems) and **configurable** (set once in the TOML and passed to each system).

### Fixed Constraints (enforced in code, not TOML)

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Network transport | **TCP or UDP** (no RDMA) | Pringles uses UDP; Scalog uses gRPC/TCP natively; Kafka uses TCP natively. **LazyLog is RDMA-only** (confirmed: eRPC with InfiniBand/RoCE, no TCP/UDP fallback in the build). LazyLog experiments therefore require RDMA-capable CloudLab nodes (e.g., machines with Mellanox ConnectX-4). |
| Client-side batching | **Disabled on client, allowed on server** | Pringles `batch_on = false`; Kafka producer `linger.ms=0, batch.size=1`. Server-side / broker-side batching reflects each system's natural behavior and should be left at its default. |
| Storage persistence | **In-memory / no disk flush** | Eliminates disk I/O as a confounding variable. Pringles uses `MEM_KV`; configure other systems equivalently. |

### Configurable Parameters (TOML fields, applied uniformly to all systems)

These are set once in the `[comparison_parameters]` TOML section and must be explicitly plumbed into each system's startup config. The intent is that changing one value here changes it for every system in the same run.

| TOML field | What it controls |
|------------|-----------------|
| `num_shards` | Number of storage shard/server nodes. Pringles storage count comes from `stor_ips`; other systems must be given the same count. |
| `num_sequencer_nodes` | Number of ordering/sequencer nodes. Pringles uses one switch/sequencer; comparison systems should match. |
| `message_size` | Payload size in bytes (already exists as `message_size` in `[experiment_parameters]`). |
| `num_client_threads` | Client concurrency (already exists; carried through `[[experiment]]` blocks). |
| `experiment_duration` | How long the benchmark runs in seconds (already exists). |
| `replication_factor` | Replication factor for systems that support it (default: 2). |
| `num_partitions` | Kafka-specific: number of topic partitions. Should equal `num_shards` for a fair comparison. For non-Kafka systems this field is ignored. |

### Example `[comparison_parameters]` block in TOML

```toml
[comparison_parameters]
num_shards = 1               # TBD: start with 1, sweep to find saturation point
num_sequencer_nodes = 1      # TBD: match Pringles single-sequencer config
replication_factor = 2
num_partitions = 1           # Kafka only; should match num_shards
```

These values are explicitly marked **TBD** because the right baseline numbers depend on the hardware available and the specific comparison being made. They should be decided before the first real experiment run.

---

## TOML Config Structure for Comparison Systems

Each comparison system has its own TOML config (e.g., `scalog.toml`, `lazylog.toml`, `kafka.toml`). These share the same top-level sections as the existing `config.toml` but drop `[routing]` and `switch_*` fields. A new `[system]` section identifies which cycle function to invoke, and `[comparison_parameters]` holds the normalized values described above.

```toml
[system]
name = "scalog"  # or "lazylog" or "kafka"

[network_setup]
stor_ips = ["10.10.1.4"]          # one entry per shard; count must match num_shards
stor_net_ifs = ["enp23s0f0np0"]
seq_ips = ["10.10.1.3"]           # sequencer/ordering nodes; count must match num_sequencer_nodes
cli_ips = ["10.10.1.2"]
cli_net_ifs = ["enp23s0f0np0"]
ssh_key = "/proj/ove-PG0/murray/cloudlab"
ssh_user = "murray22"

[program_paths]
path_server = "..."               # filled in per system
path_client = "..."
run_setup_script = "False"

[comparison_parameters]
num_shards = 1                    # TBD
num_sequencer_nodes = 1           # TBD
replication_factor = 2
num_partitions = 1                # Kafka only

[experiment_parameters]
experiment_name = "scalog_baseline"
json_name = "scalog_run"
experiment_duration = 30
warm_up = 5
cool_down = 5
message_size = 100
num_client_threads = 10
log_level = 6
num_failures = 0

[[experiment]]
num_client_threads = 10
json_name = "ten_clients"

[[experiment]]
num_client_threads = 20
json_name = "twenty_clients"
```

The `main` function dispatch in `experiment.py` should read `config['system']['name']` and call the appropriate cycle function:

```python
system = base_config.get('system', {}).get('name', 'pringles')
cycle_fn = {
    'pringles': run_experiment_cycle,
    'scalog':   run_experiment_cycle_scalog,
    'lazylog':  run_experiment_cycle_lazylog,
    'kafka':    run_experiment_cycle_kafka,
}.get(system, run_experiment_cycle)
```

---

## System-Specific Experiment Cycles

### 1. Scalog

**Repo:** https://github.com/chn0318/scalog
**Language:** Go 1.22+
**Components:** Discovery node, Order layer nodes, Data (storage) layer nodes, client

**Build (one-time, on each remote machine):**
```bash
git clone https://github.com/chn0318/scalog
cd scalog && go build -mod=vendor .
```

**Normalized configuration:**
- `num_shards` Data layer nodes, matching `stor_ips` count
- `num_sequencer_nodes` Order layer nodes, matching `seq_ips` count
- Replication factor set to `replication_factor` in `.scalog.yaml`
- No client-side batching in Scalog — the client sends individual records directly to the data layer. Both batching interval parameters in `.scalog.yaml` are server-side (order layer cut aggregation); leave them at their defaults.
- Transport: TCP (Scalog uses gRPC over TCP natively; no change needed)

**Config format:** `.scalog.yaml` — YAML with fields for node IPs, ports, replication factor, batching interval.
**Control CLI:** `scalogctl start --config <path>` / `scalogctl stop --config <path>`

**`run_experiment_cycle_scalog` outline:**
```
function run_experiment_cycle_scalog(config, exp_index, local_results_dir):
    1. Read comparison_parameters: num_shards, num_sequencer_nodes, replication_factor
    2. Assert len(stor_ips) == num_shards and len(seq_ips) == num_sequencer_nodes
    3. Generate .scalog.yaml with node IPs, ports, replication_factor (server-side batching intervals left at defaults)
    4. SCP config to all nodes
    5. SSH into discovery node: start discovery service
    6. SSH into seq_ips[0..num_sequencer_nodes]: start order layer nodes
    7. SSH into stor_ips[0..num_shards]: start data servers
    8. Wait SERVER_START_DELAY seconds
    9. SSH into client machine: run scalog benchmark client
       - Client must write results JSON to ~/  with throughput + avg_latency fields
       - If no native JSON output: run parse_results.py wrapper after benchmark
    10. Wait for client process to complete
    11. SCP JSON results back via copy_results_back()
    12. SCP server logs back via copy_log_file_back()
    13. Stop server processes (scalogctl stop or kill_remote_process())
```

**Setup note:** `scalogctl` is not pre-installed on CloudLab. It must be built from source as part of the Scalog repo (`go build -mod=vendor .` produces the binary). Add this to the setup step for Scalog nodes.

**Open questions:**
- Output format of the native Scalog benchmark client — likely stdout text; will need `parse_results.py`

---

### 2. LazyLog

**Repo:** https://github.com/dassl-uiuc/LazyLog-Artifact
**Language:** C++ with CMake
**Components:** Sequencing layer (in-memory ring), Storage shards, Bridge/cons_svr coordinator, client

**Build (one-time, on each remote machine or shared filesystem):**
```bash
git clone --recursive https://github.com/dassl-uiuc/LazyLog-Artifact
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCORFU=OFF
cmake --build build -j
```

**Normalized configuration:**
- `num_shards` storage shard servers, matching `stor_ips` count
- `num_sequencer_nodes` sequencing layer nodes, matching `seq_ips` count
- Replication factor set to `replication_factor`
- **Transport: RDMA required.** Confirmed: LazyLog uses eRPC with InfiniBand/RoCE exclusively (`add_subdirectory(RDMA)` in CMakeLists.txt; no TCP/UDP transport flag exists). LazyLog experiments must be run on CloudLab nodes with Mellanox ConnectX-4 or equivalent RDMA hardware. Use the `-DTRANSPORT=infiniband -DROCE=ON` eRPC build flags.
- Client-side batching: disabled if configurable; server-side batching left at default

**Startup sequence:**
1. Start storage shard servers (`num_shards` processes across `stor_ips`)
2. Start sequencing layer nodes (`num_sequencer_nodes` processes across `seq_ips`)
3. Start bridge/cons_svr coordinator
4. Start benchmark client (`append_bench`)

**`run_experiment_cycle_lazylog` outline:**
```
function run_experiment_cycle_lazylog(config, exp_index, local_results_dir):
    1. Read comparison_parameters: num_shards, num_sequencer_nodes, replication_factor
    2. Assert len(stor_ips) == num_shards and len(seq_ips) == num_sequencer_nodes
    3. Generate LazyLog config files with shard IPs, sequencer IPs, message_size,
       replication_factor, TCP transport flag
    4. SCP configs to all nodes
    5. SSH into stor_ips[0..num_shards]: start shard servers
    6. SSH into seq_ips[0..num_sequencer_nodes]: start sequencing layer
    7. SSH into bridge node (can share an existing node): start cons_svr
    8. Wait SERVER_START_DELAY seconds
    9. SSH into client machine: run append_bench with num_client_threads, message_size,
       experiment_duration, output path
       - LazyLog stores results in logs_<num_client>_<msg_size>_<num_shards>/
       - Run parse_results.py after to convert to standard JSON schema
    10. Wait for client process to complete
    11. SCP JSON results back
    12. SCP server logs back
    13. Kill server processes via kill_remote_process()
```

**Hardware requirement:** RDMA-capable nodes are mandatory. Plan CloudLab experiments accordingly — select a node type with Mellanox ConnectX-4 NICs and provision enough nodes for shards + sequencer + client.

**Open questions:**
- Exact CLI flags for `append_bench`: num threads, duration, output directory
- Whether `cons_svr` needs its own dedicated node or can share with a shard/sequencer node

---

### 3. Kafka (mmurray22/kafka-log)

**Repo:** https://github.com/mmurray22/kafka-log
**Language:** Scala / SBT (fork of scrooge-kafka)
**Components:** Kafka broker(s), ZooKeeper or KRaft controller, producer/consumer client

**Build (one-time, on each remote machine):**
```bash
git clone https://github.com/mmurray22/kafka-log
cd kafka-log && sbt compile
# Kafka broker installed separately via Apache Kafka distribution
```

**Configuration:** All parameters are read from a `config.json` file (not CLI arguments). The experiment cycle generates this file locally and SCPs it to the client machine before running. Key fields:

```json
{
    "topic1": "<topic_name>",
    "broker_ips": "10.10.1.3:9092",
    "rsm_size": 2,
    "benchmark_duration": 30,
    "warmup_duration": 5,
    "cooldown_duration": 5,
    "message": "<payload_of_message_size_bytes>",
    "read_from_pipe": false,
    "output_path": "/tmp/kafka-output",
    "write_dr": false,
    "write_ccf": false
}
```

`rsm_size` maps to the Kafka replication factor (`replication_factor`). `broker_ips` is a comma-separated list of `seq_ips`.

**Normalized configuration:**
- `num_shards` maps to number of topic **partitions** (`num_partitions`, should equal `num_shards`)
- `num_sequencer_nodes` maps to number of Kafka broker nodes on `seq_ips`
- `replication_factor` set via `rsm_size` in `config.json` and `--replication-factor` at topic creation
- **Transport:** Kafka uses TCP natively — no change needed
- Client-side batching disabled: set `linger.ms=0` and `batch.size=1` in `server.properties` for the producer; broker-side batching left at default
- Use **KRaft mode** (no ZooKeeper) to reduce node count requirements

**Output format (confirmed from source):**
- **Producer** (`sbt "runMain main.Producer"`) prints to stdout only:
  ```
  Messages Sent: 1000
  Overall Throughput: 5000.0 msg/s
  ```
  No latency reported.
- **Consumer** (`sbt "runMain main.Consumer"`) writes JSON to `/tmp/output.json`:
  ```json
  {"Overall_Throughput_MPS": 4980.0, "Total_Messages": 1000, ...}
  ```
  And prints per-message latency to stdout: `Kafka message processing latency: 42ms`

For this experiment, run both Producer and Consumer in parallel. The `parse_results.py` wrapper should: (1) read `Overall_Throughput_MPS` from `/tmp/output.json`, (2) parse per-message latency lines from consumer stdout to compute `avg_latency`, (3) write the standard JSON schema.

**`run_experiment_cycle_kafka` outline:**
```
function run_experiment_cycle_kafka(config, exp_index, local_results_dir):
    1. Read comparison_parameters: num_shards, num_sequencer_nodes, replication_factor,
       num_partitions
    2. Assert len(seq_ips) == num_sequencer_nodes
    3. Generate server.properties for each broker (KRaft mode, listeners, log.dirs,
       node IDs, linger.ms=0, batch.size=1)
    4. SCP server.properties to each seq_ip
    5. SSH into seq_ips[0..num_sequencer_nodes]: format storage and start broker
       (bin/kafka-server-start.sh)
    6. SSH into seq_ips[0]: create topic with num_partitions and replication_factor
       via kafka-topics.sh (run_remote_command_sync)
    7. Generate config.json with broker_ips, rsm_size=replication_factor,
       benchmark_duration, warmup_duration, message=(message_size bytes), topic1
    8. SCP config.json to client machine
    9. Wait SERVER_START_DELAY seconds
    10. SSH into client machine: start Consumer (sbt "runMain main.Consumer"),
        redirecting stdout to consumer.log
    11. SSH into client machine: start Producer (sbt "runMain main.Producer"),
        redirecting stdout to producer.log
    12. Wait for producer process to complete (consumer will also finish at same time)
    13. SCP /tmp/output.json and consumer.log back
    14. Run parse_results.py on client to write standard JSON schema to ~/
    15. SCP JSON results back via copy_results_back()
    16. SSH into seq_ips: stop Kafka (bin/kafka-server-stop.sh)
```

**Open questions:**
- KRaft controller quorum configuration for multi-broker setups (single broker is straightforward; multi-broker requires setting `controller.quorum.voters`)
- Whether `sbt "runMain main.Producer"` is the correct invocation or if a fat jar is preferred for remote execution (fat jar avoids requiring sbt on remote machines)

---

## Shared Helper Additions

Two helper functions should be added to `experiment.py`:

**`run_remote_command_sync(ip, command, ssh_key, ssh_user)`**
Like `execute_remote_command` but blocks until the command finishes. Needed for steps that must complete before continuing (e.g., topic creation, storage format).

**`kill_remote_process(ip, process_name, ssh_key, ssh_user)`**
Runs `pkill -f <process_name>` on the remote machine. Needed for systems that don't self-terminate after `experiment_duration`, unlike Pringles servers.

---

## Result Normalization

Each comparison system's client must ultimately produce a file `<json_name>_<thread_id>.json` in `~/` on the client machine with:
```json
{
  "throughput": <float>,   // operations per second
  "avg_latency": <float>,  // milliseconds
  "batch_size": <int>      // set to 1 for all comparison systems (client-side batching disabled)
}
```

If the system's native benchmark does not emit this format, a `parse_results.py` wrapper script should be placed in `experiments/comparison-systems/<system>/` and SCPed to the client machine to run after the benchmark finishes.

---

## Implementation Order

1. **Resolve the LazyLog TCP question** before writing any code — determines whether LazyLog is feasible on available hardware
2. Add `run_remote_command_sync` and `kill_remote_process` helpers to `experiment.py`
3. Add `[comparison_parameters]` parsing and dispatch logic in `main`
4. Implement `run_experiment_cycle_kafka` — TCP-native, well-documented broker
5. Implement `run_experiment_cycle_scalog` — Go binary is straightforward; main unknown is client output format
6. Implement `run_experiment_cycle_lazylog` — most complex; implement last once TCP transport is confirmed
7. Write sample TOML configs for each system in `experiments/scripts/`
8. Write `parse_results.py` wrappers as needed for each system

---

## Files to Create/Modify

| File | Change |
|------|--------|
| `experiments/scripts/experiment.py` | Add 3 new cycle functions + dispatch logic + 2 helpers + `[comparison_parameters]` parsing |
| `experiments/scripts/scalog.toml` | New TOML config for Scalog runs |
| `experiments/scripts/kafka.toml` | New TOML config for Kafka runs |
| `experiments/scripts/lazylog.toml` | New TOML config for LazyLog runs |
| `experiments/comparison-systems/scalog/` | Clone/install Scalog here; add `parse_results.py` if needed |
| `experiments/comparison-systems/lazylog/` | Clone/install LazyLog here; add `parse_results.py` if needed |
| `experiments/comparison-systems/kafka/` | Clone/install kafka-log here; add `parse_results.py` if needed |
