# Kafka Experiment Debugging Notes

## What is working

- Java and Kafka install on remote nodes (broker + client)
- Broker startup, topic creation, storage formatting
- Fat jar build (`sbt assembly`) and SCP to client node
- Config injection via classpath (`/tmp/kafka-config/config.json` shadows any bundled config)
- Consumer process stays alive for full experiment duration (`nohup` fixed SIGHUP kill from SSH session close)
- Stale consumer/producer process cleanup between runs
- Producer starts and begins sending ("starting timer" confirmed in producer log)

## The outstanding problem

The consumer output JSON (`/tmp/kafka_output_*.json`) is always empty (0 bytes).
The consumer runs for the full experiment duration (confirmed via `pgrep`) but does not write results.

## What we know about the cause

- All consumer application logging is suppressed because SLF4J is NOP — we cannot see what the consumer is doing internally
- The last diagnostic added (`ls -la` + `cat` of remote JSON right after kill + 5s wait) was never actually run — this is the first thing to do
- Config field names have been verified against the bundled example config and match
- There is no bundled `config.json` in the jar; the classpath-injected one is the only source

## Next debugging steps (in order)

### Step 1: Run and share "Remote output JSON content (before SCP)" output
This diagnostic is already in `experiment.py`. It prints `ls -la` and `cat` of
`/tmp/kafka_output_*.json` on the client node right after killing consumer/producer
and waiting 5s. This tells us:
- If file is empty on the remote → consumer is not writing (bug in consumer or no data)
- If file has content but SCP fails → path/permissions issue

### Step 2: Check broker reachability from the client node
Topic creation was verified from the broker node itself, never from the client.
Add this check before starting consumer/producer:
```python
run_remote_command_sync(client_ip, f'nc -zw5 {seq_ips[0]} 9092 && echo reachable || echo UNREACHABLE', ssh_key, ssh_user)
```
If UNREACHABLE → firewall or network config issue between 10.0.0.4 and 10.0.0.3:9092.

### Step 3: If reachable and JSON still empty after kill
The consumer may only write the JSON on natural benchmark completion (not on SIGTERM).
- Wait longer before killing (increase buffer beyond `warm_up + benchmark + cooldown + 15s`)
- Or check whether the consumer exits on its own before we kill it (`pgrep` after `total_wait`)
