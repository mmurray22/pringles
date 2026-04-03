## Comparison Systems

This folder holds notes and setup instructions for the systems benchmarked against Pringles.

Comparison system source code is **not** cloned here. Each system must be cloned and built directly on the CloudLab nodes before running experiments. The binary paths on the remote machines are configured in the corresponding TOML files in `experiments/scripts/`.

### Systems

**Kafka** (`experiments/scripts/kafka.toml`)
Clone and build on each CloudLab node:
```bash
# Kafka broker (Apache Kafka distribution)
wget https://downloads.apache.org/kafka/<version>/kafka_2.13-<version>.tgz
tar -xzf kafka_2.13-<version>.tgz -C /opt/kafka --strip-components=1

# kafka-log client (mmurray22/kafka-log)
git clone https://github.com/mmurray22/kafka-log /proj/ove-PG0/murray/kafka-log
cd /proj/ove-PG0/murray/kafka-log && sbt compile
```

**Scalog** (`experiments/scripts/scalog.toml`)
Clone and build on each CloudLab node:
```bash
git clone https://github.com/chn0318/scalog /proj/ove-PG0/murray/scalog
cd /proj/ove-PG0/murray/scalog && go build -mod=vendor .
# TODO: confirm binary layout under cmd/ and update path_* fields in scalog.toml
```

**LazyLog** — blocked pending RDMA-capable CloudLab nodes (Mellanox ConnectX-4 required).
