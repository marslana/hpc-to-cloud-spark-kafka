# HPC-to-Cloud Pipeline Experiment Details

## Overview
All experiments evaluate five HPC-to-cloud data delivery approaches using the EEA air quality dataset at two scales (1GB and 5GB). Each approach is run 3 times. Record counts are verified via `kafka-get-offsets` on the destination broker.

## Infrastructure

### HPC (Source)
- **Cluster**: Aion, University of Luxembourg HPC
- **Nodes**: AMD EPYC, 128 cores, 256 GB DDR4 per node
- **Scheduler**: SLURM (dedicated node allocation)
- **Containerization**: Singularity/Apptainer 1.4.1
- **Container image**: Custom `sparkhdfs.sif` with Apache Spark 3.4, Kafka 2.8, ZooKeeper 3.7
- **Spark config**: 2 workers x 4 cores x 6 GB executor memory, 2 GB driver memory

### Cloud (Destination)
- **US-East-1 (Virginia)**: AWS EC2 m5.8xlarge (32 vCPUs, 128 GB RAM, 10 Gbps)
  - Kafka 3.7.0 in Docker (KRaft mode, single broker)
  - IP: 100.26.205.125
- **EU-Central-1 (Frankfurt)**: AWS EC2 m5.8xlarge (used for Approaches 4 & 5)
  - Kafka 3.7.0 in Docker (KRaft mode, single broker)
  - IP: 63.176.60.55

### Network
- HPC → Frankfurt: ~10 ms RTT (Luxembourg to Frankfurt, ~200 km)
- Frankfurt → US-East: ~85 ms RTT (transatlantic)
- HPC → US-East: ~90 ms RTT (transatlantic)

### TCP Tuning (Applied to Both EC2 Instances)
To ensure fair comparison with SkyHOST (which applies these via Skyplane gateway setup), both EC2 instances had identical TCP tuning:
```
net.core.rmem_max = 134217728        # 128 MB
net.core.wmem_max = 134217728        # 128 MB
net.ipv4.tcp_rmem = 4096 87380 67108864   # 64 MB max
net.ipv4.tcp_wmem = 4096 65536 67108864   # 64 MB max
net.core.somaxconn = 65535
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
```

## Dataset

| Property | 1 GB | 5 GB |
|----------|------|------|
| File | `eea_airquality_1gb.csv` | `eea_airquality_5gb.csv` |
| Raw size | 1,058 MB | 5,292 MB |
| Raw records | 16,777,216 | 83,886,080 |
| Filter | Countries: LU, DE, BE | Countries: LU, DE, BE |
| Filter ratio | 11.12% | 11.11% |
| Filtered records | 1,865,767 | 9,317,527 |
| Filtered Kafka JSON size | ~117.7 MB (~315 MB on broker) | ~588 MB (~1,572 MB on broker) |

## Kafka Configuration (Identical Across All Approaches)
- Partitions: 8
- Replication factor: 1
- acks: 1
- Batch size: 16 KB (Spark producer)
- Linger: 10 ms
- Buffer memory: 64 MB

## MM2 Configuration (Approach 4)
- Tasks: 8
- Batch size: 1 MB
- Linger: 100 ms
- Max in-flight: 10
- acks: 1
- Buffer memory: 128 MB
- Consumer fetch.min.bytes: 1 MB
- Consumer max.partition.fetch.bytes: 10 MB

## SkyHOST Configuration (Approach 5)
- Readers: 8
- Writers: 8
- Chunk size: 32 MB
- Parallel TCP connections: 8
- BBR congestion control (automatic via Skyplane)
- Gateway instances: m5.8xlarge (provisioned by Skyplane)

## Consumer Measurement
A Python consumer (`first_record_consumer.py`) using `confluent-kafka` runs on the US-East EC2 instance. It measures:
- **First record latency (FRL)**: Time from consumer start to first record received
- **Active duration**: Time from first record to last record
- **Throughput**: bytes_received / active_duration
- **Idle timeout**: 120 seconds (consumer exits after no new records for 120s)

---

## 1 GB Results

### Summary Table (Average of 3 Runs)

| # | Approach | Type | E2E (s) | Cross-Region Throughput |
|---|----------|------|---------|------------------------|
| 1 | Cloud-Side Processing | Batch | 75 | N/A (scp 10s, Spark on cloud) |
| 2 | Direct Producer | Stream | 191 | 0.65 MB/s |
| 3 | HPC-Side Processing | Batch | 176* | scp 58s + ingest 17s |
| 4 | MM2 Replication (tuned) | Stream | 178 | 8.70 MB/s |
| 5 | SkyHOST Transfer | Stream | 53 | 9.40 MB/s |

*Note: Approach 3 Run 1 had inflated E2E (209s) due to SSH overhead; Runs 2-3 averaged 159s.

### 1 GB MM2 (with TCP Tuning) — Detailed

| Run | HPC Read | HPC Filter | HPC Produce | HPC Total | FRL | Consumer Active | Consumer TP | E2E |
|-----|----------|------------|-------------|-----------|-----|-----------------|-------------|-----|
| 1 | 7.29s | 3.45s | 8.94s | 19.69s | 19.88s | 36.28s | 8.68 MB/s | 178s |
| 2 | 7.47s | 3.29s | 9.20s | 19.98s | 19.86s | 36.30s | 8.67 MB/s | 178s |
| 3 | 7.63s | 3.16s | 8.85s | 19.66s | 19.93s | 35.99s | 8.74 MB/s | 178s |
| **Avg** | **7.46** | **3.30** | **9.00** | **19.78** | **19.89** | **36.19** | **8.70** | **178** |

---

## 5 GB Results

### Summary Table (Average of 3 Runs)

| # | Approach | Type | E2E (s) | HPC Total (s) | Cross-Region TP | FRL (s) |
|---|----------|------|---------|----------------|-----------------|---------|
| 1 | Cloud-Side Processing | Batch | 429 | N/A (cloud Spark: 59s) | scp: 300s | 339 |
| 2 | Direct Producer | Stream | 670 | 670 | 0.91 MB/s | N/A |
| 3 | HPC-Side Processing | Batch | 245 | 50s (Spark) | scp: 41s + ingest: 82s | 103 |
| 4 | MM2 Replication (tuned) | Stream | 340 | 66s | 8.54 MB/s | 35 |
| 5 | SkyHOST Transfer | Stream | 254 | 68s | 10.94 MB/s | 42* |

*SkyHOST FRL avg excludes Run 1 (57s cold-start); Runs 2-3 avg = 34.4s

### 5 GB Approach 1 — Cloud-Side Processing

| Run | scp (s) | Spark Read | Spark Filter | Spark Produce | Spark Total | FRL (s) | E2E (s) |
|-----|---------|------------|--------------|---------------|-------------|---------|---------|
| 1 | 299 | 12.33 | 18.66 | 29.84 | 60.86 | 340.82 | 432 |
| 2 | 299 | 11.43 | 18.21 | 29.91 | 59.58 | 338.17 | 429 |
| 3 | 301 | 11.44 | 17.52 | 27.66 | 56.64 | 338.77 | 427 |
| **Avg** | **300** | **11.73** | **18.13** | **29.14** | **59.03** | **339.25** | **429** |

### 5 GB Approach 2 — Direct Producer

| Run | Read (s) | Filter (s) | Produce (s) | Total (s) | Throughput |
|-----|----------|------------|-------------|-----------|------------|
| 1 | 11.82 | 16.03 | 642.55 | 670.43 | 0.91 MB/s |
| 2 | 11.13 | 15.64 | 642.89 | 669.68 | 0.91 MB/s |
| 3 | 11.34 | 16.23 | 642.20 | 669.79 | 0.92 MB/s |
| **Avg** | **11.43** | **15.97** | **642.55** | **669.97** | **0.91** |

### 5 GB Approach 3 — HPC-Side Processing

| Run | Spark Read | Spark Filter | Spark Write | Spark Total | scp (s) | Ingest (s) | Ingest TP | FRL (s) | E2E (s) |
|-----|------------|--------------|-------------|-------------|---------|------------|-----------|---------|---------|
| 1 | 10.51 | 15.26 | 23.64 | 49.41 | 41 | 81.84 | 7.15 MB/s | 102.88 | 246 |
| 2 | 11.38 | 15.25 | 23.45 | 50.08 | 41 | 81.42 | 7.19 MB/s | 102.61 | 245 |
| 3 | 10.54 | 15.76 | 23.60 | 49.90 | 41 | 81.56 | 7.17 MB/s | 102.59 | 245 |
| **Avg** | **10.81** | **15.42** | **23.56** | **49.80** | **41** | **81.61** | **7.17** | **102.69** | **245** |

### 5 GB Approach 4 — MM2 Replication (with TCP Tuning)

| Run | HPC Read | HPC Filter | HPC Produce | HPC Total | HPC TP | FRL (s) | Consumer Active | Consumer TP | E2E (s) |
|-----|----------|------------|-------------|-----------|--------|---------|-----------------|-------------|---------|
| 1 | 11.46 | 14.08 | 40.29 | 65.85 | 14.59 | 34.76 | 183.57 | 8.56 MB/s | 339 |
| 2 | 11.47 | 13.87 | 40.77 | 66.13 | 14.42 | 34.51 | 184.77 | 8.51 MB/s | 339 |
| 3 | 11.26 | 15.39 | 40.20 | 66.87 | 14.62 | 35.83 | 183.62 | 8.56 MB/s | 341 |
| **Avg** | **11.40** | **14.45** | **40.42** | **66.28** | **14.54** | **35.03** | **183.99** | **8.54** | **340** |

### 5 GB Approach 5 — SkyHOST Transfer

| Run | HPC Read | HPC Filter | HPC Produce | HPC Total | HPC TP | FRL (s) | Consumer Active | Consumer TP | E2E (s) |
|-----|----------|------------|-------------|-----------|--------|---------|-----------------|-------------|---------|
| 1 | 10.69 | 15.39 | 42.01 | 68.11 | 13.99 | 57.39 | 137.61 | 11.42 MB/s | 263 |
| 2 | 11.33 | 14.93 | 41.68 | 67.96 | 14.10 | 34.72 | 147.57 | 10.65 MB/s | 250 |
| 3 | 11.17 | 15.58 | 41.45 | 68.22 | 14.18 | 34.02 | 146.26 | 10.75 MB/s | 248 |
| **Avg** | **11.06** | **15.30** | **41.71** | **68.10** | **14.09** | **42.04** | **143.81** | **10.94** | **254** |

---

## Key Observations

### 1 GB → 5 GB Scalability
- **MM2 throughput is consistent** across dataset sizes: 8.70 MB/s (1GB) vs 8.54 MB/s (5GB). The previous poor 1GB MM2 result (1.08 MB/s) was due to running without TCP tuning — with BBR and tuned buffers, MM2 performs well.
- **Direct Producer** remains bottlenecked: 0.65 MB/s (1GB) → 0.91 MB/s (5GB). The per-partition acknowledgment overhead dominates.
- **SkyHOST** scales well: ~9.40 MB/s (1GB) → 10.94 MB/s (5GB), benefiting from better amortization of chunk overhead.
- **scp** throughput increases for larger files: ~18 MB/s (1GB) → ~14.4 MB/s (5GB raw) / ~128 MB/s (5GB scp: 41s for 5.3GB).

### TCP Tuning Impact on MM2
The original 1GB MM2 results (without tuning) showed 1.08 MB/s. After applying BBR congestion control and 128MB socket buffers:
- 1GB: **8.70 MB/s** (8.1x improvement)
- 5GB: **8.54 MB/s** (consistent)

This confirms the original bottleneck was not Kafka's protocol overhead alone, but the default TCP cubic algorithm combined with small kernel buffers severely limiting throughput over the transatlantic WAN link (~85ms RTT).

### Fair Comparison Note
SkyHOST (via Skyplane) automatically applies BBR + large TCP buffers to its gateway VMs (`skyplane/compute/const_cmds.py`). For a fair comparison, we applied identical TCP tuning to both Frankfurt and US-East EC2 instances used by MM2. All MM2 results in this document use the tuned configuration.

---

## File Inventory

### Raw Result CSVs
- `1gb/approach1_cloud_side_processing.csv`
- `1gb/approach2_direct_producer.csv`
- `1gb/approach3_hpc_side_processing.csv`
- `1gb/approach4_mm2_replication_tuned.csv`
- `1gb/approach5_skyhost_transfer.csv`
- `5gb/approach1_cloud_side_processing.csv`
- `5gb/approach2_direct_producer.csv`
- `5gb/approach3_hpc_side_processing.csv`
- `5gb/approach4_mm2_replication_tuned.csv`
- `5gb/approach5_skyhost_transfer.csv`

### On HPC (~/5gb_results/ and ~/1gb_results/)
Full Spark logs, consumer logs, and raw outputs for each run.

### On EC2 US-East (~/results/)
Consumer CSV and per-run consumer logs for approaches 4 and 5.
