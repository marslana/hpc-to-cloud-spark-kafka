# Final 5 GB Pipeline Comparison

This file summarizes the final 5 GB results used in the paper.

## Common Setup

- Dataset: 5 GB synthetic EEA-style air-quality CSV
- Raw records: 83,886,080
- Filter: countries in `{LU, DE, BE}`
- Filtered records delivered: 9,317,527
- Kafka partitions: 8
- Producer acknowledgments: `acks=1`
- Spark Kafka producer batch: 16 KB
- Spark allocation: 2 workers, 4 cores per worker, 6 GB executor memory
- EC2 instances: `m5.8xlarge` in `eu-central-1` and `us-east-1`
- Runs: 3 per approach

## Summary

3-run mean values (with standard deviation in parentheses).

| # | Approach | Type | E2E (s) | FRL (s) | TP (MB/s) |
|---|---|---|---:|---:|---:|
| 1 | Cloud-Side Processing | Batch | 368 | 339 | — |
| 2 | Direct Producer | Stream | 676 (0.6) | 35.8 (0.4) | 2.45 |
| 3 | HPC-Side Processing | Batch | 187 (1.2) | 103.2 (1.2) | 6.90 |
| 4 | MM2 Replication | Stream | 234 (4.6) | 35.6 (1.4) | 7.94 |
| 5 | SkyHOST | Stream | 210 (6.8) | 36.2 (0.6) | 9.04 |

> Note: E2E used in the paper for Direct Producer is the orchestration-script
> wall-clock (`e2e_s`, mean 687 s) which adds the HPC-side setup time; the
> consumer-measured E2E (`consumer_total_s`, mean 676 s) is reported in the
> per-run CSV above.

## Tail Latency (inter-batch, ms)

Inter-batch latency at the destination consumer, computed from per-record
arrival timestamps sampled every 1,000 records over three runs per approach
(27,948 samples each).

| Approach | P50 | P90 | P95 | P99 |
|---|---:|---:|---:|---:|
| Direct Producer | 84.57 |  95.05 | 108.79 | 127.59 |
| MM2 Replication |  1.52 |  64.97 | 131.92 | 314.43 |
| SkyHOST         |  1.78 |  83.53 |  89.04 | 131.63 |

Reproduce with:
```
python3 scripts/analyze_tail_latency.py results/raw_logs/<approach>_run*_timestamps.csv
```

## Interpretation

HPC-Side Processing is the fastest overall at 187 s, but it is file-based and
the first destination record appears only after 103 s. SkyHOST completes in
210 s, within 13% of HPC-Side Processing, and exposes the first record in
~36 s.

Direct Producer is slow because the Spark Kafka producer pays the HPC-to-US-East
acknowledgment path over the WAN for every batch. MM2 improves over Direct
Producer by routing through a nearby regional broker, but the cross-region
Kafka replication stage remains bounded by Kafka protocol overhead and WAN
latency, and its P99 inter-batch latency reaches 314 ms. SkyHOST replaces the
cross-region Kafka protocol stage with transport-layer chunking (8 parallel
TCP, 32 MB transport batches), reaching a 3-run average consumer throughput
of 9.04 MB/s and a 2.4x lower P99 inter-batch latency than MM2.

## Per-Approach Raw Sources

| Approach | Raw CSV |
|---|---|
| Cloud-Side Processing | `approach1_cloud_side_processing.csv` |
| Direct Producer | `approach2_direct_producer.csv` |
| HPC-Side Processing | `approach3_hpc_side_processing.csv` |
| MM2 Replication | `approach4_mm2_replication_tuned.csv` |
| SkyHOST | `approach5_skyhost_transfer.csv` |

Per-record timestamp samples used for tail-latency analysis are in
`raw_logs/`.
