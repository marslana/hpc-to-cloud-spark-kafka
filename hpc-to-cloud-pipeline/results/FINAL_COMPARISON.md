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

| # | Approach | Type | E2E (s) | FRL (s) |
|---|---|---|---:|---:|
| 1 | Cloud-Side Processing | Batch | 368 | 339 |
| 2 | Direct Producer | Stream | 670 | 36 |
| 3 | HPC-Side Processing | Batch | 183 | 103 |
| 4 | MM2 Replication | Stream | 219 | 35 |
| 5 | SkyHOST | Stream | 186 | 35 |

## Interpretation

HPC-Side Processing is the fastest overall at 183 s, but it is file-based and the first destination record appears only after 103 s. SkyHOST completes in 186 s, within 2% of HPC-Side Processing, and exposes the first record in 35 s.

Direct Producer is slow because the Spark Kafka producer pays the HPC-to-US-East acknowledgment path over the WAN for every batch. MM2 improves over Direct Producer by routing through a nearby regional broker, but the cross-region Kafka replication stage remains bounded by Kafka protocol overhead and WAN latency. SkyHOST replaces the cross-region Kafka protocol stage with transport-layer chunking, reaching a 3-run average consumer throughput of 10.43 MB/s.

## Per-Approach Raw Sources

| Approach | Raw CSV |
|---|---|
| Cloud-Side Processing | `5gb/approach1_cloud_side_processing.csv` |
| Direct Producer | `5gb/approach2_direct_producer.csv` |
| HPC-Side Processing | `5gb/approach3_hpc_side_processing.csv` |
| MM2 Replication | `5gb/approach4_mm2_replication_tuned.csv` |
| SkyHOST | `5gb/approach5_skyhost_transfer.csv` |

## Notes

- The SkyHOST throughput value in the paper uses the same 3-run averaging policy as the E2E and FRL values, giving 10.43 MB/s and reported as 10.4 MB/s.
