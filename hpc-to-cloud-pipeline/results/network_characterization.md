# Network Characterization (May 2026)

Measured during the paper's tail-latency campaign on `m5.8xlarge` EC2 instances
launched specifically for this purpose. Aion-side measurements were taken from a
SLURM-allocated compute node (`aion-0009`) via the same egress path used by all
pipeline runs. ICMP echo is blocked outbound from Aion compute nodes by the
University of Luxembourg firewall, so RTT for those two paths is reported as the
average of 30 TCP-handshake samples to port 22 on the destination EC2; the
intra-AWS path was measured with standard `ping`.

## Endpoints

| Region | Instance type | Role |
|---|---|---|
| eu-central-1 (Frankfurt) | m5.8xlarge | MM2 / SkyHOST source-side broker |
| us-east-1 (Virginia)     | m5.8xlarge | Destination broker (sink) |

(Public IPv4 addresses and instance IDs intentionally omitted; IPs are
short-lived and instance IDs are account-specific. Reproduce by launching
two fresh `m5.8xlarge` instances in these regions and pointing
`hpc-to-cloud-pipeline/configs/experiment_5gb.conf` at their public IPs.)

## Measurements

| Path | RTT (ms) | iperf3 single-stream (Mbps) | Notes |
|---|---|---|---|
| Aion -> eu-central-1     | 5.2  | 939  | TCP-handshake RTT (n=30); iperf3 -t 30 -P 1 |
| Aion -> us-east-1        | 86.3 | 318  | TCP-handshake RTT (n=30); iperf3 -t 30 -P 1 |
| eu-central-1 -> us-east-1 | 90.5 | 4490 | ping -c 50 ICMP; iperf3 -t 30 -P 1 |

## Raw outputs

### Aion -> eu-central-1
```
TCP-handshake RTT (n=30): min=4.9  avg=5.2  max=5.8 ms
iperf3: [SUM] 0.00-30.01 sec  3.28 GBytes  939 Mbits/sec  receiver
```

### Aion -> us-east-1
```
TCP-handshake RTT (n=30): min=85.2  avg=86.3  max=88.0 ms
iperf3: [SUM] 0.00-30.09 sec  1.11 GBytes  318 Mbits/sec  receiver
```

### eu-central-1 -> us-east-1
```
ping (50 packets): min/avg/max/mdev = 90.517/90.537/90.599/0.014 ms
iperf3: [SUM] 0.00-30.09 sec  15.7 GBytes  4490 Mbits/sec  receiver
```

## Interpretation

The Aion -> us-east-1 and eu-central-1 -> us-east-1 paths exhibit
near-identical RTT (~90 ms across the Atlantic), but the intra-AWS path
delivers ~14x higher single-stream bandwidth (4490 vs 318 Mbps). Combined
with the ~17x lower RTT on the Aion -> eu-central-1 first hop, this
explains why the two-hop architectures (MirrorMaker 2 and SkyHOST) achieve
both lower FRL and competitive end-to-end time relative to the single-hop
Direct Producer baseline.
