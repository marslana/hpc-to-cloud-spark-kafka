# TCP Tuning Configuration

Applied to both EC2 instances (Frankfurt and US-East) before MM2 experiments.
This matches the tuning automatically applied by Skyplane/SkyHOST gateways
(source: `skyplane/compute/const_cmds.py:make_sysctl_tcp_tuning_command()`).

## Commands Applied

```bash
sudo sysctl -w net.core.rmem_max=134217728
sudo sysctl -w net.core.wmem_max=134217728
sudo sysctl -w net.ipv4.tcp_rmem="4096 87380 67108864"
sudo sysctl -w net.ipv4.tcp_wmem="4096 65536 67108864"
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.core.default_qdisc=fq
sudo sysctl -w net.ipv4.tcp_congestion_control=bbr
```

## Impact on MM2 Performance (1GB dataset)

| Metric | Without Tuning | With Tuning | Improvement |
|--------|---------------|-------------|-------------|
| Cross-region throughput | ~1.08 MB/s | 8.70 MB/s | 8.1x |
| E2E time | ~309 s | 178 s | 1.7x faster |

## Why This Matters

Skyplane (which SkyHOST is built on) applies BBR + large buffers to every
gateway VM it provisions. Without applying the same tuning to MM2's EC2
instances, the comparison would be unfair — MM2 would be handicapped by
default cubic congestion control and small kernel buffers, which severely
limit throughput over high-RTT transatlantic links (~85ms RTT).
