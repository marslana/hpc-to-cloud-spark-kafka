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

## Impact on Final 5GB MM2 Performance

| Metric | Tuned Result |
|--------|--------------|
| Cross-region throughput | 8.54 MB/s |
| E2E time | 219 s |
| First-record latency | 35 s |

## Why This Matters

Skyplane (which SkyHOST is built on) applies BBR + large buffers to every
gateway VM it provisions. Without applying the same tuning to MM2's EC2
instances, the comparison would be unfair — MM2 would be handicapped by
default cubic congestion control and small kernel buffers, which severely
limit throughput over high-RTT transatlantic links (~85ms RTT).
