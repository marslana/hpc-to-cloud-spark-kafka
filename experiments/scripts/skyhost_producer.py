#!/usr/bin/env python3
"""Produce filtered EEA records to local Kafka (on Frankfurt).
Generates the same format and count as HPC Spark filter output."""

import argparse
import time
import random
import string
from confluent_kafka import Producer

COUNTRIES = ["LU", "DE", "BE"]
POLLUTANTS = ["PM10", "PM2.5", "NO2", "O3", "SO2", "CO"]
STATIONS = {
    "LU": ["LU0101A", "LU0102A", "LU0103A", "LU0104A", "LU0105A"],
    "DE": [f"DE{i:04d}A" for i in range(1, 51)],
    "BE": [f"BE{i:04d}A" for i in range(1, 21)],
}

def make_record(seq):
    country = random.choice(COUNTRIES)
    station = random.choice(STATIONS[country])
    pollutant = random.choice(POLLUTANTS)
    value = round(random.uniform(1.0, 200.0), 2)
    ts = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00"
    unit = "µg/m³"
    validity = random.choice([1, 2, 3])
    verification = random.choice([1, 2, 3])
    return f"{country},{station},{pollutant},{ts},{value},{unit},{validity},{verification}"


def delivery_cb(err, msg):
    if err:
        print(f"Delivery failed: {err}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="127.0.0.1:9092")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--records", type=int, default=1865767)
    ap.add_argument("--batch-size", type=int, default=16384)
    ap.add_argument("--acks", default="1")
    args = ap.parse_args()

    conf = {
        "bootstrap.servers": args.brokers,
        "acks": args.acks,
        "batch.size": args.batch_size,
        "linger.ms": 10,
        "queue.buffering.max.messages": 2000000,
        "queue.buffering.max.kbytes": 1048576,
    }
    p = Producer(conf)
    target = args.records
    report_every = 200000

    t0 = time.time()
    for i in range(target):
        rec = make_record(i)
        ts_ms = str(int(time.time() * 1000))
        p.produce(
            args.topic,
            value=rec.encode(),
            headers=[("ts_produce", ts_ms.encode()), ("ds", b"eea_filtered")],
            callback=delivery_cb,
        )
        if i % 10000 == 0:
            p.poll(0)
        if (i + 1) % report_every == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            print(f"  {i+1}/{target} records ({rate:.0f} rec/s)")

    p.flush(timeout=120)
    t1 = time.time()

    total_bytes = target * 70  # avg ~70 bytes per record
    elapsed = t1 - t0
    mbps = (total_bytes / 1024 / 1024) / elapsed
    print(f"\n=== PRODUCE RESULTS ===")
    print(f"records: {target}")
    print(f"estimated_mb: {total_bytes/1024/1024:.1f}")
    print(f"duration_s: {elapsed:.2f}")
    print(f"throughput_mbps: {mbps:.2f}")
    print(f"records_per_sec: {target/elapsed:.0f}")


if __name__ == "__main__":
    main()
