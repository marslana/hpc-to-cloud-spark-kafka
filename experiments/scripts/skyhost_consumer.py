#!/usr/bin/env python3
"""Consume from US-East Kafka and measure E2E throughput for SkyHOST transfer."""

import argparse
import time
from confluent_kafka import Consumer


def _hdr(headers, key):
    if not headers:
        return None
    for name, val in headers:
        if name == key:
            return None if val is None else (val.decode() if isinstance(val, (bytes, bytearray)) else str(val))
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="127.0.0.1:9092")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--target-records", type=int, default=1865767)
    ap.add_argument("--timeout-ms", type=int, default=120000)
    ap.add_argument("--save", default=None)
    args = ap.parse_args()

    c = Consumer({
        "bootstrap.servers": args.brokers,
        "group.id": f"skyhost-e2e-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "fetch.min.bytes": 1048576,
        "fetch.max.bytes": 67108864,
        "max.partition.fetch.bytes": 67108864,
    })
    c.subscribe([args.topic])

    count = 0
    bytes_sum = 0
    first_produce_ms = None
    first_consume_wall = None
    last_msg_wall = None
    last_idle_ms = 0
    wall_start = time.time()
    report_every = 2000

    try:
        while True:
            m = c.poll(0.5)
            if m is None or m.error():
                if count > 0:
                    last_idle_ms += 500
                    if last_idle_ms > args.timeout_ms:
                        break
                continue

            last_idle_ms = 0
            now = time.time()
            if first_consume_wall is None:
                first_consume_wall = now
            last_msg_wall = now

            v = m.value()
            if v:
                bytes_sum += len(v)
            count += 1

            tp = _hdr(m.headers(), "ts_produce")
            if tp is not None:
                try:
                    tp_ms = int(tp)
                    if first_produce_ms is None or tp_ms < first_produce_ms:
                        first_produce_ms = tp_ms
                except Exception:
                    pass

            if count % report_every == 0:
                elapsed = now - first_consume_wall
                print(f"  consumed {count} msgs ({bytes_sum/1024/1024:.1f} MB, {elapsed:.1f}s)")

            if count >= args.target_records:
                break

    finally:
        c.close()

    active_duration = last_msg_wall - first_consume_wall if (first_consume_wall and last_msg_wall) else 0
    mb = bytes_sum / 1024 / 1024

    e2e_sec = None
    if first_produce_ms and last_msg_wall:
        e2e_sec = last_msg_wall - (first_produce_ms / 1000.0)

    print(f"\n=== SKYHOST CONSUMER RESULTS ===")
    print(f"kafka_messages: {count}")
    print(f"bytes_mb: {mb:.2f}")
    print(f"active_duration_s: {active_duration:.2f}")
    if e2e_sec:
        print(f"e2e_s: {e2e_sec:.2f}")
        print(f"e2e_throughput_mbps: {mb / e2e_sec:.2f}")
    print(f"active_throughput_mbps: {mb / active_duration:.2f}" if active_duration > 0 else "active_throughput_mbps: N/A")

    if args.save:
        import csv, os
        os.makedirs(os.path.dirname(args.save) or ".", exist_ok=True)
        exists = os.path.exists(args.save)
        with open(args.save, "a") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["kafka_messages", "bytes_mb", "active_duration_s",
                            "e2e_s", "e2e_throughput_mbps", "active_throughput_mbps"])
            w.writerow([count, f"{mb:.2f}", f"{active_duration:.2f}",
                        f"{e2e_sec:.2f}" if e2e_sec else "",
                        f"{mb/e2e_sec:.2f}" if e2e_sec else "",
                        f"{mb/active_duration:.2f}" if active_duration > 0 else ""])


if __name__ == "__main__":
    main()
