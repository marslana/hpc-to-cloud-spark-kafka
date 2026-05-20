#!/usr/bin/env python3
"""
Compute inter-record latency percentiles from a *_timestamps.csv file produced
by first_record_consumer.py --timestamps-csv.

Usage:
  python3 analyze_tail_latency.py <approach_label> <ts_csv> [<ts_csv> ...]

Output: a single line per file with P50 / P95 / P99 inter-record latency in ms,
plus a final summary line per approach if multiple runs are passed.
"""

import csv
import sys
import statistics


def percentile(sorted_values, p):
    if not sorted_values:
        return float("nan")
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


def load_rows(path):
    rows = []
    with open(path) as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append((int(row["record_index"]), float(row["wall_clock_s"])))
    rows.sort()
    return rows


def per_record_deltas_ms(rows):
    """Inter-sample gap normalized to per-record (ms)."""
    out = []
    for (i0, t0), (i1, t1) in zip(rows, rows[1:]):
        d_records = max(1, i1 - i0)
        out.append((t1 - t0) * 1000.0 / d_records)
    return out


def per_batch_deltas_ms(rows):
    """Raw inter-sample gap (ms) -- i.e. time to receive the next 1000-record batch."""
    return [(t1 - t0) * 1000.0 for (_, t0), (_, t1) in zip(rows, rows[1:])]


def summarize(label, all_deltas, units):
    s = sorted(all_deltas)
    print(f"  {label:<20s} n={len(all_deltas):6d}  "
          f"P50={percentile(s,50):8.3f}{units}  "
          f"P90={percentile(s,90):8.3f}{units}  "
          f"P95={percentile(s,95):8.3f}{units}  "
          f"P99={percentile(s,99):8.3f}{units}  "
          f"mean={statistics.mean(all_deltas):.3f}{units}")


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    label = sys.argv[1]
    paths = sys.argv[2:]
    per_record_all = []
    per_batch_all = []
    print(f"Approach: {label}")
    for p in paths:
        rows = load_rows(p)
        if len(rows) < 2:
            print(f"  {p}: insufficient samples")
            continue
        prd = per_record_deltas_ms(rows)
        pbd = per_batch_deltas_ms(rows)
        s_prd = sorted(prd)
        s_pbd = sorted(pbd)
        print(f"  {p}")
        print(f"    per-record (ms): n={len(prd):6d}  "
              f"P50={percentile(s_prd,50):7.3f}  "
              f"P90={percentile(s_prd,90):7.3f}  "
              f"P95={percentile(s_prd,95):7.3f}  "
              f"P99={percentile(s_prd,99):7.3f}")
        print(f"    per-1k-batch (ms): n={len(pbd):6d}  "
              f"P50={percentile(s_pbd,50):7.3f}  "
              f"P90={percentile(s_pbd,90):7.3f}  "
              f"P95={percentile(s_pbd,95):7.3f}  "
              f"P99={percentile(s_pbd,99):7.3f}")
        per_record_all.extend(prd)
        per_batch_all.extend(pbd)
    if per_record_all:
        print(f"  --- ALL runs combined ({len(paths)} file(s)) ---")
        summarize("per-record", per_record_all, "ms")
        summarize("per-1k-batch", per_batch_all, "ms")


if __name__ == "__main__":
    main()
