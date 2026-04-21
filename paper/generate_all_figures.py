#!/usr/bin/env python3
"""
Generate all figures for the Kafka-Spark HPC paper from raw CSV data.

Usage:
    python3 generate_all_figures.py [--data-dir PATH] [--output-dir PATH]

By default, the script auto-detects the data directory relative to its own
location (works from both paper/ and paper/artifacts/figures/).
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import csv
import os
import argparse

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

NATIVE_CSV_RELPATH = os.path.join(
    'native_kafka', 'kafka_perf_results_20250317_175725.csv')
SPARK_RUN_DIRS = [
    'spark_kafka/spark_kafka_test_20250327_1307',
    'spark_kafka/spark_kafka_test_20250327_1325',
    'spark_kafka/spark_kafka_test_20250327_1340',
]
SPARK_CSV_NAME = 'spark_producer_results.csv'

PARTITIONS = [1, 2, 4, 8]
NATIVE_MSG_SIZES = [100, 1024, 10240]
SPARK_RECORD_SIZES = [100, 1000, 10000]
MSG_LABELS = ['100 B', '1 KB', '10 KB']

BLUE = '#2166ac'
GREEN = '#1b7837'
RED = '#b2182b'
ORANGE = '#e08214'
COLORS_4 = ['#2166ac', '#4dac26', '#fee08b', '#d73027']


def _find_data_dir():
    """Auto-detect data directory relative to script location."""
    candidates = [
        os.path.join(SCRIPT_DIR, '..', 'data'),
        os.path.join(SCRIPT_DIR, 'artifacts', 'data'),
    ]
    for c in candidates:
        probe = os.path.join(c, NATIVE_CSV_RELPATH)
        if os.path.isfile(probe):
            return os.path.realpath(c)
    raise FileNotFoundError(
        'Cannot find data directory. Use --data-dir to specify.')


def _setup_style():
    plt.rcParams.update({
        'font.size': 10,
        'font.family': 'serif',
        'font.serif': ['Times New Roman', 'DejaVu Serif', 'serif'],
        'mathtext.fontset': 'dejavuserif',
        'figure.figsize': (7, 3.5),
        'axes.grid': True,
        'grid.alpha': 0.25,
        'grid.linestyle': '--',
        'legend.fontsize': 8.5,
        'legend.framealpha': 0.9,
        'axes.labelsize': 11,
        'axes.titlesize': 11,
        'xtick.labelsize': 9,
        'ytick.labelsize': 9,
        'figure.dpi': 300,
        'savefig.bbox': 'tight',
        'savefig.pad_inches': 0.05,
    })


def _save(fig, name, output_dir):
    for ext in ('pdf', 'png'):
        fig.savefig(os.path.join(output_dir, f'{name}.{ext}'), dpi=300)
    plt.close(fig)
    print(f'  -> {name}.pdf / .png')


# ── Data loading ─────────────────────────────────────────────

def load_native_csv(csv_path):
    with open(csv_path, newline='') as f:
        return list(csv.DictReader(f))


def extract_producer(rows, acks, msg_sizes=None):
    """Producer MB/s at 1M throughput target, keyed by (msg_size, partition)."""
    if msg_sizes is None:
        msg_sizes = NATIVE_MSG_SIZES
    result = {}
    for sz in msg_sizes:
        vals = []
        for p in PARTITIONS:
            hits = [r for r in rows
                    if r['test_type'] == 'producer'
                    and int(r['partitions']) == p
                    and int(r['message_size_bytes']) == sz
                    and r['producer_throughput'].strip() == '1000000'
                    and r['producer_acks'].strip() == acks]
            vals.append(float(hits[0]['mb_per_sec']) if hits else 0.0)
        result[sz] = vals
    return result


def extract_consumer_1group(rows, msg_sizes=None):
    """Consumer MB/s averaged over replicate runs (1-group only)."""
    if msg_sizes is None:
        msg_sizes = NATIVE_MSG_SIZES
    result = {}
    for sz in msg_sizes:
        vals = []
        for p in PARTITIONS:
            hits = [r for r in rows
                    if r['test_type'] == 'consumer'
                    and int(r['partitions']) == p
                    and int(r['message_size_bytes']) == sz
                    and r['consumer_groups'].strip() == '1']
            vals.append(np.mean([float(r['mb_per_sec']) for r in hits])
                        if hits else 0.0)
        result[sz] = vals
    return result


def load_spark_runs(data_dir, batch_filter='16384b'):
    """Load 3 Spark CSVs, return dict  (partitions, record_size) -> [v1,v2,v3]."""
    combined = {}
    for run_dir in SPARK_RUN_DIRS:
        path = os.path.join(data_dir, run_dir, SPARK_CSV_NAME)
        with open(path, newline='') as f:
            for row in csv.DictReader(f):
                if row['batch_size'].strip() != batch_filter:
                    continue
                key = (int(row['partitions']), int(row['record_size_bytes']))
                combined.setdefault(key, []).append(float(row['mb_per_sec']))
    return combined


# ── Figure functions ─────────────────────────────────────────

def fig2_native_kafka_producer(native_rows, output_dir):
    prod = extract_producer(native_rows, '1')
    cons = extract_consumer_1group(native_rows)

    fig, axes = plt.subplots(1, 3, figsize=(11, 3.5), sharey=True)
    for i, (sz, label) in enumerate(zip(NATIVE_MSG_SIZES, MSG_LABELS)):
        ax = axes[i]
        ax.plot(PARTITIONS, prod[sz], '-o', color=BLUE, linewidth=1.8,
                markersize=6, label='Producer', zorder=5)
        ax.plot(PARTITIONS, cons[sz], '-s', color=GREEN, linewidth=1.8,
                markersize=6, label='Consumer', zorder=5)
        for j, p in enumerate(PARTITIONS):
            ax.annotate(f'{prod[sz][j]:.1f}', (p, prod[sz][j]),
                        textcoords='offset points', xytext=(0, 8),
                        fontsize=7.5, ha='center', color=BLUE)
            ax.annotate(f'{cons[sz][j]:.1f}', (p, cons[sz][j]),
                        textcoords='offset points', xytext=(0, -12),
                        fontsize=7.5, ha='center', color=GREEN)
        ax.set_title(label, fontweight='bold')
        ax.set_xlabel('Partitions')
        ax.set_xticks(PARTITIONS)
        ax.set_ylim(0, 120)

    axes[0].set_ylabel('Throughput (MB/s)')
    axes[-1].legend(loc='center right', frameon=True)
    fig.suptitle('Native Kafka Throughput (acks=1, 1M msg/s target)',
                 fontweight='bold', fontsize=11.5, y=1.01)
    fig.tight_layout()
    _save(fig, 'fig2_native_kafka_throughput', output_dir)


def fig3_acks_comparison(native_rows, output_dir):
    prod_1 = extract_producer(native_rows, '1')
    prod_all = extract_producer(native_rows, 'all')

    fig, axes = plt.subplots(1, 3, figsize=(11, 3.5), sharey=True)
    x = np.arange(len(PARTITIONS))
    w = 0.32

    for i, (sz, label) in enumerate(zip(NATIVE_MSG_SIZES, MSG_LABELS)):
        ax = axes[i]
        b1 = ax.bar(x - w / 2, prod_1[sz], w, label='acks=1',
                     color=BLUE, edgecolor='black', linewidth=0.4)
        b2 = ax.bar(x + w / 2, prod_all[sz], w, label='acks=all',
                     color=ORANGE, edgecolor='black', linewidth=0.4)
        for bar in (*b1, *b2):
            ax.text(bar.get_x() + bar.get_width() / 2.,
                    bar.get_height() + 1,
                    f'{bar.get_height():.1f}',
                    ha='center', va='bottom', fontsize=6.5)
        ax.set_title(label, fontweight='bold')
        ax.set_xlabel('Partitions')
        ax.set_xticks(x)
        ax.set_xticklabels(PARTITIONS)
        ax.set_ylim(0, 120)

    axes[0].set_ylabel('Throughput (MB/s)')
    axes[-1].legend(loc='lower right', frameon=True)
    fig.suptitle('Impact of Acknowledgment Settings (RF=1)',
                 fontweight='bold', fontsize=11.5, y=1.01)
    fig.tight_layout()
    _save(fig, 'fig3_acks_comparison', output_dir)


def fig4_spark_kafka_throughput(spark_data, output_dir):
    fig, ax = plt.subplots(figsize=(9, 4.5))
    x_base = np.array([0, 5, 10])
    bar_w = 0.85

    for pi, p in enumerate(PARTITIONS):
        means, stds = [], []
        for sz in SPARK_RECORD_SIZES:
            vals = spark_data.get((p, sz), [0.0])
            means.append(np.mean(vals))
            stds.append(np.std(vals))

        pos = x_base + pi * bar_w
        bars = ax.bar(pos, means, bar_w * 0.82, yerr=stds,
                      color=COLORS_4[pi], edgecolor='black', linewidth=0.4,
                      capsize=3,
                      label=f'{p} partition{"s" if p > 1 else ""}',
                      error_kw={'linewidth': 0.8})
        for bar, m in zip(bars, means):
            ax.text(bar.get_x() + bar.get_width() / 2.,
                    bar.get_height() + 4,
                    f'{m:.0f}', ha='center', va='bottom',
                    fontsize=7.5, fontweight='bold')

    ax.set_ylabel('Throughput (MB/s)')
    ax.set_xlabel('Message Size')
    ax.set_title('Spark-Kafka Structured Streaming Throughput\n'
                 '(16 KB batch, acks=1, mean of 3 runs \u00b1 std)',
                 fontweight='bold', fontsize=11.5)
    ax.set_xticks(x_base + 1.5 * bar_w)
    ax.set_xticklabels(MSG_LABELS)
    ax.set_ylim(0, 210)
    ax.legend(loc='upper left', ncol=2, frameon=True)
    fig.tight_layout()
    _save(fig, 'fig4_spark_kafka_throughput', output_dir)


def fig5_spark_vs_native(native_rows, spark_data, output_dir):
    native_prod = extract_producer(native_rows, '1')

    spark_means = {}
    for sz_native, sz_spark in zip(NATIVE_MSG_SIZES, SPARK_RECORD_SIZES):
        vals = []
        for p in PARTITIONS:
            runs = spark_data.get((p, sz_spark), [0.0])
            vals.append(np.mean(runs))
        spark_means[sz_native] = vals

    fig, axes = plt.subplots(1, 3, figsize=(11, 3.5), sharey=True)
    for i, (sz, label) in enumerate(zip(NATIVE_MSG_SIZES, MSG_LABELS)):
        ax = axes[i]
        ax.plot(PARTITIONS, native_prod[sz], '-o', color=BLUE, linewidth=1.8,
                markersize=6, label='Native Kafka CLI', zorder=5)
        ax.plot(PARTITIONS, spark_means[sz], '-s', color=RED, linewidth=1.8,
                markersize=6, label='Spark Structured\nStreaming', zorder=5)
        ax.fill_between(PARTITIONS, native_prod[sz], spark_means[sz],
                        alpha=0.08, color=RED)
        ax.set_title(label, fontweight='bold')
        ax.set_xlabel('Partitions')
        ax.set_xticks(PARTITIONS)
        ax.set_ylim(0, 200)

    axes[0].set_ylabel('Throughput (MB/s)')
    axes[-1].legend(loc='lower right', fontsize=8, frameon=True)
    fig.suptitle('Native Kafka CLI vs. Spark Structured Streaming',
                 fontweight='bold', fontsize=11.5, y=1.01)
    fig.tight_layout()
    _save(fig, 'fig5_spark_vs_native', output_dir)


def extract_producer_latency(rows, acks, field='avg_latency_ms', msg_sizes=None):
    """Producer latency at 1M throughput target, keyed by msg_size."""
    if msg_sizes is None:
        msg_sizes = NATIVE_MSG_SIZES
    result = {}
    for sz in msg_sizes:
        vals = []
        for p in PARTITIONS:
            hits = [r for r in rows
                    if r['test_type'] == 'producer'
                    and int(r['partitions']) == p
                    and int(r['message_size_bytes']) == sz
                    and r['producer_throughput'].strip() == '1000000'
                    and r['producer_acks'].strip() == acks]
            vals.append(float(hits[0][field]) if hits else 0.0)
        result[sz] = vals
    return result


def fig6_latency_analysis(native_rows, output_dir):
    avg_acks1 = extract_producer_latency(native_rows, '1', 'avg_latency_ms')
    avg_acksA = extract_producer_latency(native_rows, 'all', 'avg_latency_ms')
    tput_acks1 = extract_producer(native_rows, '1')

    fig, axes = plt.subplots(1, 3, figsize=(11, 3.5), sharey=False)

    for i, (sz, label) in enumerate(zip(NATIVE_MSG_SIZES, MSG_LABELS)):
        ax = axes[i]
        ax.bar(np.arange(len(PARTITIONS)) - 0.17, avg_acks1[sz], 0.30,
               label='acks=1', color=BLUE, edgecolor='black', linewidth=0.4)
        ax.bar(np.arange(len(PARTITIONS)) + 0.17, avg_acksA[sz], 0.30,
               label='acks=all', color=ORANGE, edgecolor='black', linewidth=0.4)

        for j in range(len(PARTITIONS)):
            ax.text(j - 0.17, avg_acks1[sz][j] + 2,
                    f'{avg_acks1[sz][j]:.0f}', ha='center', fontsize=6.5)
            ax.text(j + 0.17, avg_acksA[sz][j] + 2,
                    f'{avg_acksA[sz][j]:.0f}', ha='center', fontsize=6.5)

        ax.set_title(label, fontweight='bold')
        ax.set_xlabel('Partitions')
        ax.set_xticks(np.arange(len(PARTITIONS)))
        ax.set_xticklabels(PARTITIONS)
        ax.set_ylabel('Avg Latency (ms)' if i == 0 else '')

    axes[-1].legend(loc='upper right', frameon=True)
    fig.suptitle('Producer Avg Latency (1M msg/s target)',
                 fontweight='bold', fontsize=11.5, y=1.01)
    fig.tight_layout()
    _save(fig, 'fig6_latency_analysis', output_dir)


def fig7_throughput_vs_latency(native_rows, output_dir):
    """Scatter: throughput vs latency for all producer configs at 1M target."""
    fig, ax = plt.subplots(figsize=(7, 4.5))

    markers = {'100': 'o', '1024': 's', '10240': 'D'}
    labels_map = {100: '100 B', 1024: '1 KB', 10240: '10 KB'}

    for sz in NATIVE_MSG_SIZES:
        tputs_1, lats_1 = [], []
        tputs_a, lats_a = [], []
        for p in PARTITIONS:
            for acks, tlist, llist in [('1', tputs_1, lats_1),
                                        ('all', tputs_a, lats_a)]:
                hits = [r for r in native_rows
                        if r['test_type'] == 'producer'
                        and int(r['partitions']) == p
                        and int(r['message_size_bytes']) == sz
                        and r['producer_throughput'].strip() == '1000000'
                        and r['producer_acks'].strip() == acks]
                if hits:
                    tlist.append(float(hits[0]['mb_per_sec']))
                    llist.append(float(hits[0]['avg_latency_ms']))

        mkr = markers[str(sz)]
        ax.scatter(tputs_1, lats_1, marker=mkr, s=60, color=BLUE, zorder=5,
                   edgecolors='black', linewidth=0.4,
                   label=f'{labels_map[sz]}, acks=1')
        ax.scatter(tputs_a, lats_a, marker=mkr, s=60, color=ORANGE, zorder=5,
                   edgecolors='black', linewidth=0.4,
                   label=f'{labels_map[sz]}, acks=all')

    ax.set_xlabel('Throughput (MB/s)')
    ax.set_ylabel('Avg Latency (ms)')
    ax.set_title('Throughput vs. Latency Tradeoff (1M msg/s target)',
                 fontweight='bold', fontsize=11.5)
    ax.legend(fontsize=7.5, ncol=2, loc='upper left', frameon=True)
    fig.tight_layout()
    _save(fig, 'fig7_throughput_vs_latency', output_dir)


# ── Main ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Generate paper figures from raw CSV data.')
    parser.add_argument('--data-dir', default=None,
                        help='Path to the data/ directory')
    parser.add_argument('--output-dir', default=None,
                        help='Directory for output figures (default: script dir)')
    args = parser.parse_args()

    data_dir = args.data_dir or _find_data_dir()
    output_dir = args.output_dir or SCRIPT_DIR
    os.makedirs(output_dir, exist_ok=True)
    _setup_style()

    native_csv = os.path.join(data_dir, NATIVE_CSV_RELPATH)
    print(f'Loading native Kafka data from {native_csv}')
    native_rows = load_native_csv(native_csv)
    print(f'  {len(native_rows)} rows loaded')

    print(f'Loading Spark-Kafka data (3 runs) from {data_dir}')
    spark_data = load_spark_runs(data_dir)
    print(f'  {len(spark_data)} unique (partition, size) configs, '
          f'{sum(len(v) for v in spark_data.values())} total measurements')

    print('\nGenerating figures...')
    fig2_native_kafka_producer(native_rows, output_dir)
    fig3_acks_comparison(native_rows, output_dir)
    fig4_spark_kafka_throughput(spark_data, output_dir)
    fig5_spark_vs_native(native_rows, spark_data, output_dir)
    fig6_latency_analysis(native_rows, output_dir)
    fig7_throughput_vs_latency(native_rows, output_dir)
    print(f'\nDone! Figures saved to: {output_dir}')


if __name__ == '__main__':
    main()
