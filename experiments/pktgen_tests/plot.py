#!/usr/bin/env python3
"""
P4 Benchmark Plotting Utilities
Author: Network & Systems Performance Engineer
Description: Extracts performance telemetry from experiment JSON footprints
             (one file per active pipeline per run) and plots aggregate
             offered load vs. aggregate observed throughput, with independent
             reference-line families for recirc-port count and pipeline count.
Usage:
    ./plot.py /results/common_name*.json
"""

import os
import sys
import json
import glob
import logging
from collections import defaultdict
import matplotlib.pyplot as plt

# Globally adjust the canvas dimension to stay clean and compliant
plt.rcParams["figure.figsize"] = (11, 6.5)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("BenchmarkPlotter")

# Physical constants used to compute the TRUE achievable ceiling for a port/pipe,
# as opposed to a naive (unreachable) payload-only calculation.
DEFAULT_PAYLOAD_SIZE = 100   # full on-wire frame size, fallback for older JSON files lacking 'payload_size'
PHY_OVERHEAD_BYTES = 20     # mandatory 8B preamble + 12B inter-frame gap, per frame, always present
PORT_LINE_RATE_BPS = 100e9  # 100G per port -- make this a JSON field too if you ever mix port speeds


def per_pipe_capacity_pps(payload_size):
    """
    Physical max packets/sec achievable by a single 100G port for a given frame
    size, accounting for mandatory preamble + inter-frame gap overhead (which is
    never part of payload_size, since it's line-side signaling, not frame content).
    """
    return PORT_LINE_RATE_BPS / (8.0 * (payload_size + PHY_OVERHEAD_BYTES))


def load_telemetry_data(file_paths):
    """
    Parses a list of JSON files -- ONE FILE PER ACTIVE PIPELINE PER RUN -- and
    extracts per-file records. Aggregation across pipelines belonging to the
    same run happens separately, in aggregate_by_run().
    """
    records = []

    for path in file_paths:
        if not os.path.exists(path):
            logger.warning(f"File path not found, skipping: {path}")
            continue

        try:
            with open(path, 'r') as f:
                data = json.load(f)

            nsperpkt = data.get('nsperpkt')
            throughput = data.get('throughput_pps')
            avg_latency = data.get('average_latency_us')
            total_loopback = data.get('total_loopback')
            num_pipelines = data.get('num_pipelines')
            payload_size = DEFAULT_PAYLOAD_SIZE #data.get('payload_size')

            if nsperpkt is None or throughput is None or total_loopback is None:
                logger.warning(f"File {path} missing critical fields ('nsperpkt', 'throughput_pps', "
                                f"or 'total_loopback'). Skipping.")
                continue

            if num_pipelines is None:
                logger.warning(f"File {path} missing 'num_pipelines' -- assuming 1 (single-pipe run).")
                num_pipelines = 1

            if payload_size is None:
                logger.warning(f"File {path} missing 'payload_size' -- assuming default of "
                                f"{DEFAULT_PAYLOAD_SIZE} bytes.")
                payload_size = DEFAULT_PAYLOAD_SIZE

            # Requested vs. physically-achievable offered load for THIS ONE PIPE.
            # A pipe can never actually offer more than its port's line rate allows,
            # regardless of what nsperpkt was configured to request.
            requested_rate = (1_000_000_000.0 / nsperpkt) if nsperpkt > 0 else 0.0
            cap = per_pipe_capacity_pps(payload_size)
            capped_rate_per_pipe = min(requested_rate, cap)

            if requested_rate > cap:
                logger.info(f"{os.path.basename(path)}: requested {requested_rate:,.0f} pps/pipe exceeds "
                            f"the {payload_size}B-frame 100G physical cap of {cap:,.0f} pps/pipe -- capping.")

            records.append({
                'file_name': os.path.basename(path),
                'nsperpkt': nsperpkt,
                'num_pipelines': num_pipelines,
                'total_loopback': total_loopback,
                'payload_size': payload_size,
                'throughput_pps': throughput,
                'average_latency_us': avg_latency,
                'capped_offered_load_per_pipe': capped_rate_per_pipe,
            })

        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed parsing file {path}: {str(e)}")

    return records


def aggregate_by_run(records):
    """
    Groups per-pipe records into ONE aggregate data point per run, keyed by
    (num_pipelines, total_loopback, nsperpkt). Sums throughput and capped
    offered load across every pipe belonging to that run, since a run's true
    aggregate offered load / throughput is the sum of its pipes' individual
    contributions, not any single pipe's file in isolation.
    """
    groups = defaultdict(list)
    for r in records:
        key = (r['num_pipelines'], r['total_loopback'], r['nsperpkt'])
        groups[key].append(r)

    aggregated = []
    for (num_pipelines, total_loopback, nsperpkt), group in groups.items():
        if len(group) != num_pipelines:
            logger.warning(f"Run (num_pipelines={num_pipelines}, total_loopback={total_loopback}, "
                            f"nsperpkt={nsperpkt}) has {len(group)} file(s) but expected {num_pipelines} "
                            f"-- data may be incomplete for this point.")

        offered_load = sum(r['capped_offered_load_per_pipe'] for r in group)
        throughput = sum(r['throughput_pps'] for r in group)

        payload_size_values = {r['payload_size'] for r in group}
        if len(payload_size_values) > 1:
            logger.warning(f"Run (num_pipelines={num_pipelines}, total_loopback={total_loopback}, "
                            f"nsperpkt={nsperpkt}) has inconsistent payload_size across its pipe files: "
                            f"{payload_size_values}. Using the first value encountered.")
        payload_size = next(iter(payload_size_values))

        aggregated.append({
            'num_pipelines': num_pipelines,
            'total_loopback': total_loopback,
            'nsperpkt': nsperpkt,
            'payload_size': payload_size,
            'offered_load_pps': offered_load,
            'throughput_pps': throughput,
        })

    aggregated.sort(key=lambda x: x['offered_load_pps'])
    return aggregated


def plot_send_rate_vs_throughput(aggregated, output_filename="throughput_vs_send_rate.png"):
    """
    Generates offered load vs. throughput, with:
      - one solid empirical curve per distinct num_pipelines value (colored)
      - one vertical dotted ceiling per num_pipelines value (max offered load
        achievable given that many independent packet generators), same color
        as its matching empirical curve
      - one horizontal dashed ceiling per distinct total_loopback (recirc port
        count) value, in a neutral gray family with direct text labels --
        deliberately NOT color-matched to pipeline count, since these are two
        independent dimensions, not two views of the same one.
    """
    if not aggregated:
        logger.error("No valid data points found to plot!")
        return

    logger.info(f"Generating Offered Load vs Throughput graph with {len(aggregated)} aggregate run(s)...")

    max_observed_x = max(p['offered_load_pps'] for p in aggregated)
    max_observed_y = max(p['throughput_pps'] for p in aggregated)
    plot_extent = max(max_observed_x, max_observed_y) * 1.15

    # --- Group aggregate points by num_pipelines for the solid empirical curves ---
    by_pipelines = defaultdict(list)
    for p in aggregated:
        by_pipelines[p['num_pipelines']].append(p)

    pipeline_values = sorted(by_pipelines.keys())
    color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']
    pipeline_colors = {n: color_cycle[i % len(color_cycle)] for i, n in enumerate(pipeline_values)}

    labels_to_render = []

    for n_pipelines in pipeline_values:
        group = sorted(by_pipelines[n_pipelines], key=lambda x: x['offered_load_pps'])
        x_vals = [p['offered_load_pps'] for p in group]
        y_vals = [p['throughput_pps'] for p in group]
        color = pipeline_colors[n_pipelines]
        plural = 's' if n_pipelines != 1 else ''

        plt.plot(x_vals, y_vals, marker='o', linestyle='-', linewidth=2,
                  color=color, label=f"{n_pipelines} pipeline{plural} (observed)")

        # Vertical dotted ceiling: max offered load reachable with n_pipelines
        # independent packet generators, each capped at its own port's physical rate.
        payload_size = group[0]['payload_size']
        max_offered_for_n = n_pipelines * per_pipe_capacity_pps(payload_size)
        if max_offered_for_n <= plot_extent:
            plt.axvline(x=max_offered_for_n, linestyle=':', color=color, alpha=0.6, linewidth=1.5)
            plt.text(max_offered_for_n, plot_extent * 0.015, f" max load, {n_pipelines}p",
                      color=color, fontsize=7, rotation=90, va='bottom', ha='left')

        plateau_val = max(y_vals)
        labels_to_render.append({
            'x': x_vals[-1], 'y_orig': y_vals[-1],
            'text': f" {plateau_val:,.0f} pps", 'color': color
        })

    # --- Horizontal dashed ceilings for recirc-port count (independent dimension) ---
    recirc_values = sorted({p['total_loopback'] for p in aggregated})
    payload_size_by_recirc = {}
    for p in aggregated:
        payload_size_by_recirc.setdefault(p['total_loopback'], p['payload_size'])

    for r in recirc_values:
        payload_size = payload_size_by_recirc[r]
        ceiling_y = r * per_pipe_capacity_pps(payload_size)
        plural = 's' if r != 1 else ''
        if ceiling_y <= plot_extent:
            plt.axhline(y=ceiling_y, linestyle='--', color='gray', alpha=0.5, linewidth=1.2)
            plt.text(plot_extent * 0.995, ceiling_y, f" {r} recirc port{plural} ceiling",
                      color='dimgray', fontsize=8, va='bottom', ha='right')

    # --- Fan the plateau value labels (same technique as before) ---
    labels_to_render.sort(key=lambda l: l['y_orig'])
    for i, label in enumerate(labels_to_render):
        if i == 0:
            fan_angle, v_align = -20, 'top'
        elif i == 1:
            fan_angle, v_align = 0, 'center'
        elif i == 2:
            fan_angle, v_align = 12, 'bottom'
        elif i == 3:
            fan_angle, v_align = 24, 'bottom'
        else:
            fan_angle, v_align = 24 + (i - 3) * 10, 'bottom'

        plt.text(
            label['x'], label['y_orig'], label['text'],
            color=label['color'], fontweight='bold', fontsize=8,
            rotation=fan_angle, va=v_align, ha='left'
        )

    # Detailing and Metadata Setup
    plt.title("P4 Switch Packet Gen Performance: Offered Load vs Throughput",
              fontsize=14, fontweight='bold', pad=15)
    plt.xlabel("Aggregate Offered Load Across Active Pipelines (Packets per Second)", fontsize=11, labelpad=10)
    plt.ylabel("Observed Aggregate Throughput (Packets per Second)", fontsize=11, labelpad=10)
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend(loc="upper left", fontsize=9)
    plt.xlim(0, plot_extent)
    plt.ylim(0, plot_extent)

    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    logger.info(f"Successfully saved graph visualization directly to: {output_filename}")
    plt.close()


def main():
    args = sys.argv[1:]
    if not args:
        logger.critical("No JSON input pattern provided!")
        print("Usage: ./plot.py <path_to_json_files_or_wildcard>")
        print("Example: ./plot.py /results/common_name*.json")
        sys.exit(1)

    target_files = []
    for argument in args:
        matched = glob.glob(argument)
        if matched:
            target_files.extend(matched)
        else:
            target_files.append(argument)

    target_files = list(set(target_files))
    logger.info(f"Discovered {len(target_files)} target files matching invocation footprint.")

    data_points = load_telemetry_data(target_files)
    aggregated = aggregate_by_run(data_points)
    plot_send_rate_vs_throughput(aggregated)


if __name__ == "__main__":
    main()
