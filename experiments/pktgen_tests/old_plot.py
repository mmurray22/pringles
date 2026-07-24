#!/usr/bin/env python3
"""
P4 Benchmark Plotting Utilities
Author: Network & Systems Performance Engineer
Description: Extracts performance telemetry from experiment JSON footprints 
             and provides scalable functions for data visualization.
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


def load_telemetry_data(file_paths):
    """
    Parses a list of JSON files and extracts key tracking variables.
    Calculates send_rate_s dynamically from nsperpkt.
    """
    dataset = []
    
    for path in file_paths:
        if not os.path.exists(path):
            logger.warning(f"File path not found, skipping: {path}")
            continue
            
        try:
            with open(path, 'r') as f:
                data = json.load(f)
                
            # Extract desired metrics (fall back to None if missing)
            nsperpkt = data.get('nsperpkt')
            throughput = data.get('throughput_pps')
            avg_latency = data.get('average_latency_us')
            total_loopback = data.get('total_loopback')
            
            if nsperpkt is None or throughput is None or total_loopback is None:
                logger.warning(f"File {path} missing critical fields ('nsperpkt', 'throughput_pps', or 'total_loopback'). Skipping.")
                continue

            # Calculate send rate: 1,000,000,000 / nsperpkt
            if nsperpkt > 0:
                send_rate_s = 1_000_000_000.0 / nsperpkt
            else:
                send_rate_s = 0.0
                
            dataset.append({
                'file_name': os.path.basename(path),
                'nsperpkt': nsperpkt,
                'send_rate_s': send_rate_s,
                'throughput_pps': throughput,
                'average_latency_us': avg_latency,
                'total_loopback': total_loopback
            })
            
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed parsing file {path}: {str(e)}")
            
    # Sort dataset by send_rate_s so that line plots render sequentially
    dataset.sort(key=lambda x: x['send_rate_s'])
    return dataset


def plot_send_rate_vs_throughput(dataset, output_filename="throughput_vs_send_rate.png"):
    """
    Generates a line plot with markers mapping Send Rate vs Throughput, 
    grouped and colored uniquely by total_loopback values with custom fanned labels.
    Includes theoretical ceilings for raw payload and payload + Preamble/IFG overhead.
    """
    if not dataset:
        logger.error("No valid data points found to plot!")
        return

    logger.info(f"Generating Send Rate vs Throughput graph with {len(dataset)} points...")

    # Find the absolute maximum offered load in the entire dataset to terminate the ideal ceiling lines cleanly
    max_observed_x = max(point['send_rate_s'] for point in dataset) if dataset else 1_000_000_000.0

    # Group records dynamically using total_loopback values as map keys
    grouped_data = defaultdict(list)
    for point in dataset:
        grouped_data[point['total_loopback']].append(point)

    labels_to_render = []

    # Plot each group sequentially
    for t_val in sorted(grouped_data.keys()):
        group = grouped_data[t_val]
        group.sort(key=lambda x: x['send_rate_s'])

        x_send_rates = [point['send_rate_s'] for point in group]
        y_throughput = [point['throughput_pps'] for point in group]

        # Plot observed line and capture auto-assigned color index
        line, = plt.plot(x_send_rates, y_throughput, marker='o', linestyle='-', linewidth=2, label=f"num_recirc_ports = {t_val}")
        line_color = line.get_color()

        # 1. Standard Ideal Curve (100 Byte Payload Ceiling)
        max_y = (t_val * 100 * 10**9) / (8 * 100)
        inflection_point_x = min(max_y, max_observed_x)
        ideal_x = [0, inflection_point_x, max_observed_x]
        ideal_y = [0, inflection_point_x, inflection_point_x]
        plt.plot(ideal_x, ideal_y, linestyle='--', color=line_color, alpha=0.3, label=f"Ideal (num_recirc_ports = {t_val})")

        # 2. Padded Ideal Curve (100 Byte Payload + 20 Bytes Preamble/IFG Overhead = 120 Bytes total)
        max_y_padded = (t_val * 100 * 10**9) / (8 * (100 + 20))
        inflection_point_padded_x = min(max_y_padded, max_observed_x)
        ideal_padded_x = [0, inflection_point_padded_x, max_observed_x]
        ideal_padded_y = [0, inflection_point_padded_x, inflection_point_padded_x]
        plt.plot(ideal_padded_x, ideal_padded_y, linestyle=':', color=line_color, alpha=0.5, label=f"Ideal w/ preamble + IFG (num_recirc_ports = {t_val})")

        # Determine the empirical plateau (maximum throughput observed for this group)
        plateau_val = max(y_throughput)
        
        # Position label at the right-most data point of this specific trendline
        labels_to_render.append({
            'x': x_send_rates[-1],
            'y_orig': y_throughput[-1],
            'text': f" {plateau_val:,.0f} pps",
            'color': line_color
        })

    # --- Precise Fanning Configuration ---
    # Sort labels vertically to process them sequentially from lowest throughput to highest
    labels_to_render.sort(key=lambda l: l['y_orig'])
    
    # Render data-point value tags with explicitly targeted angles
    for i, label in enumerate(labels_to_render):
        if i == 0:
            # Lowest line (Blue): Angle downwards out of the way
            fan_angle = -20
            v_align = 'top'
        elif i == 1:
            # Second line (Orange): Stays completely horizontal
            fan_angle = 0
            v_align = 'center'
        elif i == 2:
            # Third line (Green): Moderate upward angle
            fan_angle = 12
            v_align = 'bottom'
        elif i == 3:
            # Fourth line (Red): Angled slightly higher
            fan_angle = 24
            v_align = 'bottom'
        else:
            fan_angle = 24 + ((i - 3) * 10)
            v_align = 'bottom'
        
        plt.text(
            label['x'], 
            label['y_orig'], 
            label['text'], 
            color=label['color'], 
            fontweight='bold', 
            fontsize=8,        
            rotation=fan_angle,
            va=v_align,       
            ha='left'
        )

    # Detailing and Metadata Setup
    plt.title("Single P4 Switch Packet Gen Performance", fontsize=14, fontweight='bold', pad=15)
    plt.xlabel("Offered Load / Send Rate (Packets per Second)", fontsize=11, labelpad=10)
    plt.ylabel("Observed Throughput (Packets per Second)", fontsize=11, labelpad=10)
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend(loc="upper left")
    
    # Auto-adjust layouts and save to disk using tight bounding to dynamically manage whitespace
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
    plot_send_rate_vs_throughput(data_points)


if __name__ == "__main__":
    main()
