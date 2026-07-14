#!/usr/bin/env python3
import argparse
import json
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

def main():
    # Set up command line arguments
    parser = argparse.ArgumentParser(description="Plot JSON benchmark data.")
    parser.add_argument("x_key", help="JSON field key for the x-axis")
    parser.add_argument("y_key", help="JSON field key for the y-axis")
    parser.add_argument("x_label", nargs="?", default=None, help="Optional label for the x-axis")
    parser.add_argument("y_label", nargs="?", default=None, help="Optional label for the y-axis")
    parser.add_argument("--no-labels", action="store_true", help="Remove the filename labels from the data points")
    parser.add_argument("--dir", nargs="+", default=["."], help="One or more directories containing JSON files (defaults to current dir)")
    parser.add_argument("--out", default="plot.png", help="Output filename for the graph")
    
    args = parser.parse_args()

	# Data extraction
    data = []
    for directory in args.dir:
        search_path = os.path.join(directory, "*.json")
        for filepath in glob.glob(search_path):
            with open(filepath, 'r') as file:
                try:
                    d = json.load(file)
                    # Ensure the required keys exist in this JSON file
                    if args.x_key in d and args.y_key in d and "system_name" in d:
                        filename = os.path.splitext(os.path.basename(filepath))[0]
                        # Capture the time the file was last modified/created
                        file_time = os.path.getmtime(filepath) 
                        data.append({
                            args.x_key: d[args.x_key],
                            args.y_key: d[args.y_key],
                            'system_name': d['system_name'],
                            'filename': filename,
                            'timestamp': file_time  # Add it to our data
                        })
                except json.JSONDecodeError:
                    print(f"Warning: Could not parse {filepath}. Skipping.")

    if not data:
        print(f"No valid data found containing keys '{args.x_key}', '{args.y_key}', and 'system_name'.")
        return

    # Convert to Pandas DataFrame and sort by the x-axis so lines draw correctly (left-to-right)
    df = pd.DataFrame(data)
    df = df.sort_values(by='timestamp')

    # Plotting Setup
    plt.figure(figsize=(12, 7), dpi=150)
    
    # Aesthetic matching to your provided images
    plt.grid(True, linestyle='--', color='#d3d3d3', linewidth=0.7)
    ax = plt.gca()
    for spine in ax.spines.values():
        spine.set_edgecolor('#cccccc')
    ax.set_facecolor('#ffffff')
    plt.gcf().patch.set_facecolor('#ffffff')

    # Styles for up to 6 distinct systems (can easily be expanded)
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#800080', '#d62728', '#8c564b']
    linestyles = ['-', '-', '-', '-', '--', '-.']
    markers = ['o', 'x', 's', 'D', '^', 'v']

    # Group by system_name and plot
    groups = df.groupby('system_name')
    for i, (name, group) in enumerate(groups):
        color = colors[i % len(colors)]
        ls = linestyles[i % len(linestyles)]
        marker = markers[i % len(markers)]
        
        # Plot the line and markers
        plt.plot(group[args.x_key], group[args.y_key], 
                 label=name, color=color, linestyle=ls, marker=marker, 
                 linewidth=2.5, markersize=7)
        
        # Annotate the points with filenames
        if not args.no_labels:
            for _, row in group.iterrows():
                plt.annotate(row['filename'], 
                             (row[args.x_key], row[args.y_key]),
                             textcoords="offset points",
                             xytext=(8, 0), # Offset slightly to the right of the point
                             ha='left', va='center',
                             fontsize=9, color='#666666')

    # Formatting labels and titles
    final_x_label = args.x_label if args.x_label else args.x_key
    final_y_label = args.y_label if args.y_label else args.y_key

    plt.xlabel(final_x_label, fontsize=12)
    plt.ylabel(final_y_label, fontsize=12)
    
    # Create a clean title based on the axes
    title_text = f"{final_y_label} vs. {final_x_label}"
    plt.title(title_text, fontsize=16, pad=20)

    # Add legend if there are multiple systems, matching the "Experiments" title in your image
    if len(groups) > 0:
        plt.legend(title="Experiments", title_fontsize='11', fontsize='10', 
                   loc='center left', bbox_to_anchor=(1.02, 0.5), frameon=True, facecolor='white', framealpha=1)

    # Save and display
    plt.tight_layout()
    plt.savefig(args.out)
    print(f"Plot saved successfully to {args.out}")
    plt.show()

if __name__ == "__main__":
    main()
