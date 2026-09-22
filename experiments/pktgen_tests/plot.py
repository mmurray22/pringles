import pandas as pd
import seaborn as sns
import os
import sys
import json
import glob
import re
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "script_libs")))
from json_parsing import *
from graphs import *


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("BenchmarkPlotter")

def main():
    # Get JSON files
    args = sys.argv[1:]
    if not args:
        logger.critical("No JSON input pattern provided!")
        print("Usage: ./plot.py <path_to_json_files_or_wildcard> OR ./plot.py <json_file>")
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

    # Parse JSON filenames
    datastore, json_files = parse_filename_metadata(target_files, logger)

    # Parse JSONs into dataframes
    parse_jsons(json_files, datastore, logger)

    # Now that JSON is in dataframes, time to plot

    #print(datastore)

    ########### TIMESERIES GRAPHS ########################
    MAX_RING_SIZE = 6
    pktgenGraphs = PktgenGraphs()
    pktgenGraphs.initialize(sys.argv[0])
    for i in range(6, MAX_RING_SIZE+1):
        grid_size = i if i % 2 == 0 else i + 1
        # Packet generation graph: Queue Timeseries Graph

        timeseries_df = [pipe[1] for switch in datastore[i].values() for pipe in switch.values()]
        graph_name = "queue_timeseries_test"
        graph_type = "line"
        pktgenGraphs.graph_grid(timeseries_df, "queue_count_timeseries", "time", "Queue Count Over Time", graph_name, graph_type, 1, grid_size)

        # Packet generation graph: Average Latency Timeseries Graph
        graph_name = "avg_latency_timeseries_test"
        graph_type = "line"
        pktgenGraphs.graph_grid(timeseries_df, "avg_lat_timeseries", "time", "Average Latency", graph_name, graph_type, 1, grid_size)

        # Packet generation graph: Throughput Timeseries Graph
        graph_name = "tput_timeseries_test"
        graph_type = "line"
        pktgenGraphs.graph_grid(timeseries_df, "tput_timeseries", "time", "Throughput", graph_name, graph_type, 1, grid_size)

        # Packet generation graph: True Pktgen Rate Timeseries Graph
        graph_name = "pktgen_timeseries_test"
        graph_type = "line"
        pktgenGraphs.graph_grid(timeseries_df, "true_gen_pkt_rate_timeseries", "time", "Packet Generation Rate", graph_name, graph_type, 1, grid_size)

        # Number of packets graph: Packets over time
        graph_name = "num_pkts_test"
        graph_type = "line"
        pktgenGraphs.graph_grid(timeseries_df, "num_pkts_timeseries", "time", "Number of Packets Over Time", graph_name, graph_type, 1, grid_size)


    ########### AGGREGATE GRAPHS ########################
    # Throughput v Latency over ring size
    graph_name = "tput_lat_test"
    tput_columns = []
    lat_columns = []
    for ring in datastore:
        tput = 0
        lat = 0
        num_lats = 0
        for switch in datastore[ring]:
            for pipe in datastore[ring][switch]:
                tput += datastore[ring][switch][pipe][0]["throughput_pps"][0]
                lat += datastore[ring][switch][pipe][0]["average_latency_us"][0]
                num_lats += 1
        tput_columns.append(tput)
        print(lat)
        print(tput)
        avg_lat = float(lat)/num_lats
        lat_columns.append(lat)
    tput_lat_columns = [tput_columns, lat_columns]
    tput_lat_df = pd.DataFrame({"Throughput (pps)": tput_columns, "Latency (us)": lat_columns})
    graph_type = "line"
    pktgenGraphs.graph(tput_lat_df, "Latency (us)", "Throughput (pps)", "Throughput vs. Latency as ringe sizes increases", graph_name, graph_type)


if __name__ == "__main__":
    main()
