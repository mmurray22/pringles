import pandas as pd
import numpy as np
import os
import sys
import json
import glob
import re
import logging

# Matches orchestration.py's generated filename convention, e.g.:
#   ringsize_6_switch_2_pipe0_1_nsperpkt.json
# ring_size here is the TRUE total active-pipeline count across the whole
# distributed ring for this run -- NOT the same thing as the JSON's own
# 'num_pipelines' field, which reflects only how many pipes THIS ONE SWITCH
# configured (always 2, since the "always configure both pipes" fix), not how
# many were active across the whole multi-switch experiment.
FILENAME_RE = re.compile(r'ringsize_(\d+)_switch_(\d+)_pipe(\d+)_\d+_nsperpkt\.json$')


def parse_filename(path):
    """Extracts (switch_id, ring_size, pipe_number) from the standard
    orchestration.py filename convention. Returns None if the filename doesn't
    match (e.g. an older or manually-created file), so callers can fall back
    to a legacy grouping strategy for just that file rather than failing outright."""

    m = FILENAME_RE.search(os.path.basename(path))
    if not m:
        return None
    ring_size, switch_id, pipe_number = m.groups()
    return {
        'switch_id': int(switch_id),
        'ring_size': int(ring_size),
        'pipe_number': int(pipe_number)
    }


def parse_filename_metadata(target_files, logger):
    datastore = {}
    json_files = []
    for path in target_files:
        if not os.path.exists(path):
            logger.warning(f"File path not found, skipping: {path}")
            continue

        try:
            data = {}
            with open(path, 'r') as f:
                data = parse_filename(path)
            if data is None:
                continue
            if data['ring_size'] not in datastore.keys():
                print("New ring!")
                datastore[data['ring_size']] = {}
                datastore[data['ring_size']][data['switch_id']] = {}
                datastore[data['ring_size']][data['switch_id']][data['pipe_number']] = []
            elif data['switch_id'] not in datastore[data['ring_size']].keys():
                print("New switch!")
                datastore[data['ring_size']][data['switch_id']] = {}
                datastore[data['ring_size']][data['switch_id']][data['pipe_number']] = []
            elif data['pipe_number'] not in datastore[data['ring_size']][data['switch_id']].keys():
                print("New pipe!")
                datastore[data['ring_size']][data['switch_id']][data['pipe_number']] = []
            
            json_files.append(path)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed parsing file {path}: {str(e)}")
    return datastore, json_files

def parse_jsons(target_files, datastore, logger):
    for path in target_files:
        if not os.path.exists(path):
            logger.warning(f"File path not found, skipping: {path}")
            continue

        try:
            data = {}
            filename_data = {}
            with open(path, 'r') as f:
                data = json.load(f)
                filename_data = parse_filename(path)

            timeseries_df = pd.DataFrame()
            final_data_df = pd.DataFrame()

            interval_s = float(data["timeseries_interval_ms"])/1000
            duration = data["duration"]
            timeseries_df["time"] = list(np.arange(0, duration, interval_s))
            for key in data.keys():
                # Get type of the value
                if type(data[key]) is list:
                    # If the value is an array, add to timeseries dataframe
                    if len(data[key]) < len(timeseries_df["time"]):
                        # insert dummy values at the beginning
                        num_zeroes = duration*10 - len(data[key])
                        data[key] = [0]*int(num_zeroes) + data[key]
                    timeseries_df[key] = data[key]
                else:
                    # Else add to general pipe dataframe
                    final_data_df[key] = [data[key]] 

            #print(datastore)
            datastore[filename_data['ring_size']][filename_data['switch_id']][filename_data['pipe_number']].append(final_data_df)
            datastore[filename_data['ring_size']][filename_data['switch_id']][filename_data['pipe_number']].append(timeseries_df)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed parsing file {path}: {str(e)}")

