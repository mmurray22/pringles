#!/usr/bin/env python3

# Expectations for the configuration yaml passed into this script require
# that it include all of the following fields:
# -- Platform (Cloudlab, AWS, Azure)
# -- Number of storage servers
# -- Type of storage server machines
# -- Number of clients
# -- Type of client machines
# -- Number of ordering nodes
# -- Type of ordering machines
# -- Location of machines (optional)
# -- Machine Image (to be used for all machines)
# -- System we are testing (e.g. Scalog, Ringer, etc.)
# -- Workload we are running (directory)
# -- File where workload stats should be written
# Additional fields, as indicated below, may be required depending on 
# certain deployment platforms.

import argparse
import yaml
from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from spawn_machines import *

def main(config_yaml):
    print("Starting main function! Reading in the config file\n")
    # Step 1: Read in setup config yaml which specifies:
    stream = open(config_yaml, 'r')
    config_dict = yaml.load_all(stream, Loader)
    # Step 2: Instantiate machines on chosen platform
    print("Now creating the machines\n")
    if config_dict['platform'] == 'cloudlab':
        spawn_cloudlab(config_dict)
    else:
        print("Platform not currently handled!\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Setup cluster')
    parser.add_argument('--config', metavar='path', required=True, help='the path to the config file')
    args = parser.parse_args()
    main(config_yaml=args.config)
