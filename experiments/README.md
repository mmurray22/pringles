# How to Setup and Run Experiments
This document is a guide for setting up the infrastructure necessary to run experiments.

## Config files
All experiment clusters will be created based on a YAML specification. This YAML spec will include the following information:
 - Platform (Cloudlab, AWS, Azure)
 - Number of storage servers
 - Type of storage server machines
 - Number of clients
 - Type of client machines
 - Number of ordering nodes
 - Type of ordering machines
 - Location of machines (optional)
 - Machine Image (to be used for all machines)
 - System we are testing (e.g. Scalog, Ringer, etc.)
 - Workload we are running (directory)
 - File where workload stats should be written

## Workload

## Experiments on Cloudlab

### Setting up cloudlab 

