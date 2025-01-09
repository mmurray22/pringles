# Pringles

A log which scales to the datacenter.


## General layout
The general layout of this repo is as follows:
```
Pringles
L___code: Contains all code for all logs (Pringles and comparisons)
    |   client: All client code for logging apps
    |   sequencer: All sequence code for logging apps
    |   storage: All storage code for logging apps
    |   utils: Functionality which supports various log functionality
    |   tutorials: Contains all bmv2 switch sim code. Will delete soon
|   proto: Contains all protobuf message types
|   experiments: Contains all scripts and application code for experiments
|   paper: Latex paper for submission
|   CMakeLists.txt: file specifying cmake infrastructure
|   README.md: File with general info about the project and the repo
```
## How to Compile
1.`cd /path/to/ringlog/build/`
2. `cmake ..`
3. `make`

## Necessary setup steps: (Debian trixie)
- See setup.sh for all relevant packages
- I am requiring C++17 and above

- Add path to ringlog as an environment variable RINGLOG_PATH
BEST: Add `export RINGLOG_PATH=/path/to/ringlog` to .bashrc
OR: Command `export RINGLOG_PATH=/path/to/ringlog`

## Current build system status:
01/2025: Using Meson build system. Primary commands are:
- `meson setup builddir && cd builddir`
- `meson compile`
- `meson test` // runs unit tests

## Current Compilation status:
01/08/2025: All code currently compiles and passes a single basic test.
Code consists solely of a networking library at the moment. Other code going
to be added in as layers on top of networking lib.
