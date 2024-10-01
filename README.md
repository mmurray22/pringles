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
## Necessary setup steps:
- I am requiring C++17 and above

- Install cmake (I am using version 3.30.3)

- Install protobuf (I am using proco version 3.21.12) 
Command: `sudo apt install -y protobuf-compiler` (Needs to be protoc --version > 3.x.x)

## Current build system status:
09/30: I am using cmake for this project since it is largely built in C++. The cmake bare bones skeleton has been created. 
Right now, I am only compiling the client C++ source files, but in time I will add all components of the logging
infrastructure to the CMake build system. The only executable being generated at the moment is a simple client 
which is meant to unit test the client libraries.

## Current Compilation status:
09/30: Primary codebase does NOT compile :') Will fix ASAP
09/30: Paper codebase also does NOT compile (missing a sty file)
