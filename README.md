# Pringles

The log which scales to the datacenter.

## Codebase overview
The general layout of this repo is as follows:
```
Pringles
L___code: Contains all code for the Pringles and Corfu logs
    |   include: Contains all header files
    |   src: Contains all .cpp files
    |   proto: Contains all protobuf message types for all protocols
    |   tutorials: Contains all P4 code and bmv2 switch simulator
L___experiments: Contains all scripts and application code for experiments
    |   unit_tests: Test files which target specific parts of the codebase, including the networking library, client impl, etc.
    |   yaml: Folder containing all YAML configurations for the logging protocol
    |   traces: Contains a variety of traces for testing and performance eval
    |   scripts: Scripts to run the full experiments for measuring performance of the different logging protocols
    |   results: Store performance results from experiment runs
    |   applications: Applications which will use the logging protocols in experiments 
L___paper: Latex paper for submission (STALE need to update)
L___meson.build: Core meson build file which describes how to build and compile the entire project
L___setup.sh: Bash script which installs all necessary dependencies for the project
L___README.md: File with general info about the project and the repo
```

Key components of the codebase:
- Network class: The underlying networking library which sends and receives all messages for all supported logging protocols. Contains a threadpool for the sending of messages. supports UDP and custom raw packets, and will support message batching (TODO).
- Trace class: This class ingests a text file with two space separated columns, with the first column specifying the type of request (e.g. append, read, etc.) and the second column specifying the actual payload of the request.
- Base Client class: Sets up a virtual class from which all client classes for custom logging protocols can be derived. Contains core functions including append, read, subscribe, getTail, and trim.
- Base Storage class: Sets up a virtual class from which all storage server classes for custom logging protocols can be derived. Contains core functions including getting from and writing to a KV store of indices --> values
- Structs/Custom headers: Contains all the custom headers used by all logging protocols and helper functions for their construction/destruction.
- Utils: A collection of general purpose functions, which are mostly focused on YAML parsing.

## How to Setup and Compile
See setup.sh for all relevant packages. C++17 at minimum required. If you are using cloudlab for testing, you can use the Cloudlab profile `run-pringles` which already has the appropriate packages.

To compile, follow these steps:
1. `cd /path/to/pringles`
2.  `meson setup build && cd build`
3. `meson compile`

## How to Run

** NOTE **: All executables must be run with `sudo` due to the use of raw sockets in the networking library.

To execute unit tests, first `cd /path/to/ringlog/build` and then `sudo meson test`.

To run a specific binary file, first `cd /path/to/ringlog/build` and then `sudo ./path/to/executable arg1 arg2 ... argN`.

## Resources
This project makes use of a series of libraries, tools, and languages that users may have varying levels of experience with. You can find some resources to better understand these components of the codebase below:

### C++
This project makes use of C++17. Here are some good resources to reference when reading through and adding to the code:
- [cppreference](https://en.cppreference.com/w/) This is the primary C++ reference website. All syntax questions should go to this website first since it's very thorough and does a decent job of explaining most C++ functionality.
- [learncpp](https://www.learncpp.com/) This website has a series of tutorials on all primary aspects of the C++ language, including examples. I recommend checking out Chapters 12, 14, 15, 22, 24 and 25.
- [CS106L](https://web.stanford.edu/class/cs106l/index.html) This class is a C++ intro class at Stanford which has some good information.

StackOverflow is also a great resource when you have a question about how to program something or how to handle an error. Speaking of errors, there are also many tools to debug C++. A couple of resources I'll recommend: 
- gdb: allows for stepping through each instruction of the program and analyzing the state of program memory. Here's an [introduction/refresher to GDB](https://web.stanford.edu/class/archive/cs/cs107/cs107.1254/resources/gdb) and also a [concise reference card](https://web.stanford.edu/class/archive/cs/cs107/cs107.1254/resources/gdb_refcard.pdf) you can download.
- [valgrind](https://web.stanford.edu/class/archive/cs/cs107/cs107.1254/resources/valgrind): Detects memory leaks and memory errors, use early and often.
- [Address sanitizers](https://mesonbuild.com/howtox.html#use-address-sanitizer): For detecting and resolving memory errors, which you can [read more about here](https://cppscripts.com/address-sanitizer-cpp). To use the address sanitizer, run `meson setup -Db_sanitize=address`. To turn off address santiizers, run `meson setup -Db_sanitize=none`.
- Check out Chapter 3 from learncpp

### Meson
This project uses the Meson build system (as opposed to the maybe more known CMakeLists). You can read more about meson [at this link](https://mesonbuild.com/). I would particularly check out their [tutorial's page](https://mesonbuild.com/Tutorial.html), [unit tests info](https://mesonbuild.com/Unit-tests.html), [how to do X in meson](https://mesonbuild.com/howtox.html), and [FAQ](https://mesonbuild.com/FAQ.html). Note that if you are doing non-Pringles development, you will not need to alter the meson build system too much. Most updates will be adding header and cpp files to the respective include and src meson.build files, as well as additional tests to the meson.build in unit_tests.

### Git
This project uses Git to push changes to the codebase, open issues, submit PRs, etc. You can reference this [git intro manual](https://git-scm.com/docs/gittutorial) for more information on how to use git.

### YAML
We are using [Yet Another Markup Language](https://www.redhat.com/en/topics/automation/what-is-yaml) (YAML) to specify the configuration for the network, traces, log clients, etc. The YAML parsing library we use is yaml-cpp, and you can find more information on that library [here](https://github.com/jbeder/yaml-cpp/wiki/Tutorial).

### Protobuf
We use [protobuf](https://protobuf.dev/) to structure data and easily serailize/deserialize the data to send over the network. You can find specific information on Proto3 for C++ [here](https://protobuf.dev/getting-started/cpptutorial/).

### spdlog
Print statements are a key way to get insight into what is going on in the program, but sometimes we only want to display some print statements if we are debugging versus if we are doing a full experiment run where we want to collect meaningful numbers. The [spdlog library](https://github.com/gabime/spdlog) automatically takes care of that, allowing you to choose from a [7 different log printing levels](https://internal.dunescience.org/doxygen/namespacespdlog_1_1level.html#a35f5227e5daf228d28a207b7b2aefc8b). 

While spdlog has 7 different types of log statements to choose from, you can primarily just use `spdlog::debug` and `spdlog::critical`. To display the debug statements, set log level to 1 and to hide the debug statements (and only print critical ones) set the log level to 5.