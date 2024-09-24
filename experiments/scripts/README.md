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

### Setting up cloudlab remote instantiation
Setting up a cloudlab cluster via the command line requires usage of a python library called geni-lib.One note about geni-lib is that it is compatible with *python2.7*. This means that geni-lib must be installed in a python2.7 environment. In order to successfully install geni-lib on the machine, the following steps need to be taken:

1. Install `libssl`, `libxml2`, `python2.7`. To install `python2.7` on Debian:
    - Add `deb http://archive.debian.org/debian/ stretch contrib main non-free` to `/etc/apt/sources.list`.
    - `sudo apt update`
    - `sudo apt install python2.7`
    - Remove the new line in sources.list
2. Install `pip` for *python2.7*. using apt package manager. To install `pip` properly:
    - `wget -P ~/.local/lib https://bootstrap.pypa.io/pip/2.7/get-pip.py`
    - `python2.7 ~/.local/lib/get-pip.py --user`
    - `printf "\nPATH=\$PATH:~/.local/bin/" >> ~/.bashrc`
    - `source ~/.bashrc`
3. Install `python2.7 -m pip install virtualenv`. 
4. In the ringlog/experiments/ folder, run `python2.7 -m virtualenv ./`. This creates a virtual python2.7 environment in which we will install geni-lib.
5. In that same folder, run `./bin/pip install geni-lib`

Congratulations! geni-lib has been successfully installed. The next step will be to create a context - a file which lets your Cloudlab experiment scripts know what geni platform, project, and user you are.

1. First, navigate to the Cloudlab GUI using a web browser. Log into your account. 
2. In the top right corner, click on your username and then click "Download Credentials". This should download a file called "cloudlab.pem" to your computer.
3. Navigate to your experiments directory (in our case, ringlog/experiments). 
4. Run `cp template_personal.yaml personal.yaml`. Then open `personal.yaml`.
5. Fill in all the empty fields with the appropriate values or directories.

