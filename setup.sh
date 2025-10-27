#!/bin/bash

# All the packages which need to be installed on clean debian machine
sudo apt update
sudo apt upgrade
sudo apt install ninja-build
sudo apt install python3
sudo apt install python3-setuptools
sudo apt install dpkg-dev
sudo apt install -y protobuf-compiler # Needs to be at least v3.x.x
sudo apt install libyaml-cpp-dev
sudo apt install libspdlog-dev
sudo apt install meson
sudo apt install pkg-config
sudo apt install openssl # New
sudo apt-get install libssl-dev

# utilities
sudo apt install ripgrep
sudo apt install vim
sudo apt install net-tools
sudo apt install iputils-ping
sudo apt install gdb
sudo apt install tmux
sudo apt install tcpdump
