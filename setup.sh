#!/bin/bash

# All the packages which need to be installed on clean debian machine
sudo apt update
sudo apt upgrade
sudo apt install -y ninja-build python3 python3-setuptools dpkg-dev protobuf-compiler libyaml-cpp-dev meson libspdlog-dev pkg-config
sudo apt install python3-pip
#sudo apt install -y protobuf-compiler # Needs to be at least v3.x.x

# utilities
sudo apt install -y ripgrep vim net-tools iputils-ping gdb tmux tcpdump

#pip install toml PyYAML
sudo apt install python3-toml python3-yaml python3-matplotlib
