#!/bin/bash

# All the packages which need to be installed on clean debian machine
sudo apt update
sudo apt -y upgrade
sudo apt -y install ninja-build python3 python3-setuptools dpkg-dev protobuf-compiler libyaml-cpp-dev meson libspdlog-dev pkg-config
sudo apt -y install python3-pip
#sudo apt -y install protobuf-compiler # Needs to be at least v3.x.x

# utilities
sudo apt -y install ripgrep vim net-tools iputils-ping gdb tmux tcpdump
sudo apt -y install libtbb-dev

#pip install toml PyYAML
sudo apt install -y python3-toml python3-yaml python3-matplotlib
