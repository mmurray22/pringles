#!/bin/bash

# All the packages which need to be installed on clean debian machine
sudo apt update
sudo apt -y upgrade
sudo apt -y install ninja-build python3 python3-setuptools dpkg-dev protobuf-compiler libyaml-cpp-dev meson libspdlog-dev pkg-config
sudo apt -y install python3-pip
#sudo apt -y install protobuf-compiler # Needs to be at least v3.x.x

# utilities
sudo apt -y install ripgrep vim net-tools iputils-ping gdb tmux tcpdump neovim
sudo apt -y install libtbb-dev

#pip install toml PyYAML
sudo apt install -y python3-toml python3-yaml python3-matplotlib

# Go
wget https://go.dev/dl/go1.26.1.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.26.1.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# Docker
sudo apt update
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
sudo apt update
sudo apt -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
