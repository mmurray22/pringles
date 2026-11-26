sudo apt install -y build-essential meson ninja-build python3-pyelftools \
  libnuma-dev pkg-config libpcap-dev rdma-core libibverbs-dev python3-scapy
wget https://fast.dpdk.org/rel/dpdk-23.11.tar.xz && tar xf dpdk-23.11.tar.xz
cd dpdk-23.11
meson setup build -Dexamples=skeleton \
  -Denable_drivers=bus/pci,bus/vdev,bus/auxiliary,mempool/ring,common/mlx5,net/mlx5,net/pcap,net/tap
ninja -C build && sudo ninja -C build install && sudo ldconfig

# Test to make sure everything works
sudo ./build/app/dpdk-testpmd --no-huge -m 512 --no-pci \
  --vdev 'net_pcap0,iface=lo' -- -i --total-num-mbufs=8192
