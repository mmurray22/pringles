#!/bin/bash

rmmod bf_kpkt
rmmod bf_knet
insmod bf_kpkt/bf_kpkt.ko kpkt_mode=1
insmod bf_knet/bf_knet.ko
ip link set dev enp5s0 up
ip link set dev enp5s0 promisc on
