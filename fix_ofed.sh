#!/bin/bash

sudo dpkg --remove --force-remove-reinstreq mlnx-ofed-kernel-dkms
sudo apt autoremove
sudo apt autoclean
sudo dpkg --configure -a

