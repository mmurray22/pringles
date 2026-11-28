#!/bin/bash
cd /home/corfu-vm/pringles/
rm -rf build
meson setup build --default-library=static && cd build
meson compile
