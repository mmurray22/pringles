#!/bin/bash
cd /home/mathwiz23pi/pringles/
meson setup build --default-library=static && cd build
meson compile
