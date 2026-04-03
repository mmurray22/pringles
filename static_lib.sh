#!/bin/bash
sudo apt -y install cmake
cd /home/mathwiz23pi/pringles/spdlog
rm -rf build && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DSPDLOG_BUILD_STATIC=ON -DSPDLOG_BUILD_SHARED=OFF -DSPDLOG_BUILD_EXAMPLE=OFF -DSPDLOG_BUILD_TESTS=OFF
cmake --build . --parallel $(nproc)
cd /home/mathwiz23pi/pringles/oneTBB
rm -rf build && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DTBB_STATIC=ON -DBUILD_SHARED_LIBS=OFF -DTBB_TEST=OFF
cmake --build . --parallel $(nproc)
