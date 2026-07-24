#!/bin/bash

./configure --prefix=$SDE_INSTALL --with-p4c=bf-p4c --enable-thrift --with-tofino P4_NAME=sequencing_only P4_PATH=/root/pringles/code/p4/pktgen_tests/pktgen.p4 P4_VERSION=p4-16 P4_ARCHITECTURE=tna
make compile
