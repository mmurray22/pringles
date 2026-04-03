#!/bin/bash

### Download ALL the packages ####
function download_pkgs {
    curl -O https://files.pythonhosted.org/packages/b2/40/4e00501c204b457f10fe410da0c97537214b2265247bc9a5bc6edd55b9e4/setuptools-44.1.1.zip
    curl -O https://files.pythonhosted.org/packages/1b/90/f531329e628ff34aee79b0b9523196eb7b5b6b398f112bb0c03b24ab1973/protobuf-3.6.1.tar.gz
    curl -O https://files.pythonhosted.org/packages/11/c4/2da1f4952ba476677a42f25cd32ab8aaf0e1c0d0e00b89822b835c7e654c/enum34-1.1.10.tar.gz
    curl -O https://files.pythonhosted.org/packages/c0/12/927b89a24dcb336e5af18a8fbf581581c36e9620ae963a693a2522b2d340/futures-2.2.0.tar.gz
    curl -O https://files.pythonhosted.org/packages/98/df/e181e36dc54fc0166d59cf2cb25991e33df52090922495175b2e2abc1381/grpcio-1.17.1.tar.gz
    curl -O https://files.pythonhosted.org/packages/31/8f/53d4140a5100ce21fef6294ce06be82aa5b7942be27355e532343901eb57/Tenjin-1.1.1.tar.gz
    curl -O https://files.pythonhosted.org/packages/c0/6c/9f840c2e55b67b90745af06a540964b73589256cb10cc10057c87ac78fc2/wheel-0.37.1.tar.gz
    curl -O https://files.pythonhosted.org/packages/9d/0d/197f4a023269b5018054c5c2def0dd33b8dee04cdab6c60654182d2b0fbb/ctypesgen-1.0.2-py2.py3-none-any.whl
    wget http://ftp.debian.org/debian/pool/main/libn/libnl3/libnl-3-200_3.4.0-1+b1_amd64.deb
    wget http://ftp.debian.org/debian/pool/main/libn/libnl3/libnl-genl-3-200_3.4.0-1+b1_amd64.deb
    wget http://ftp.debian.org/debian/pool/main/libn/libnl3/libnl-3-dev_3.4.0-1+b1_amd64.deb
    wget http://ftp.debian.org/debian/pool/main/libn/libnl3/libnl-genl-3-dev_3.4.0-1+b1_amd64.deb
}

function scp_all_packages {
    scp go1.20.1.linux-amd64.tar.gz root@$1:~
    scp grpcio-1.17.1.tar.gz root@$1:~
    scp pkg-config-0.29.tar.gz root@$1:~
    scp protobuf-3.6.1.tar.gz root@$1:~
    scp setuptools-44.1.1.zip root@$1:~
    scp thrift-0.19.0.tar.gz root@$1:~
    scp futures-2.2.0.tar.gz root@$1:~
    scp enum34-1.1.10.tar.gz root@$1:~
    scp Tenjin-1.1.1.tar.gz root@$1:~
    scp ctypesgen-1.0.2-py2.py3-none-any.whl root@$1:~
    scp wheel-0.37.1.tar.gz root@$1:~
    scp cmake-3.7.2.tar.gz root@$1:~
    scp -r bf-sde root@$1:~
    scp -r root@$2:~/grpc root@$1:~
    scp -r root@$2:/usr/share/onl/packages/amd64/onl-kernel-4.14-lts-x86-64-all root@$1:/usr/share/onl/packages/amd64/
    scp libnl-3-dev_3.4.0-1+b1_amd64.deb root@10.229.49.X:~
    scp libnl-3-200_3.4.0-1+b1_amd64.deb  root@10.229.49.X:~
    scp libnl-genl-3-200_3.4.0-1+b1_amd64.deb  root@10.229.49.X:~
    scp libnl-genl-3-dev_3.4.0-1+b1_amd64.deb root@10.229.49.X:~
}

#### Install Go ####
function install_go {
    tar -C /usr/local -xzf go1.20.1.linux-amd64.tar.gz
    echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile
    source /etc/profile
}

#### Install pkg-config ####
function install_pkg_config {
    tar -xvf pkg-config-0.29.tar.gz
    cd pkg-config-0.29
    ./configure --with-internal-glib --prefix=/usr/local CFLAGS="-Wno-error=format-nonliteral -g -O2"
    make
    make install
    cd ..
}

#### Install Cmake ####
function install_cmake {
    tar -xvf cmake-3.7.2.tar.gz
    cd cmake-3.7.2
    ./bootstrap --prefix=/usr/local
    make
    make install
    cd ..
}

#### Install Thrift libraries ####
function install_thrift {
    tar -xzf thrift-0.19.0.tar.gz
    cd thrift-0.19.0
    export ACLOCAL_PATH=/usr/local/share/aclocal
    ./bootstrap.sh
    ./configure --with-cpp --without-c-glib --with-python --without-java --without-ruby
    echo "THIS IS GOING TO ERROR! NEED TO ALTER FILES IN THRIFT"
    make -j$(nproc)
    make install
}

#### Install Protobuf libraries ####
function install_protobuf {
    # Download the github repo and go to branch v3.6.1
    cd protobuf
    # 2. Prepare the build system
    ./autogen.sh
    # 3. Configure (Match your SDE path)
    ./configure
    # 4. Compile and Install
    make -j$(nproc)
    make install
    ldconfig 
}

#### Install GRPC libraries ####
function install_grpc {
    cd grpc/cmake/build
    cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DBUILD_SHARED_LIBS=ON ../..
    make -j$(nproc)
    make install
    cd ../..
    export LIB_PATH=/root/grpc/cmake/build
    echo "$LIB_PATH" | tee /etc/ld.so.conf.d/grpc.conf
    ldconfig
}

#### Install Python libraries ####
function install_python_libs {
    # Six
    
    # Setuptools - DONE
    unzip setuptools-44.1.1.zip -d setuptools-44.1.1
    cd setuptools-44.1.1
    python2.7 setup.py install
    python3 setup.py install
    
    # Protobuf - DONE
    tar -xvf protobuf-3.6.1.tar.gz
    cd protobuf-3.6.1
    export LIBRARY_PATH=/usr/local/lib
    export LIBRARY_PATH=/usr/local/lib
    python2.7 setup.py build --cpp_implementation
    python2.7 setup.py install --cpp_implementation
    
    # Enum34 - DONE
    tar -xvf enum34-1.1.10.tar.gz
    cd enum34-1.1.10
    python2.7 setup.py install
    
    # Futures - DONE
    tar -xvf futures-2.2.0.tar.gz
    cd futures-2.2.0
    python2.7 setup.py install

    # Tenjin - DONE
    tar -xvf Tenjin-1.1.1.tar.gz
    cd Tenjin-1.1.1
    python2.7 setup.py install
    python3 setup.py install

    # wheel - DONE
    tar -xvf wheel-0.37.1.tar.gz
    cd wheel-0.37.1
    echo "NEED TO UPDATE THE MAINTAINER NAME!!"
    python2.7 -c "import sys; reload(sys); sys.setdefaultencoding('utf-8'); import setup; setup.setup(maintainer='Alex')" install
    python3 -c "import sys; reload(sys); sys.setdefaultencoding('utf-8'); import setup; setup.setup(maintainer='Alex')" install

    # ctypesgen - DONE
    pip install ctypesgen-1.0.2-py2.py3-none-any.whl --no-index --find-links .
    pip3 install ctypesgen-1.0.2-py2.py3-none-any.whl --no-index --find-links .
    
    # Grpcio - DONE
    tar -xvf grpcio-1.17.1.tar.gz
    cd grpcio-1.17.1
    export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=0
    export GRPC_PYTHON_BUILD_SYSTEM_ZLIB=0
    export GRPC_PYTHON_BUILD_SYSTEM_CARES=0
    python2.7 setup.py build
    python2.7 setup.py install
    
    # Parsing - DONE
    tar -xvf pyparsing-2.0.2.tar.gz
    cd pyparsing-2.0.2
    python2.7 setup.py install
    python3 setup.py install

    # Packaging - DONE
    tar -xvf packaging-20.9.tar.gz
    cd packaging-20.9
    python2.7 setup.py install
    python3 setup.py install
}

#### Install Barefoot SDE ####
function install_bf_sys {
    cd $SDE/bf-syslibs-9.4.0
    ./configure --prefix=$SDE_INSTALL
    mpython2.7 setup.py installake
    make install
}

function install_bf_utils {
    cd $SDE/bf-utils-9.4.0
    ./configure --prefix=$SDE_INSTALL
    make
    make install
}

function install_bf_drivers {
    ln -s /usr/share/onl/packages/amd64/onl-kernel-4.14-lts-x86-64-all/mbuilds/ /lib/modules/4.14.151-OpenNetworkLinux/build
    cd $SDE/bf-drivers-9.4.0
    export PATH=/root/grpc/cmake/build:$PATH
    cd kdrv/bf_kpkt
    insmod bf_kpkt.ko kpkt_mode=1
    ip link set dev enp5s0 up
    ip link set dev enp5s0 promisc on
    ip link show enp5s0
    cd ../..
    ln -s /root/grpc/cmake/build/libgrpc.so /usr/local/lib/libgrpc.so
    ln -s /root/grpc/cmake/build/libgrpc++.so /usr/local/lib/libgrpc++.so
    cp grpc.pc /usr/local/lib/pkgconfig
    cp grpc++.pc /usr/local/lib/pkgconfig
    cp gpr.pc /usr/local/lib/pkgconfig
    cp openssl.pc /usr/local/lib/pkgconfig
    ln -s /root/grpc/cmake/build/grpc_cpp_plugin /usr/local/bin/grpc_cpp_plugin
    export PKG_CONFIG_PATH=/root/bf-sde-9.4.0/bf-drivers-9.4.0:$PKG_CONFIG_PATH
    cp /root/grpc/cmake/build/libgrpc++.so /usr/local/lib/
    cp /root/grpc/cmake/build/libgrpc.so /usr/local/lib/
    ./configure --prefix=$SDE_INSTALL --enable-thrift --enable-grpc
    make
    make install
}

function install_switch_p4 {
    dpkg -i libnl-3-200_3.4.0-1+b1_amd64.deb
    dpkg -i libnl-genl-3-200_3.4.0-1+b1_amd64.deb
    dpkg -i libnl-3-dev_3.4.0-1+b1_amd64.deb
    dpkg -i libnl-genl-3-dev_3.4.0-1+b1_amd64.deb
    cd $SDE/switch-p4-16-9.4.0
    ./autogen.sh
    ./configure --prefix=$SDE_INSTALL --enable-thrift
    make x1_tofino
    make install
}

function install_bf_platforms {
    cd $SDE/bf-reference-bsp-9.4.0
    #tar -xzf bf-platforms-9.4.0.tar.gz
    #./extract_all.sh
    # Exports done below
    cd bf-platforms-9.4.0
    ./configure --prefix=$BSP_INSTALL --enable-thrift
    # Make sure constants cpp files are added
    make
    make install
}

function install_ptf_modules {
    ./configure --prefix=$SDE_INSTALL
    make
    make install
}


# Add exports to bashrc
echo 'export SDE=/root/bf-sde-9.4.0' >> ~/.bashrc
echo 'export SDE_INSTALL=/root/bf-sde-9.4.0/install' >> ~/.bashrc
echo 'export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH}' >> ~/.bashrc
echo 'export PATH=$SDE_INSTALL/bin:$PATH' >> ~/.bashrc
echo 'export BSP=$SDE/bf-reference-bsp-9.4.0' >> ~/.bashrc
echo 'export BSP_INSTALL=$SDE_INSTALL' >> ~/.bashrc
source ~/.bashrc

# Run all the install functions
install_go # DONE
install_pkg_config  # DONE
install_cmake # DONE
install_thrift # DONE
install_protobuf # DONE
install_grpc # DONE
install_python_libs # DONE

# Install bf SDE
install_bf_sys
install_bf_utils
install_bf_drivers
install_switch_p4
install_bf_platforms
install_ptf_modules

# NEED the virtual interfaces
$SDE_INSTALL/bin/veth_setup.sh
