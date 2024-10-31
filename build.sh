sudo apt install ninja-build libunwind-dev libboost-fiber-dev libssl-dev \
     autoconf-archive libtool cmake g++ libzstd-dev bison libxml2-dev

git clone --recursive https://github.com/dragonflydb/dragonfly && cd dragonfly

# Configure the build
./helio/blaze.sh -release

# Build
cd build-opt && ninja dragonfly
