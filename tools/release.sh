#!/bin/sh

set -e

if ! [ -d helio ]; then
   echo "could not find helio"
   exit 1
fi

ARCH=`uname -m`
pwd
./helio/blaze.sh -release -DBoost_USE_STATIC_LIBS=ON -DOPENSSL_USE_STATIC_LIBS=ON
cd build-opt 
ninja dragonfly && ldd dragonfly
mv dragonfly dragonfly-${ARCH}