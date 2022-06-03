#!/bin/sh

set -e

if ! [ -d helio ]; then
   echo "could not find helio"
   exit 1
fi

ARCH=`uname -m`
NAME="dragonfly-${ARCH}"

pwd
./helio/blaze.sh -release -DBoost_USE_STATIC_LIBS=ON -DOPENSSL_USE_STATIC_LIBS=ON \
          -DENABLE_GIT_VERSION=ON -DWITH_UNWIND=OFF

cd build-opt 
ninja dragonfly && ldd dragonfly
mv dragonfly $NAME
tar cvfz $NAME.unstripped.tar.gz $NAME ../LICENSE.md
strip $NAME
tar cvfz $NAME.tar.gz $NAME ../LICENSE.md
