#!/usr/bin/env sh

APP_PATH=build-opt/dragonfly

set -e

if ! [ -f "helio/blaze.sh" ]; then
   echo "ERROR"
   echo "Could not find helio. Please only run this script from repo root."
   echo "If you are already on the repo root, you might've cloned without submodules."
   echo "Try running 'git submodule update --init --recursive'"
   exit 1
fi

pwd

make HELIO_RELEASE=y release

if ! [ -f ${APP_PATH} ]; then
   echo "ERROR"
   echo "Failed to generate new dragonfly binary."
   exit 1
fi

${APP_PATH} --version

if readelf -a ${APP_PATH} | grep GLIBC_PRIVATE >/dev/null 2>&1 ; then
   echo "ERROR"
   echo "The generated binary contain invalid GLIBC version entries."
   exit 1
fi

make package
