#!/bin/sh

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

build-opt/dragonfly --version

make package
