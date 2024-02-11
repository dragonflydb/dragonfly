#!/bin/bash

containerWorkspaceFolder=$1
git config --global --add safe.directory ${containerWorkspaceFolder}/helio
mkdir -p /root/.local/share/CMakeTools
cp ${containerWorkspaceFolder}/.devcontainer/ubuntu20/cmake-tools-kits.json /root/.local/share/CMakeTools/
