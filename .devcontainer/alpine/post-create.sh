#!/bin/bash

containerWorkspaceFolder=$1
git config --global --add safe.directory ${containerWorkspaceFolder}
git config --global --add safe.directory ${containerWorkspaceFolder}/helio
mkdir -p /root/.local/share/CMakeTools
