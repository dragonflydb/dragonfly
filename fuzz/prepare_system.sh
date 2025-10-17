#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#
# prepare_system.sh — Helper script to configure the host OS for optimal AFL++ performance
# Performs the same steps as documented in README (System Preparation for Testing)
# 1) Sets /proc/sys/kernel/core_pattern to "core" so AFL++ can obtain core dumps.
# 2) Forces all CPU frequency governors to "performance" to reduce fuzzing jitter.
#
# Usage:
#   sudo ./scripts/prepare_system.sh
#
# The script will attempt to re-execute itself under sudo if not already root.
# For Linux only.
set -euo pipefail

# Re-exec with sudo if not root
if [[ $(id -u) -ne 0 ]]; then
    echo "[prepare_system] This script must run as root. Attempting to use sudo…"
    exec sudo "$0" "$@"
fi

echo "[prepare_system] Setting core_pattern to 'core'…"
echo core > /proc/sys/kernel/core_pattern

echo "[prepare_system] Switching CPU governors to 'performance'…"
cd /sys/devices/system/cpu
echo performance | tee cpu*/cpufreq/scaling_governor
echo "[prepare_system] System preparation complete."
