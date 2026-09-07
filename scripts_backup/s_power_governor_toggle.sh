#!/bin/bash
# Description: Toggles or explicitly sets the CPU scaling governor between 'performance' and 'powersave', or displays current governors across all CPUs.
# Usage: ./governor.sh {perf|save|toggle|show}

set -eo pipefail

if [ $# -ne 1 ]; then
  echo "Error: Missing argument." >&2
  echo "Usage: $0 {perf|save|toggle|show}" >&2
  exit 1
fi

CURRENT_GOV=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "unknown")

case "$1" in
  perf)
    TARGET_GOV="performance"
    ;;
  save)
    TARGET_GOV="powersave"
    ;;
  toggle)
    if [ "$CURRENT_GOV" = "powersave" ]; then
      TARGET_GOV="performance"
    else
      TARGET_GOV="powersave"
    fi
    ;;
  show)
    for path in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
      printf '%s=' "$path"
      cat "$path"
    done
    exit 0
    ;;
  *)
    echo "Error: Invalid argument '$1'." >&2
    echo "Usage: $0 {perf|save|toggle|show}" >&2
    exit 1
    ;;
esac

# Check for root privileges before executing state-changing commands
if [ "$EUID" -ne 0 ]; then
  echo "Error: Root privileges required to change CPU governor. Please run with 'sudo'." >&2
  exit 1
fi

if cpupower frequency-set -g "$TARGET_GOV" >/dev/null 2>&1; then
  echo "Success: $CURRENT_GOV -> $TARGET_GOV"
else
  echo "Failure: 'cpupower' failed to set governor to $TARGET_GOV" >&2
  exit 1
fi
