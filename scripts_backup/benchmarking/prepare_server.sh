#!/bin/bash

#  Disable deep CPU idle states
sudo cpupower idle-set -D 1   # disable states deeper than ~1us

# Pin NIC interrupts to core 0
sudo systemctl stop irqbalance
for irq in $(grep -i ena /proc/interrupts | awk -F: '{print $1}'); do
  echo 0 | sudo tee /proc/irq/$irq/smp_affinity_list
done
