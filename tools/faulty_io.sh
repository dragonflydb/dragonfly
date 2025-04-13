#!/bin/bash

# Utility script for creating block devices with fault injection to test tiering
#
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root"
   exit 1
fi

function finalize_block_dev {
    mke2fs /dev/mapper/$1
    mkdir -p /mnt/$1
    mount /dev/mapper/$1  /mnt/$1
    chmod o+rw /mnt/$1
}

function remove_block_dev {
     umount /dev/mapper/$1
    rm -rf /mnt/$1
}

if [[ "$1" == "create" ]]
then
    # Create backing file of 256MB
    dd if=/dev/zero of=./tiering_backing bs=1024 count=262144
    # Create loopback device
    DEV=$(losetup --find --show ./tiering_backing)

    # Create first block device with flaky sectors
    dmsetup create tiering_flaky << EOF
    0 20000 linear $DEV 0
    20000 105424 flakey $DEV 0 1 1
EOF
    finalize_block_dev tiering_flaky
elif [[ "$1" == "remove" ]]
then
    remove_block_dev tiering_flaky
    dmsetup remove_all
    losetup -a | grep tiering | awk -F ':' '{print $1}'| xargs losetup --detach
    rm ./tiering_backing
else
    echo """Devices created by this script:
    1. /mnt/tiering_flaky_1 - flaky device with 1:1 second success/error intervals

use with either create/remove arguments"""
fi
