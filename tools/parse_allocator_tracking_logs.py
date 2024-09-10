#!/usr/bin/env python3

"""
Usage:
1. First run Dragonfly with tracking allocator enabled. Must be a single allocator range with 100% samping rate to catch both allocations and deallocations.
2. Finish tracking.
3. cat /tmp/dragonfly.INFO |  ./parse_allocator_tracking_logs.py
"""
import re
import sys


def parse_log(log_lines):
    memory_map = {}

    allocation_pattern = re.compile(r"Allocating (\d+) bytes \((0x[0-9a-f]+)\)")
    deallocation_pattern = re.compile(r"Deallocating (\d+) bytes \((0x[0-9a-f]+)\)")

    for line in log_lines:
        allocation_match = allocation_pattern.search(line)
        deallocation_match = deallocation_pattern.search(line)

        if allocation_match:
            size = int(allocation_match.group(1))
            address = allocation_match.group(2)
            assert address not in memory_map
            memory_map[address] = (size, line)
        elif deallocation_match:
            size = int(deallocation_match.group(1))
            address = deallocation_match.group(2)
            if address in memory_map:
                assert size == memory_map[address][0]
                del memory_map[address]
            else:
                print(f"Deallocating non existing address: {address} {size}")

    return memory_map


if __name__ == "__main__":
    log_lines = sys.stdin.readlines()
    memory_map = parse_log(log_lines)

    for address, item in memory_map.items():
        print(f"Address: {address}, Size: {item[0]} bytes, original line: `{item[1]}`")
