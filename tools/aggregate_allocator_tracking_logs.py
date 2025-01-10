#!/usr/bin/env python3

"""
Usage:
1. First run Dragonfly with tracking allocator enabled.
2. Finish tracking.
3. cat /tmp/dragonfly.INFO |  ./aggregate_allocator_tracking_logs.py
"""

import sys


def aggregate(log_lines):
    alloc_sum = 0
    dealloc_sum = 0

    for line in log_lines:
        allocating = False
        deallocating = False

        for word in line.rstrip().split():
            if allocating:
                alloc_sum = alloc_sum + int(word)
                break
            elif deallocating:
                dealloc_sum = dealloc_sum + int(word)
                break

            if word == "Allocating":
                allocating = True
            if word == "Deallocating":
                deallocating = True

    print(f"Total allocations: {alloc_sum}\nTotal deallocations {dealloc_sum}")


if __name__ == "__main__":
    log_lines = sys.stdin.readlines()
    aggregate(log_lines)
