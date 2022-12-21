#!/usr/bin/env python3

import argparse
import random
from array import array

# We print in 64 bit words.
ALIGN = 1 << 10  # 1KB alignment


def print_small_bins():
    prev_val = 0
    for i in range(56, 1, -1):
        len = (4096 - i*8)  # reduce by size of hashes
        len = (len // 8)*8  # make it 8 bytes aligned
        if len != prev_val:
            print(i, len)
            prev_val = len
    print()


def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-n', type=int, dest='num',
                        help='number of quadruplets', default=9)
    parser.add_argument('-small', action='store_true')

    args = parser.parse_args()
    if args.small:
        print("small")
        print_small_bins()
        return

    size = 512*4
    print ('{512, 512*2, 512*3, ', end=' ')
    # print ('{', end=' ')
    for i in range(args.num):
        incr = size // 4
        for j in range(4):
            assert size % 512 == 0, size
            print (f'{size}, ', end=' ')
            size += incr
        if i % 2 == 1:
            print('')
    print('};')

if __name__ == "__main__":
    main()
