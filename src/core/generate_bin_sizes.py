#!/usr/bin/env python3

import argparse
import random 
from array import array

# We print in 64 bit words.
ALIGN = 1 << 10  # 1KB alignment


def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-n', type=int, dest='num', 
                        help='number of quadruplets', default=9)

    args = parser.parse_args()

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