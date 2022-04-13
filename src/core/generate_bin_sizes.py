#!/usr/bin/env python3

import argparse
import random 
from array import array

def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-n', type=int, dest='num', 
                        help='number of numbers', default=9)

    args = parser.parse_args()

    size = 512
    print ('{512, ', end=' ')
    for i in range(args.num):
        incr = size // 4
        for j in range(4):
            print (f'{size}, ', end=' ')
            size += incr
        if i % 2 == 1:
            print('')
    print('};')

if __name__ == "__main__":
    main()