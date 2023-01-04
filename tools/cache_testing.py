#!/usr/bin/env python

import redis
import argparse
from urllib.parse import urlparse
import numpy as np

'''
Run Cache Testing.
This tool performs cache testing for Dragonfly
by calling the `incrby` function on a constrained set
of items, as defined by the user. Additionally, it
distributes the frequency of `incrby` calls for each
item based on a Zipfian distribution (with alpha values
between 0 and 1 being representative of real-life cache
load scenarios)
'''

def rand_zipf(n, alpha, numSamples):
    """
    n: The upper bound of the values to generate a zipfian distribution over
    (n = 30 would generate a distribution of given alpha from values 1 to 30)
    alpha: The alpha parameter to be used while creating the Zipfian distribution
    num_samples: The total number of samples to generate over the Zipfian distribution
    returns a list of num_samples values of items in the range [1, n] that follow a
    Zipfian distribution.
    """

    # Calculate Zeta values from 1 to n:
    tmp = np.power( np.arange(1, n+1), -alpha )
    zeta = np.r_[0.0, np.cumsum(tmp)]

    # Store the translation map:
    distMap = [x / zeta[-1] for x in zeta]

    # Generate an array of uniform 0-1 pseudo-random values:
    u = np.random.random(numSamples)

    # bisect them with distMap
    v = np.searchsorted(distMap, u)

    samples = [t-1 for t in v]
    return samples

def update_stats(r, initial_hits, initial_misses, value_index, total_count):
    """
    A void function that uses terminal control sequences to update hit/miss
    ratio stats for the user while the testing tool runs.
    """
    percent_complete = (value_index + 1) / total_count

    # Use the terminal control sequence to move the cursor to the beginning of the line
    print("\r", end="")

    current_info = r.info()
    current_hits = current_info["keyspace_hits"]
    current_misses = current_info["keyspace_misses"]

    # Print the loading bar and current hit rate
    print("[{}{}] {:.0f}%, current hit rate: {:.6f}%".format("#" * int(percent_complete * 20), " " * int(20 - percent_complete * 20), percent_complete * 100, (current_hits / (current_hits + current_misses)) * 100), end="")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Cache Benchmark', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c', '--count', type=int, default=100000, help='total number of incrby operations')
    parser.add_argument('-u', '--uri', type=str, default='redis://localhost:6379', help='Redis server URI')
    parser.add_argument('-a', '--alpha', type=int, default=1.0, help='alpha value being used for the Zipf distribution')
    parser.add_argument('-n', '--number', type=int, default=30, help='the number of values to be used in the distribution')
    args = parser.parse_args()
    uri = urlparse(args.uri)

    r = redis.Redis(host=uri.hostname, port=uri.port)

    initial_hits = r.info()['keyspace_hits']
    initial_misses = r.info()['keyspace_misses']

    distribution_values = rand_zipf(args.number, args.alpha, args.count)
    for idx, val in enumerate(distribution_values):
        r.incrby(str(val), 1)
        if idx % 50 == 0:
            update_stats(r, initial_hits, initial_misses, idx, args.count)
