#!/usr/bin/env python

import redis
import aioredis
import asyncio
import argparse
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


def rand_zipf_generator(alpha: float, upper: int, batch: int):
    """
    n: The upper bound of the values to generate a zipfian distribution over
    (n = 30 would generate a distribution of given alpha from values 1 to 30)
    alpha: The alpha parameter to be used while creating the Zipfian distribution
    num_samples: The total number of samples to generate over the Zipfian distribution
    This is a generator that yields up to count values using a generator.
    """

    # Calculate Zeta values from 1 to n:
    tmp = np.power(np.arange(1, upper+1), -alpha)
    zeta = np.r_[0.0, np.cumsum(tmp)]

    # Store the translation map:
    distMap = [x / zeta[-1] for x in zeta]

    while True:
        # Generate an array of uniform 0-1 pseudo-random values:
        u = np.random.random(batch)

        # bisect them with distMap
        v = np.searchsorted(distMap, u)

        samples = [t-1 for t in v]
        yield samples


def update_stats(hits, misses, value_index, total_count):
    """
    A void function that uses terminal control sequences
    to update hit/miss ratio stats for the user
    while the testing tool runs.
    """
    percent_complete = (value_index + 1) / total_count

    # Use the terminal control sequence to move the cursor to the beginning of the line
    print("\r", end="")

    # Print the loading bar and current hit rate
    print("[{}{}] {:.0f}%, current hit rate: {:.6f}%".format("#" * int(percent_complete * 20), " " *
          int(20 - percent_complete * 20), percent_complete * 100, (hits / (hits + misses)) * 100), end="")


async def run_single_conn(redis_client, keys_gen, args) -> None:
    misses = 0
    hits = 0
    val = 'x' * args.length
    items_sent = 0
    last_stat = 0
    for keys in keys_gen:
        if len(keys) == 1:
            result = await redis_client.set(str(keys[0]), val, nx=True)
            responses = [result]
        else:
            p = redis_client.pipeline(transaction=False)
            for key in keys:
                p.set(str(key), val, nx=True)
            responses = await p.execute()

        for resp in responses:
            if resp:
                misses += 1
            else:
                hits += 1
        items_sent += len(keys)
        if items_sent // 100 != last_stat:
            last_stat = items_sent // 100
            update_stats(hits, misses, items_sent, args.count)
        if items_sent >= args.count:
            break
    print()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Cache Benchmark', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c', '--count', type=int, default=100000,
                        help='total number of operations')
    parser.add_argument('-u', '--uri', type=str,
                        default='localhost:6379', help='Redis server URI')
    parser.add_argument('-a', '--alpha', type=float, default=1.0,
                        help='alpha value being used for the Zipf distribution')
    parser.add_argument('--upper_bound', type=int, default=1000,
                        help='the number of values to be used in the distribution')
    parser.add_argument('-d', '--length', type=int, default=10,
                        help='the length of the values to be used in the distribution')
    parser.add_argument('-p', '--pipeline', type=int,
                        default=1, help='pipeline size')
    parser.add_argument('-t', '--test', action='store_true')

    args = parser.parse_args()
    if args.test:
        for idx, items in enumerate(rand_zipf_generator(args.alpha, args.upper_bound, 1)):
            assert len(items) == 1
            print(items[0])
            if idx == args.count:
                break
        exit(0)

    r = aioredis.from_url(
        f"redis://{args.uri}", encoding="utf-8", decode_responses=True)

    distribution_keys_generator = rand_zipf_generator(
        args.alpha, args.upper_bound, args.pipeline)

    asyncio.run(run_single_conn(r, distribution_keys_generator, args))
