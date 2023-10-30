#!/usr/bin/env python3

"""
This script implements facilities for assessing cache eviction.
Two major functions have been implemented that allow users to
  1. Populate Dragonfly with a specified key and value length distributions.
  2. Measuring the cache hit rate of Dragonfly with workloads that access keys using Zipfian distribution.

Usage:
To perform database population, simply run:

./fill_db.py -f

This will automatically populate the database to the point where about 2X of maxmemory (specified by Dragonfly)
of KV pairs will be inserted. By default, we always stop at 2X maxmemory, and this can be changed using the -r
option, for instance

./fill_db.py -f -r 0.25  # population stops at 4x maxmemory

To accelerate the population, we can use multiple processes running this script in parallel. A convenient script
has been provided in this directory:
./run_fill_db.sh 10  # use 10 processes to fill in parallel

After database has been populated, we can start measuring cache hit rate using the -m option:
./fill_db.py -m
Note that the measurement must be done after the population as this mode relies on reading back the complete key
space inserted during the population phase. By default, we perform 100000 set operations for calculating cache hit rate.
This number can be changed using the -c option:
./fill_db.py -m -c 2000
"""


import redis
import string
from random import choice
from random import shuffle
import numpy as np

import asyncio
from redis import asyncio as aioredis
import os
import argparse
import re
import glob

all_val_lens = [400, 800, 1600, 25000]
val_lens_probs = [0.003, 0.78, 0.2, 0.017]

all_key_lens = [35, 60, 70]
key_lens_probs = [0.2, 0.06, 0.74]

inserted_keys = []


def random_str(len):
    return "".join(
        choice(string.ascii_letters + string.digits + string.punctuation) for i in range(len)
    )


def random_key():
    global all_key_lens, key_lens_probs
    return random_str(np.random.choice(all_key_lens, p=key_lens_probs))


def random_val():
    global all_val_lens, val_lens_probs
    return random_str(np.random.choice(all_val_lens, p=val_lens_probs))


def flush_keys_to_file(file_name):
    global inserted_keys
    with open(file_name, "a") as f:
        for key in inserted_keys:
            f.write(f"{key}\n")


def read_keys_from_file(file_name):
    global inserted_keys
    with open(file_name) as file:
        for line in file:
            inserted_keys.append(line.rstrip())


def read_keys():
    global inserted_keys
    inserted_keys.clear()
    key_files = glob.glob("./keys_*.txt")
    for key_file in key_files:
        read_keys_from_file(key_file)


def sync_populate_db():
    r = redis.Redis(decode_responses=True)
    n = 0
    while True:
        r.set(random_key(), random_val())
        n += 1
        if n % 1000 == 0:
            print("\r>> Number of key-value pairs inserted: {}".format(n), end="")


def sync_query_db():
    global inserted_keys
    r = redis.Redis(decode_responses=True)
    n = 0
    read_keys()
    misses = 0
    hits = 0
    for key in inserted_keys:
        resp = r.set(key, random_val(), nx=True)
        # print(resp)
        if resp:
            misses += 1
        else:
            hits += 1
        n += 1
        if n % 1000 == 0:
            print(
                "\r>> Number of key-value pairs inserted: {0}, hit: {1}, miss: {2}".format(
                    n, hits, misses
                ),
                end="",
            )


async def populate_db(ratio):
    global inserted_keys
    r = aioredis.Redis(decode_responses=True)
    n = 0
    misses = 0
    hits = 0

    total_key_count = 0
    while True:
        # await r.set(random_key(), random_val())
        pipeline = r.pipeline(False)
        for x in range(200):
            k = random_key()
            inserted_keys.append(k)
            pipeline.set(k, random_val())
            # pipeline.set(k, random_val(), nx=True)
        await pipeline.execute()
        # responses = await pipeline.execute()
        # for resp in responses:
        #    if resp:
        #        misses += 1
        #    else:
        #        hits += 1

        # key file names are in keys_xxxx.txt format
        key_file_name = "keys_" + str(os.getpid()) + ".txt"
        flush_keys_to_file(key_file_name)
        inserted_keys.clear()
        n += 200

        if total_key_count == 0:
            db_info = await r.info()
            used_mem = float(db_info["used_memory"])
            max_mem = float(db_info["maxmemory"])
            redline = 0.9
            # we will know the total number of keys of the whole space
            # only when we approach the maxmemory of the db
            if used_mem >= max_mem * redline:
                total_key_count = int(float(n) / ratio)
                print(
                    "\n>> Determined target key count: {0}, current key count: {1}, ratio: {2}".format(
                        total_key_count, n, ratio
                    ),
                    end="",
                )
        else:
            if n >= total_key_count:
                print("\n>> Target number of keys reached: {}, stopping...".format(n), end="")
                break
        if n % 1000 == 0:
            print("\r>> Number of key-value pairs inserted: {0}".format(n), end="")
            # print("\r>> Number of key-value pairs inserted: {0}, hit: {1}, miss: {2}".format(n, hits, misses), end='')


def rand_zipf_generator(alpha: float, upper: int, batch: int):
    """
    n: The upper bound of the values to generate a zipfian distribution over
    (n = 30 would generate a distribution of given alpha from values 1 to 30)
    alpha: The alpha parameter to be used while creating the Zipfian distribution
    num_samples: The total number of samples to generate over the Zipfian distribution
    This is a generator that yields up to count values using a generator.
    """

    # Calculate Zeta values from 1 to n:
    tmp = np.power(np.arange(1, upper + 1), -alpha)
    zeta = np.r_[0.0, np.cumsum(tmp)]

    # Store the translation map:
    distMap = [x / zeta[-1] for x in zeta]

    while True:
        # Generate an array of uniform 0-1 pseudo-random values:
        u = np.random.random(batch)

        # bisect them with distMap
        v = np.searchsorted(distMap, u)

        samples = [t - 1 for t in v]
        yield samples


def rearrange_keys():
    """
    This function potentially provides the capability for testing different caching workloads.
    for instance, if we rearrange all the keys via sorting based on the k-v memory usage,
    we will generate a zipfian hotspot that prefers to access small kv pairs (or larger kv pairs)
    current implementation just uses a random shuffle.
    """
    global inserted_keys
    shuffle(inserted_keys)


async def query_db_with_locality(count):
    global inserted_keys
    r = aioredis.Redis(decode_responses=True)
    n = 0
    read_keys()
    rearrange_keys()
    misses = 0
    hits = 0
    pipeline_size = 200
    key_index_gen = rand_zipf_generator(1.0, len(inserted_keys), pipeline_size)
    for key_indices in key_index_gen:
        pipeline = r.pipeline(False)
        # print(key_indices)
        for key_index in key_indices:
            k = inserted_keys[key_index]
            pipeline.set(k, random_val(), nx=True)

        responses = await pipeline.execute()
        n += pipeline_size
        for resp in responses:
            if resp:
                misses += 1
            else:
                hits += 1
        print(
            "\r>> Number of ops: {0}, hit: {1}, miss: {2}, hit rate: {3:.4f}".format(
                n, hits, misses, float(hits) / float(hits + misses)
            ),
            end="",
        )
        if n >= count:
            break
    hit_rate = float(hits) / float(hits + misses)
    print("\n>> Cache hit rate: {:.4f}".format(hit_rate))


class Range(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __eq__(self, other):
        return self.start <= other <= self.end


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Cache Benchmark", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-f",
        "--fill",
        action="store_true",
        help="fill database with random key-value pairs with their lengths follow some distributions",
    )

    parser.add_argument(
        "-r",
        "--ratio",
        type=float,
        default=0.5,
        choices=[Range(0.0, 1.0)],
        help="the ratio between in memory data size and total data size",
    )

    parser.add_argument(
        "-m",
        "--measure",
        action="store_true",
        help="measure cache hit rate by visiting the entire key space with a Zipfian distribution",
    )

    parser.add_argument(
        "-c",
        "--count",
        type=int,
        default=100000,
        help="total number of operations to be performed when measuring cache hit rate",
    )

    args = parser.parse_args()

    if args.fill:
        asyncio.run(populate_db(args.ratio))
        exit(0)

    if args.measure:
        asyncio.run(query_db_with_locality(args.count))
