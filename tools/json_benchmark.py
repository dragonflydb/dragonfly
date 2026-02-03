#!/usr/bin/env python

import multiprocessing
import time
import redis
import sys
import argparse
from urllib.parse import urlparse
import os
from collections import defaultdict
import math

'''
Run JSON benchmark for 3 commands:
    JSON.SET
    JSON.GET
    JSON.TYPE
We want to the overall time it takes
to save and access keys that contains
JSON values with this benchmark.
This also verify that the basic functionalities
for using JSON types work correctly
'''

def ping(r):
    r.ping()

def jsonset(r, i):
    key = "json-{}".format(i)
    r.execute_command('JSON.SET', key, '.', '{"a":123456, "b": "hello", "nested": {"abc": "ffffff", "bfb": null}}')


def jsonget(r, i):
    key = "json-{}".format(i)
    r.execute_command('JSON.GET', key, '$.a', '$..abc')

def jsontype(r, i):
    key = "json-{}".format(i)
    r.execute_command('JSON.TYPE', key, '$.a')

def runWorker(ctx):
    wpid = os.getpid()
    print( '{} '.format(wpid))

    rep = defaultdict(int)
    r = redis.StrictRedis(host=ctx['host'], port=ctx['port'])
    work = ctx['work']
    if ctx['pipeline'] == 0:
        total_count = int(ctx['count'])
        for i in range(0, total_count):
            s0 = time.time()
            jsonset(r, i)
            s1 = time.time() - s0
            bin = int(math.floor(s1 * 1000)) + 1
            rep[bin] += 1
        for i in range(0, total_count):
            s0 = time.time()
            jsonget(r, i)
            s1 = time.time() - s0
            bin = int(math.floor(s1 * 1000)) + 1
            rep[bin] += 1
        for i in range(0, total_count):
            s0 = time.time()
            jsontype(r, i)
            s1 = time.time() - s0
            bin = int(math.floor(s1 * 1000)) + 1
            rep[bin] += 1
    else:
        for i in range(0, ctx['count'], ctx['pipeline']):
            p = r.pipeline()
            s0 = time.time()
            for j in range(0, ctx['pipeline']):
                work(p)
            p.execute()
            s1 = time.time() - s0
            bin = int(math.floor(s1 * 1000)) + 1
            rep[bin] += ctx['pipeline']

    return rep

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ReJSON Benchmark', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c', '--count', type=int, default=100000, help='total number of operations')
    parser.add_argument('-p', '--pipeline', type=int, default=0, help='pipeline size')
    parser.add_argument('-w', '--workers', type=int, default=8, help='number of worker processes')
    parser.add_argument('-u', '--uri', type=str, default='redis://localhost:6379', help='Redis server URI')
    args = parser.parse_args()
    uri = urlparse(args.uri)

    r = redis.Redis(host=uri.hostname, port=uri.port)

    pool = multiprocessing.Pool(args.workers)
    s0 = time.time()
    ctx = {
        'count': args.count / args.workers,
        'pipeline': args.pipeline,
        'host': uri.hostname,
        'port': uri.port,
        'work': jsonset,
    }

    print ('Starting workers: ')
    p = multiprocessing.Pool(args.workers)
    results = p.map(runWorker, (ctx, ) * args.workers)
    print("")
    sys.stdout.flush()

    s1 = time.time() - s0
    agg = defaultdict(int)
    for res in results:
        for k, v in res.items():
            agg[k] += v

    print()
    count = args.count * 3
    print (f'Count: {args.count}, Workers: {args.workers}, Pipeline: {args.pipeline}')
    print (f'Using hireds: {redis.utils.HIREDIS_AVAILABLE}')
    print (f'Runtime: {round(s1, 2):,} seconds')
    print (f'Throughput: {round(count/s1, 2):,} requests per second')
    for k, v in sorted(agg.items()):
        perc = 100.0 * v / count
        print (f'{perc:.4f}% <= {k:,} milliseconds')
