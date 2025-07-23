#!/usr/bin/env python3
# Measure difference of total metrics per second for every client
# client_list_per_second.py [host] [port]

import sys
import redis
import time


def build_table(entries: list[dict]) -> dict:
    return {e["id"]: e for e in entries}


def collect(entry: dict, scratch: dict, sign: int):
    for key in scratch:
        scratch[key] += int(entry[key]) * sign


r = redis.Redis(host=sys.argv[1], port=int(sys.argv[2]))

s1 = r.client_list()
time.sleep(1)
s2 = r.client_list()

t1 = build_table(s1)
t2 = build_table(s2)

base_scratch = {k: 0 for k in next(iter(t2.values())).keys() if k.startswith("total")}

for entry_id in t1:
    try:
        e1 = t1[entry_id]
        e2 = t2[entry_id]

        scratch = base_scratch.copy()
        collect(e2, scratch, 1)
        collect(e1, scratch, -1)

        print(scratch)
    except Exception as e:
        print(e)
        pass
