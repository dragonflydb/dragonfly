#!/usr/bin/env python3

"""Stress Dragonfly's memory to test memory tracking and behavior."""

import asyncio
import aioredis
import argparse
import base64
import random
import os


def list_to_dict(l):
    return {l[i]: l[i + 1] for i in range(0, len(l), 2)}


# Taken from https://stackoverflow.com/questions/12523586 with slight modifications
def humanbytes(b_orig):
    """Return the given bytes as a human friendly KB, MB, GB, or TB string."""
    B = float(abs(b_orig))
    KB = float(1024)
    MB = float(KB**2)  # 1,048,576
    GB = float(KB**3)  # 1,073,741,824
    TB = float(KB**4)  # 1,099,511,627,776
    prefix = "-" if b_orig < 0 else ""

    if B < KB:
        return "{}b".format(b_orig)
    elif KB <= B < MB:
        return "{}{:.2f}kb".format(prefix, B / KB)
    elif MB <= B < GB:
        return "{}{:.2f}mb".format(prefix, B / MB)
    elif GB <= B < TB:
        return "{}{:.2f}gb".format(prefix, B / GB)
    elif TB <= B:
        return "{}{:.2f}tb".format(prefix, B / TB)


def get_fraction_str(part, total):
    return "{} ({:.1%})".format(humanbytes(part), part / total)


async def print_mem(args):
    client = aioredis.Redis(decode_responses=True, host="localhost", port=6379)
    stats = await client.execute_command("memory", "stats")
    stats = list_to_dict(stats)
    rss = stats["rss_bytes"]
    data = stats["data_bytes"]
    conn = stats["connections.total_bytes"]
    replication = stats["replication.total_bytes"]
    serialization = stats["serialization"]
    accounted = data + conn + replication + serialization
    print("RSS: " + get_fraction_str(rss, rss))
    print("Accounted: " + get_fraction_str(accounted, rss))
    print("Unaccounted: " + get_fraction_str(rss - accounted, rss))
    print("Breakdown:")
    print("- Data: " + get_fraction_str(data, rss))
    print("- Connections: " + get_fraction_str(conn, rss))
    print("- Replication: " + get_fraction_str(replication, rss))
    print("- Serialization: " + get_fraction_str(serialization, rss))


async def print_timer(args):
    def kvstr(name, v):
        return f"{name}={humanbytes(v)}"

    client = aioredis.Redis(decode_responses=True, host="localhost", port=6379)

    while True:
        stats = await client.execute_command("memory", "stats")
        stats = list_to_dict(stats)
        print_stats = {}
        print_stats["rss"] = stats["rss_bytes"]
        print_stats["data"] = stats["data_bytes"]
        print_stats["conn"] = stats["connections.total_bytes"]
        print_stats["replication"] = stats["replication.total_bytes"]
        print_stats["serialization"] = stats["serialization"]
        print_stats["unaccounted"] = print_stats["rss"] - (
            print_stats["data"]
            + print_stats["conn"]
            + print_stats["replication"]
            + print_stats["serialization"]
        )
        print(", ".join(kvstr(k, v) for k, v in print_stats.items()))
        await asyncio.sleep(1)


def random_str(n):
    return str(base64.encodebytes(os.urandom(n)), encoding="utf-8")[0:n]


def random_strs(n, m):
    return [random_str(n) for _ in range(m)]


async def insert(args):
    client = aioredis.Redis(decode_responses=True, host="localhost", port=6379)

    awaits = []
    for i in range(args.keys):
        key = f"{args.type}:{random_str(15)}"
        if args.type == "string":
            awaits.append(client.set(key, random_str(args.member_size)))
        elif args.type == "set":
            awaits.append(client.sadd(key, *random_strs(args.member_size, args.members)))
        elif args.type == "zset":
            scores = random.sample(range(0, 100_000), args.members)
            strs = random_strs(args.member_size, args.members)
            l = list(sum(zip(scores, strs), ()))
            awaits.append(client.execute_command("zadd", key, *l))
        else:
            print(f"Error: unsupported type {args.type}, supported types are string/set/zset")
            exit(-1)

        if len(awaits) >= args.pipeline_size:
            await asyncio.gather(*awaits)
            awaits.clear()

    await asyncio.gather(*awaits)


async def connections(args):
    clients = []
    print(f"Creating {args.keys} connections...")
    for i in range(args.keys):
        client = aioredis.Redis(decode_responses=True, host="localhost", port=6379)
        # By awaiting for a ping we actually initiate the connection, as otherwise we don't give
        # the async io CPU time to handle the connection.
        await client.ping()
        clients.append(client)

    print(f"Done. Sending random sets until killed.")
    while True:
        await random.choice(clients).set(random_str(10), random_str(10))


async def main():
    parser = argparse.ArgumentParser(description="Stress Dragonfly memory")
    parser.add_argument(
        "--action",
        default="print",
        help="Which action to take? print: print accounting, insert=inserts data",
    )
    parser.add_argument("--type", default="string")
    parser.add_argument("--keys", type=int, default="1")
    parser.add_argument("--members", type=int, default="1")
    parser.add_argument("--member-size", type=int, default="1")
    parser.add_argument("--pipeline-size", type=int, default="1")
    args = parser.parse_args()

    actions = {
        "print": print_mem,
        "insert": insert,
        "print_timer": print_timer,
        "connections": connections,
    }
    action = actions.get(args.action.lower())
    if action:
        await action(args)
    else:
        print(f'Error - unknown action "{args.action}"')
        exit(-1)


asyncio.run(main())
