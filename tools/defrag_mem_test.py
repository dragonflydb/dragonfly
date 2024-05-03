#!/usr/bin/env python3
import asyncio
import aioredis
import async_timeout
import sys
import argparse

"""
To install: pip install -r requirements.txt

Run
dragonfly --mem_defrag_threshold=0.01 --mem_defrag_waste_threshold=0.01
defrag_mem_test.py -k 8000000 -v 645

This program would try to re-create the issue with memory defragmentation.
See issue number 448 for more details.
To run this:
    You can just execute this from the command line without any arguemnts.
    Or you can run with --help to see the options.
    The defaults are:
    number of keys: 800,000
    value size: 64 bytes
    key name pattern: key-for-testing
    host: localhost
    port: default redis port
    Please note that this would create 4 * number of keys entries
    You can see the memory usage/defrag state with the monitoring task that
    prints the current state

NOTE:
    If this seems to get stuck please kill it with ctrl+c
    This can happen in case we don't have "defrag_realloc_total > 0"
"""


class TaskCancel:
    def __init__(self):
        self.run = True

    def dont_stop(self):
        return self.run

    def stop(self):
        self.run = False


async def run_cmd(connection, cmd, sub_val):
    val = await connection.execute_command(cmd, sub_val)
    return val


async def handle_defrag_stats(connection, prev):
    info = await run_cmd(connection, "info", "stats")
    if info is not None:
        if info["defrag_task_invocation_total"] != prev:
            print("--------------------------------------------------------------")
            print(f"defrag_task_invocation_total: {info['defrag_task_invocation_total']:,}")
            print(f"defrag_realloc_total: {info['defrag_realloc_total']:,}")
            print(f"defrag_attempt_total: {info['defrag_attempt_total']:,}")
            print("--------------------------------------------------------------")
            if info["defrag_realloc_total"] > 0:
                return True, None
            return False, info["defrag_task_invocation_total"]
    return False, None


async def memory_stats(connection):
    print("--------------------------------------------------------------")
    info = await run_cmd(connection, "info", "memory")
    # print(f"memory commited: {info['comitted_memory']:,}")
    print(f"memory used: {info['used_memory']:,}")
    # print(f"memory usage ratio: {info['comitted_memory']/info['used_memory']:.2f}")
    print("--------------------------------------------------------------")


async def stats_check(connection, condition):
    try:
        defrag_task_invocation_total = 0
        runs = 0
        while condition.dont_stop():
            await asyncio.sleep(0.3)
            done, d = await handle_defrag_stats(connection, defrag_task_invocation_total)
            if done:
                print("defrag task successfully found memory locations to reallocate")
                condition.stop()
            else:
                if d is not None:
                    defrag_task_invocation_total = d
            runs += 1
            if runs % 3 == 0:
                await memory_stats(connection)
        for i in range(5):
            done, d = await handle_defrag_stats(connection, -1)
            if done:
                print("defrag task successfully found memory locations to reallocate")
                return True
            else:
                await asyncio.sleep(2)
        return True
    except Exception as e:
        print(f"failed to run monitor task: {e}")
    return False


async def delete_keys(connection, keys):
    results = await connection.delete(*keys)
    return results


def generate_keys(pattern: str, count: int, batch_size: int) -> list:
    for i in range(1, count, batch_size):
        batch = [f"{pattern}{j}" for j in range(i, batch_size + i, 3)]
        yield batch


async def mem_cleanup(connection, pattern, num, cond, keys_count):
    counter = 0
    for keys in generate_keys(pattern=pattern, count=keys_count, batch_size=950):
        if cond.dont_stop() == False:
            print(f"task number {num} that deleted keys {pattern} finished")
            return counter
        counter += await delete_keys(connection, keys)
        await asyncio.sleep(0.2)
    print(f"task number {num} that deleted keys {pattern} finished")
    return counter


async def run_tasks(pool, key_name, value_size, keys_count):
    keys = [f"{key_name}-{i}" for i in range(4)]
    stop_cond = TaskCancel()
    try:
        connection = aioredis.Redis(connection_pool=pool)
        for key in keys:
            print(f"creating key {key} with size {value_size} of count {keys_count}")
            await connection.execute_command("DEBUG", "POPULATE", keys_count, key, value_size)
            await asyncio.sleep(2)
        tasks = []
        count = 0
        for key in keys:
            pattern = f"{key}:"
            print(f"deleting keys from {pattern}")
            tasks.append(
                mem_cleanup(
                    connection=connection,
                    pattern=pattern,
                    num=count,
                    cond=stop_cond,
                    keys_count=int(keys_count),
                )
            )
            count += 1
        monitor_task = asyncio.create_task(stats_check(connection, stop_cond))
        total = await asyncio.gather(*tasks, return_exceptions=True)
        print(f"successfully deleted {sum(total)} keys")
        stop_cond.stop()
        await monitor_task
        print("finish executing")
        return True
    except Exception as e:
        print(f"got error {e} while running delete keys")
        return False


def connect_and_run(key_name, value_size, keys_count, host="localhost", port=6379):
    async_pool = aioredis.ConnectionPool(
        host=host, port=port, db=0, decode_responses=True, max_connections=16
    )

    loop = asyncio.new_event_loop()
    success = loop.run_until_complete(
        run_tasks(pool=async_pool, key_name=key_name, value_size=value_size, keys_count=keys_count)
    )
    return success


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="active memory testing", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-k", "--keys", type=int, default=800000, help="total number of keys")
    parser.add_argument("-v", "--value_size", type=int, default=645, help="size of the values")
    parser.add_argument(
        "-n", "--key_name", type=str, default="key-for-testing", help="the base key name"
    )
    parser.add_argument("-s", "--server", type=str, default="localhost", help="server host name")
    parser.add_argument("-p", "--port", type=int, default=6379, help="server port number")
    args = parser.parse_args()
    keys_num = args.keys
    key_name = args.key_name
    value_size = args.value_size
    host = args.server
    port = args.port
    print(
        f"running key deletion on {host}:{port} for keys {key_name} value size of {value_size} and number of keys {keys_num}"
    )
    result = connect_and_run(
        key_name=key_name, value_size=value_size, keys_count=keys_num, host=host, port=port
    )
    if result == True:
        print("finished successfully")
    else:
        print("failed")
