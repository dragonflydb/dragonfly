import asyncio
import aioredis
import random
import string
import itertools
import time
from collections import deque
from enum import Enum

from utility import grouper, batch_fill_data_async, gen_test_data


class Action(Enum):
    SHRINK = 0
    NO_CHANGE = 1
    GROW = 2


class ValueType(Enum):
    STRING = 0
    LIST = 1
    SET = 2


class TestingGenerator:
    def __init__(self, val_size, batch_size, target_keys, max_multikey):
        self.val_size = val_size
        self.batch_size = batch_size
        self.key_cnt_target = target_keys
        self.max_multikey = max_multikey

        self.key_sets = [set() for _ in ValueType]
        self.key_cursor = 0
        self.key_cnt = 0

        self.str_filler = ''.join(random.choices(
            string.ascii_letters, k=val_size))

        self.list_filler = ' '.join(
            random.choices(string.ascii_letters, k=val_size//2))

        self.set_filler = ' '.join(
            (''.join(random.choices(string.ascii_letters, k=3))
             for _ in range(val_size//4))
        )

    def action_weigts(self):
        diff = self.key_cnt_target - self.key_cnt
        relative_diff = diff / self.key_cnt_target
        return [
            max(0.1 - 4 * relative_diff, 0.05),
            1.0,
            max(0.15 + 8 * relative_diff, 0.05)
        ]

    def set_of_type(self, t: ValueType):
        return self.key_sets[t.value]

    def keys(self):
        return itertools.chain(*self.key_sets)

    def add_key(self, t: ValueType):
        k = self.key_cursor
        self.key_cursor += 1
        self.set_of_type(t).add(k)
        return k

    def randomize_type(self):
        return random.choice(list(ValueType))

    def randomize_nonempty_set(self):
        if not any(self.key_sets):
            return None, None

        t = self.randomize_type()
        s = self.set_of_type(t)

        if len(s) == 0:
            return self.randomize_nonempty_set()
        else:
            return s, t

    def randomize_key(self, t=None, pop=False):
        if t is None:
            s, t = self.randomize_nonempty_set()
        else:
            s = self.set_of_type(t)

        if s is None or len(s) == 0:
            return None

        k = s.pop()
        if not pop:
            s.add(k)

        return k

    def make_shrink(self):
        keys_gen = (self.randomize_key(pop=True)
                    for _ in range(random.randint(1, self.max_multikey)))
        keys = [str(k) for k in keys_gen if k is not None]
        if len(keys) == 0:
            return None, 0
        return "DEL " + " ".join(keys), -len(keys)

    def make_no_change(self):
        NO_CHANGE_ACTIONS = [
            ('APPEND {k} {val}', ValueType.STRING),
            ('SETRANGE {k} 10 {val}', ValueType.STRING),
            ('LPUSH {k} {val}', ValueType.LIST),
            ('LPOP {k}', ValueType.LIST),
            ('SADD {k} {val}', ValueType.SET),
            ('SPOP {k}', ValueType.SET)
        ]

        cmd, t = random.choice(NO_CHANGE_ACTIONS)
        k = self.randomize_key(t)
        val = ''.join(random.choices(string.ascii_letters, k=4))
        return cmd.format(k=str(k), val=val) if k is not None else None, 0

    def make_grow(self):
        t = self.randomize_type()
        if t == ValueType.STRING:
            keys = (self.add_key(t)
                    for _ in range(random.randint(1, self.max_multikey)))
            pairs = [str(k) + " " + self.str_filler for k in keys]
            return "MSET " + " ".join(pairs), len(pairs)
        elif t == ValueType.LIST:
            k = self.add_key(t)
            return f"LPUSH {k} {self.list_filler}", 1
        else:
            k = self.add_key(t)
            return f"SADD {k} {self.set_filler}", 1

    def make(self, action):
        if action == Action.SHRINK:
            return self.make_shrink()
        elif action == Action.NO_CHANGE:
            return self.make_no_change()
        else:
            return self.make_grow()

    def generate(self):
        actions = []

        cmds = []
        while len(cmds) < self.batch_size:
            if len(actions) == 0:
                actions = random.choices(
                    list(Action), weights=self.action_weigts(), k=50)

            cmd, delta = self.make(actions.pop())
            if cmd is not None:
                cmds.append(cmd)
                self.key_cnt += delta

        return cmds, self.key_cnt/self.key_cnt_target


class TestingExecutor:
    def __init__(self, pool, target_bytes, dbcount):
        self.pool = pool

        max_multikey = 5
        batch_size = 1_000
        val_size = 50
        target_keys = 500_000

        print(target_keys * dbcount)

        self.gen = TestingGenerator(
            val_size, batch_size, target_keys, max_multikey)
        self.dbcount = dbcount

    async def run(self, **kwargs):
        queues = [asyncio.Queue(maxsize=30) for _ in range(self.dbcount)]
        producer = asyncio.create_task(self.generator_task(queues, **kwargs))
        consumers = [
            asyncio.create_task(self.cosumer_task(i, queue))
            for i, queue in enumerate(queues)
        ]

        await producer
        for consumer in consumers:
            consumer.cancel()

    async def generator_task(self, queues, target_times=None, target_deviation=None):
        cpu_time = 0

        submitted = 0
        while True:
            start_time = time.time()
            blob, deviation = self.gen.generate()
            cpu_time += (time.time() - start_time)

            await asyncio.gather(*(q.put(blob) for q in queues))
            submitted += 1

            if target_times is not None and submitted >= target_times:
                break
            if target_deviation is not None and abs(1-deviation) < target_deviation:
                break

            await asyncio.sleep(0.0)

        print("done")

        for q in queues:
            await q.join()

        print("cpu time", cpu_time)

    async def cosumer_task(self, db, queue):
        client = aioredis.Redis(db=db)
        while True:
            cmds = await queue.get()
            pipe = client.pipeline(transaction=False)
            for cmd in cmds:
                pipe.execute_command(cmd)
            await pipe.execute()
            queue.task_done()

    async def checkhash(self, client=None):
        if client is None:
            client = self.client

        all_keys = sorted(list(self.gen.keys()))
        hashes = []

        for chunk in grouper(self.gen.batch_size, all_keys):
            pipe = client.pipeline(transaction=False)
            for k in chunk:
                pipe.execute_command(f"DUMP {k}")
            res = await pipe.execute()
            hashes.append(hash(tuple(res)))

        return hash(tuple(hashes))

async def main():
    client = aioredis.Redis()
    await client.flushall()

    pool = aioredis.ConnectionPool()

    #random.seed(11111)

    # Ask for 20 mb
    ex = TestingExecutor(pool, 1000, 16)

    # Fill with max target deviation of 10%
    await ex.run(target_deviation=0.1)

    # Run a few times
    # await ex.run(target_times=5)

    # print("hash: ", await ex.checkhash())

    print((await client.info("MEMORY"))['used_memory_human'])

asyncio.run(main())
