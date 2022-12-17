import asyncio
import aioredis
import random
import string
import itertools
from collections import deque
from enum import Enum
from utility import grouper

class Action(Enum):
    SHRINK = 0
    NO_CHANGE = 1
    GROW = 2


class ValueType(Enum):
    STRING = 0
    LIST = 1


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

        self.list_filler = ''.join(random.choices(
            string.ascii_letters + 13 * ' ', k=val_size))

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
            ('LPUSH {k} {val}', ValueType.LIST),
            ('LPOP {k}', ValueType.LIST),
            ('APPEND {k} {val}', ValueType.STRING),
            ('SETRANGE {k} 10 {val}', ValueType.STRING)
        ]

        cmd, t = random.choice(NO_CHANGE_ACTIONS)
        k = self.randomize_key(t)
        return cmd.format(k=str(k), val="--delta--") if k is not None else None, 0

    def make_grow(self):
        t = self.randomize_type()
        if t == ValueType.STRING:
            keys = (self.add_key(t)
                    for _ in range(random.randint(1, self.max_multikey)))
            pairs = [str(k) + " " + self.str_filler for k in keys]
            return "MSET " + " ".join(pairs), len(pairs)
        else:
            k = self.add_key(t)
            return f"LPUSH {k} {self.list_filler}", 1

    def make(self, action):
        if action == Action.SHRINK:
            return self.make_shrink()
        elif action == Action.NO_CHANGE:
            return self.make_no_change()
        else:
            return self.make_grow()

    def generate(self):
        actions = random.choices(
            list(Action), weights=self.action_weigts(), k=self.batch_size)

        cmds = []
        sum_delta = 0
        for action in actions:
            cmd, delta = self.make(action)
            if cmd is not None:
                cmds.append(cmd)
                self.key_cnt += delta
                sum_delta += delta

        #print(sum_delta, self.key_cnt, self.key_cnt/self.key_cnt_target)
        return cmds, self.key_cnt/self.key_cnt_target


class TestingExecutor:
    def __init__(self, client, target_bytes):
        self.client = client

        # Count number of expected keys. Account for memory overhead.
        max_multikey = 5
        batch_size = 500
        val_size = 500
        target_keys = target_bytes // val_size
        print("target keys", target_keys)
        self.gen = TestingGenerator(
            val_size, batch_size, target_keys, max_multikey)

        self.task = None
        self.task_target_rel = None
        self.batches = deque()
        self.max_batches = 5

    def prefill(self):
        while len(self.batches) < self.max_batches:
            self.batches.append(tuple(self.gen.generate()))

    async def run(self, target_times=None, target_devitation=None):
        submitted = 0
        while True:
            # While we're waiting lets pre-generate data
            while self.task is not None and not self.task.done() and len(self.batches) < self.max_batches:
                self.batches.append(tuple(self.gen.generate()))

            # Await task
            if self.task is not None:
                await self.task
                submitted += 1
                #print("Submitted", self.task_target_rel)
                if target_times is not None and submitted >= target_times:
                    return
                if target_devitation is not None and abs(1-self.task_target_rel) < target_devitation:
                    print("deviation is ",  self.task_target_rel)
                    return

            # Generate or fetch new data
            if len(self.batches) > 0:
                cmds, target_rel = self.batches.popleft()
            else:
                print("generating on demand :(")
                cmds, target_rel = self.gen.generate()

            # Schedule task
            pipe = self.client.pipeline()
            for cmd in cmds:
                pipe.execute_command(cmd)

            self.task = asyncio.create_task(pipe.execute())
            self.task_target_rel = target_rel

    async def checkhash(self, client=None):
        if client is None:
            client = self.client

        all_keys = sorted(list(self.gen.keys()))
        hashes = []

        for chunk in grouper(self.gen.batch_size, all_keys):
            pipe = client.pipeline()
            for k in chunk:
                pipe.execute_command(f"DUMP {k}")
            res = await pipe.execute()
            hashes.append(hash(tuple(res)))

        return hash(tuple(hashes))


async def main():
    client = aioredis.Redis()
    await client.flushall()

    random.seed(11111)

    # Ask for 100mb
    ex = TestingExecutor(client, 100_000_000)
    ex.prefill()

    # Fill with max target deviation of 10%
    await ex.run(target_devitation=0.1)

    # Run a few times
    await ex.run(target_times=5)

    print("hash: ", await ex.checkhash())

    print((await client.info("MEMORY"))['used_memory_human'])
    print("Target key count", ex.gen.key_cnt_target, ex.gen.key_cnt, (await client.dbsize()))

asyncio.run(main())
