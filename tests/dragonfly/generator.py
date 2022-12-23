import asyncio
import aioredis
import random
import string
import itertools
import time
import difflib
import sys
from enum import Enum
from .utility import grouper


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class SizeChange(Enum):
    SHRINK = 0
    NO_CHANGE = 1
    GROW = 2


class ValueType(Enum):
    STRING = 0
    LIST = 1
    SET = 2
    HSET = 3


class TestingGenerator:
    """Class for generating complex command sequences"""

    def __init__(self, target_keys, val_size, batch_size, max_multikey):
        self.key_cnt_target = target_keys
        self.val_size = val_size
        self.batch_size = batch_size
        self.max_multikey = max_multikey

        # Key management
        self.key_sets = [set() for _ in ValueType]
        self.key_cursor = 0
        self.key_cnt = 0

        # Random string for MSET
        self.str_filler = lambda: ''.join(random.choices(
            string.ascii_letters, k=val_size))

        # Random sequence of single letter elements for LPUSH
        self.list_filler = lambda: ' '.join(
            random.choices(string.ascii_letters, k=val_size//2))

        # Random sequence of 3 letter elements for SADD
        self.set_filler = lambda: ' '.join(
            (''.join(random.choices(string.ascii_letters, k=3))
             for _ in range(val_size//4))
        )

        self.hset_filler = lambda: 'v0 0 v1 0 ' + ' '.join(
            (''.join(random.choices(string.ascii_letters, k=3))
             + ' ' + str(random.randint(0, 100))
             for _ in range(val_size//5))
        )

    def keys(self):
        return itertools.chain(*self.key_sets)

    def keys_and_types(self):
        out = []
        for t in list(ValueType):
            out.extend((k, t) for k in self.set_for_type(t))
        return out

    def set_for_type(self, t: ValueType):
        """Get backing keyset for value type"""

        return self.key_sets[t.value]

    def add_key(self, t: ValueType):
        k, self.key_cursor = self.key_cursor, self.key_cursor + 1
        self.set_for_type(t).add(k)
        return k

    def randomize_type(self):
        types = [t for t in ValueType if t != ValueType.SET]
        return random.choice(types)

    def randomize_nonempty_set(self):
        if not any(self.key_sets):
            return None, None

        t = self.randomize_type()
        s = self.set_for_type(t)

        if len(s) == 0:
            return self.randomize_nonempty_set()
        else:
            return s, t

    def randomize_key(self, t=None, pop=False) -> str:
        if t is None:
            s, t = self.randomize_nonempty_set()
        else:
            s = self.set_for_type(t)

        if s is None or len(s) == 0:
            return None, None

        k = s.pop()
        if not pop:
            s.add(k)

        return k, t

    def make_shrink(self):
        # Simulate shrinking by deleting a few random keys
        keys_gen = (self.randomize_key(pop=True)
                    for _ in range(random.randint(1, self.max_multikey)))
        keys = [str(k) for k, _ in keys_gen if k is not None]

        if len(keys) == 0:
            return None, 0
        return "DEL " + " ".join(keys), -len(keys)

    def make_no_change(self):
        # Simulate no changes to memory size by issuing one of the following commands:
        NO_CHANGE_ACTIONS = [
            ('APPEND {k} {val}', ValueType.STRING),
            ('SETRANGE {k} 10 {val}', ValueType.STRING),
            ('LPUSH {k} {val}', ValueType.LIST),
            ('LPOP {k}', ValueType.LIST),
            ('SADD {k} {val}', ValueType.SET),
            ('SPOP {k}', ValueType.SET),
            ('HSETNX {k} v0 {val}', ValueType.HSET),
            ('HINCRBY {k} v1 1', ValueType.HSET)
        ]

        cmd, t = random.choice(NO_CHANGE_ACTIONS)
        k, _ = self.randomize_key(t)
        val = ''.join(random.choices(string.ascii_letters, k=4))
        return cmd.format(k=str(k), val=val) if k is not None else None, 0

    def make_grow(self):
        # Simulate growing memory usage by using MSET, LPUSH or SADD
        # with large values or COPY

        # TODO: Implement COPY in Dragonfly.
        # 50% chance of copy
        # if self.key_cnt > 0 and random.random() < 0.5:
        #    k_old, t_old = self.randomize_key()
        #    if k_old is not None:
        #        k_new = self.add_key(t_old)
        #        return f"COPY {k_old} {k_new}", 1

        # Euqal chances for all types
        t = self.randomize_type()
        if t == ValueType.STRING:
            keys = (self.add_key(t)
                    for _ in range(random.randint(1, self.max_multikey)))
            pairs = [str(k) + " " + self.str_filler() for k in keys]
            return "MSET " + " ".join(pairs), len(pairs)
        elif t == ValueType.LIST:
            k = self.add_key(t)
            return f"LPUSH {k} {self.list_filler()}", 1
        elif t == ValueType.SET:
            k = self.add_key(t)
            return f"SADD {k} {self.set_filler()}", 1
        else:
            k = self.add_key(t)
            return f"HMSET {k} {self.hset_filler()}", 1

    def make(self, action):
        """ Create command for action and return it together with number of keys added (removed)"""

        if action == SizeChange.SHRINK:
            return self.make_shrink()
        elif action == SizeChange.NO_CHANGE:
            return self.make_no_change()
        else:
            return self.make_grow()

    def reset(self):
        self.key_sets = [set() for _ in ValueType]
        self.key_cursor = 0
        self.key_cnt = 0

    def size_change_probs(self):
        """Calculate probabilities of size change actions"""
        # Relative distance to key target
        dist = (self.key_cnt_target - self.key_cnt) / self.key_cnt_target
        return [
            max(0.15 - 4 * dist, 0.05),
            1.0,
            max(0.15 + 8 * dist, 0.05)
        ]

    def generate(self):
        """Generate next batch of commands, return it and ratio of current keys to target"""
        changes = []
        cmds = []
        while len(cmds) < self.batch_size:
            # Re-calculating changes in small groups increases performance
            if len(changes) == 0:
                changes = random.choices(
                    list(SizeChange), weights=self.size_change_probs(), k=50)

            cmd, delta = self.make(changes.pop())
            if cmd is not None:
                cmds.append(cmd)
                self.key_cnt += delta

        return cmds, self.key_cnt/self.key_cnt_target


class DataCapture:
    def __init__(self, entries):
        self.entries = entries

    def compare(self, other):
        if self.entries == other.entries:
            return True

        self._print_diff(other)
        return False

    def _print_diff(self, other):
        eprint("=== DIFF ===")
        printed = 0
        diff = difflib.ndiff(self.entries, other.entries)
        for line in diff:
            if line.startswith(' '):
                if printed >= 10:
                    eprint("... omitted ...")
                    break
                continue
            eprint(line)
            printed += 1
        eprint("=== END DIFF ===")


class DflySeeder:
    """Data seeder that supports strings, lists and sets"""

    def __init__(self, port=6379, keys=1000, val_size=50, batch_size=1000, max_multikey=5, dbcount=1):
        self.gen = TestingGenerator(
            keys, val_size, batch_size, max_multikey
        )
        self.port = port
        self.dbcount = dbcount
        self.stop_flag = False

    async def run(self, target_times=None, target_deviation=None):
        """
        Run a seeding cycle on all dbs until either a fixed number of batches (target_times)
        or until reaching an allowed deviation from the target number of keys (target_deviation)
        """
        self.stop_flag = False
        queues = [asyncio.Queue(maxsize=3) for _ in range(self.dbcount)]
        producer = asyncio.create_task(self._generator_task(
            queues, target_times=target_times, target_deviation=target_deviation))
        consumers = [
            asyncio.create_task(self._cosumer_task(i, queue))
            for i, queue in enumerate(queues)
        ]

        await producer
        for consumer in consumers:
            consumer.cancel()

    def stop(self):
        self.stop_flag = True

    def reset(self):
        self.gen.reset()

    async def capture(self, port=None, target_db=0, keys=None):
        def tostr(b): return str(b) if isinstance(
            b, int) else b.decode("utf-8")

        if port is None:
            port = self.port

        if keys is None:
            keys = sorted(list(self.gen.keys_and_types()))

        client = aioredis.Redis(port=port, db=target_db)
        fragments = []
        for group in grouper(100, keys):
            pipe = client.pipeline()
            for k, t in group:
                if t == ValueType.STRING:
                    pipe.get(k)
                elif t == ValueType.LIST:
                    pipe.lrange(k, start=0, end=-1)
                elif t == ValueType.SET:
                    pipe.sscan(k)
                else:
                    pipe.hgetall(k)

            results = await pipe.execute()
            for (k, t), res in zip(group, results):
                out = f"{t.name} {k}: "
                if t == ValueType.STRING:
                    out += tostr(res)
                elif t == ValueType.LIST:
                    out += ' '.join(tostr(s) for s in res)
                elif t == ValueType.SET:
                    out += ' '.join(sorted(tostr(s) for s in res[1]))
                else:
                    out += ' '.join(
                        tostr(k) + '=' + tostr(v)
                        for k, v in res.items()
                    )
                fragments.append(out)

        return DataCapture(fragments)

    async def compare(self, initial_capture, port=6379):
        keys = sorted(list(self.gen.keys_and_types()))
        for db in range(self.dbcount):
            capture = await self.capture(port=port, target_db=db, keys=keys)
            if not initial_capture.compare(capture):
                eprint(f">>> Inconsistent data on port {port}, db {db}")
                return False
        return True

    async def _generator_task(self, queues, target_times=None, target_deviation=None):
        cpu_time = 0
        submitted = 0

        while True:
            if self.stop_flag:
                break

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

        print("cpu time ", cpu_time)

        for q in queues:
            await q.join()

    async def _cosumer_task(self, db, queue):
        client = aioredis.Redis(port=self.port, db=db)
        while True:
            cmds = await queue.get()
            pipe = client.pipeline(transaction=False)
            for cmd in cmds:
                pipe.execute_command(cmd)
            await pipe.execute()
            queue.task_done()


async def main():
    random.seed(100)

    client = aioredis.Redis()
    await client.flushall()

    ex = DflySeeder(keys=10_000, batch_size=2500, dbcount=5)

    # Fill with max key deviation of 10%
    await ex.run(target_deviation=0.1)

    print((await client.info("MEMORY"))['used_memory_human'])

    # capture = await ex.capture()

# asyncio.run(main())
