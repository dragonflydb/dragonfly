import aioredis
import itertools
import sys
import asyncio
import random
import string
import itertools
import time
import difflib
import json
from enum import Enum


def chunked(n, iterable):
    """Transform iterable into iterator of chunks of size n"""
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def eprint(*args, **kwargs):
    """Print to stderr"""
    print(*args, file=sys.stderr, **kwargs)


def gen_test_data(n, start=0, seed=None):
    for i in range(start, n):
        yield "k-"+str(i), "v-"+str(i) + ("-"+str(seed) if seed else "")


def batch_fill_data(client, gen, batch_size=100):
    for group in chunked(batch_size, gen):
        client.mset({k: v for k, v, in group})


async def wait_available_async(client: aioredis.Redis):
    """Block until instance exits loading phase"""
    its = 0
    while True:
        try:
            await client.get('key')
            return
        except aioredis.ResponseError as e:
            assert "Can not execute during LOADING" in str(e)

        # Print W to indicate test is waiting for replica
        print('W', end='', flush=True)
        await asyncio.sleep(0.01)
        its += 1


class SizeChange(Enum):
    SHRINK = 0
    NO_CHANGE = 1
    GROW = 2


class ValueType(Enum):
    STRING = 0
    LIST = 1
    SET = 2
    HSET = 3
    ZSET = 4
    JSON = 5


class CommandGenerator:
    """Class for generating complex command sequences"""

    def __init__(self, target_keys, val_size, batch_size, max_multikey, unsupported_types=[]):
        self.key_cnt_target = target_keys
        self.val_size = val_size
        self.batch_size = min(batch_size, target_keys)
        self.max_multikey = max_multikey
        self.unsupported_types = unsupported_types

        # Key management
        self.key_sets = [set() for _ in ValueType]
        self.key_cursor = 0
        self.key_cnt = 0

        # Grow factors
        self.diff_speed = 5
        self.base_diff_prob = 0.2
        self.min_diff_prob = 0.1

    def keys(self):
        return itertools.chain(*self.key_sets)

    def keys_and_types(self):
        return ((k, t) for t in list(ValueType) for k in self.set_for_type(t))

    def set_for_type(self, t: ValueType):
        return self.key_sets[t.value]

    def add_key(self, t: ValueType):
        """Add new key of type t"""
        k, self.key_cursor = self.key_cursor, self.key_cursor + 1
        self.set_for_type(t).add(k)
        return k

    def random_type(self):
        return random.choice([t for t in ValueType if (t not in self.unsupported_types)])

    def randomize_nonempty_set(self):
        """Return random non-empty set and its type"""
        if not any(self.key_sets):
            return None, None

        t = self.random_type()
        s = self.set_for_type(t)

        if len(s) == 0:
            return self.randomize_nonempty_set()
        else:
            return s, t

    def randomize_key(self, t=None, pop=False):
        """Return random key and its type"""
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

    def generate_val(self, t: ValueType):
        """Generate filler value of configured size for type t"""
        def rand_str(k=3, s=''):
            # Use small k value to reduce mem usage and increase number of ops
            return s.join(random.choices(string.ascii_letters, k=k))

        if t == ValueType.STRING:
            # Random string for MSET
            return (rand_str(self.val_size),)
        elif t == ValueType.LIST:
            # Random sequence k-letter elements for LPUSH
            return tuple(rand_str() for _ in range(self.val_size//4))
        elif t == ValueType.SET:
            # Random sequence of k-letter elements for SADD
            return tuple(rand_str() for _ in range(self.val_size//4))
        elif t == ValueType.HSET:
            # Random sequence of k-letter keys + int and two start values for HSET
            elements = ((rand_str(), random.randint(0, self.val_size))
                        for _ in range(self.val_size//5))
            return ('v0', 0, 'v1', 0) + tuple(itertools.chain(*elements))
        elif t == ValueType.ZSET:
            # Random sequnce of k-letter keys and int score for ZSET
            elements = ((random.randint(0, self.val_size), rand_str())
                        for _ in range(self.val_size//4))
            return tuple(itertools.chain(*elements))

        elif t == ValueType.JSON:
            # Json object with keys:
            # - arr (array of random strings)
            # - ints (array of objects {i:random integer})
            # - i (random integer)
            ints = [{"i": random.randint(0, 100)}
                    for i in range(self.val_size//6)]
            strs = [rand_str() for _ in range(self.val_size//6)]
            return "$", json.dumps({"arr": strs, "ints": ints, "i": random.randint(0, 100)})
        else:
            assert False, "Invalid ValueType"

    def gen_shrink_cmd(self):
        """
        Generate command that shrinks data: DEL of random keys or almost immediate <=50ms PEXPIRE.
        """
        if random.random() < 0.3:
            key, _ = self.randomize_key(pop=True)
            if key == None:
                return None, 0
            return f"PEXPIRE k{key} {random.randint(0, 50)}", -1
        else:
            keys_gen = (self.randomize_key(pop=True)
                        for _ in range(random.randint(1, self.max_multikey)))
            keys = [f"k{k}" for k, _ in keys_gen if k is not None]

            if len(keys) == 0:
                return None, 0
            return "DEL " + " ".join(keys), -len(keys)

    UPDATE_ACTIONS = [
        ('APPEND {k} {val}', ValueType.STRING),
        ('SETRANGE {k} 10 {val}', ValueType.STRING),
        ('LPUSH {k} {val}', ValueType.LIST),
        ('LPOP {k}', ValueType.LIST),
        ('SADD {k} {val}', ValueType.SET),
        ('SPOP {k}', ValueType.SET),
        ('HSETNX {k} v0 {val}', ValueType.HSET),
        ('HINCRBY {k} v1 1', ValueType.HSET),
        ('ZPOPMIN {k} 1', ValueType.ZSET),
        ('ZADD {k} 0 {val}', ValueType.ZSET),
        ('JSON.NUMINCRBY {k} $..i 1', ValueType.JSON),
        ('JSON.ARRPOP {k} $.arr', ValueType.JSON),
        ('JSON.ARRAPPEND {k} $.arr "{val}"', ValueType.JSON)
    ]

    def gen_update_cmd(self):
        """
        Generate command that makes no change to keyset: random of UPDATE_ACTIONS.
        """
        cmd, t = random.choice(self.UPDATE_ACTIONS)
        k, _ = self.randomize_key(t)
        val = ''.join(random.choices(string.ascii_letters, k=3))
        return cmd.format(k=f"k{k}", val=val) if k is not None else None, 0

    GROW_ACTINONS = {
        ValueType.STRING: 'MSET',
        ValueType.LIST: 'LPUSH',
        ValueType.SET: 'SADD',
        ValueType.HSET: 'HMSET',
        ValueType.ZSET: 'ZADD',
        ValueType.JSON: 'JSON.SET'
    }

    def gen_grow_cmd(self):
        """
        Generate command that grows keyset: Initialize key of random type with filler value.
        """
        # TODO: Implement COPY in Dragonfly.
        t = self.random_type()
        if t == ValueType.STRING:
            count = random.randint(1, self.max_multikey)
        else:
            count = 1

        keys = (self.add_key(t) for _ in range(count))
        payload = itertools.chain(
            *((f"k{k}",) + self.generate_val(t) for k in keys))
        filtered_payload = filter(lambda p: p is not None, payload)

        return (self.GROW_ACTINONS[t],) + tuple(filtered_payload), count

    def make(self, action):
        """ Create command for action and return it together with number of keys added (removed)"""
        if action == SizeChange.SHRINK:
            return self.gen_shrink_cmd()
        elif action == SizeChange.NO_CHANGE:
            return self.gen_update_cmd()
        else:
            return self.gen_grow_cmd()

    def reset(self):
        self.key_sets = [set() for _ in ValueType]
        self.key_cursor = 0
        self.key_cnt = 0

    def size_change_probs(self):
        """Calculate probabilities of size change actions"""
        # Relative distance to key target
        dist = (self.key_cnt_target - self.key_cnt) / self.key_cnt_target
        # Shrink has a roughly twice as large expected number of changed keys than grow
        return [
            max(self.base_diff_prob - self.diff_speed * dist, self.min_diff_prob),
            1.0,
            max(self.base_diff_prob + 2 *
                self.diff_speed * dist, self.min_diff_prob)
        ]

    def generate(self):
        """Generate next batch of commands, return it and ratio of current keys to target"""
        changes = []
        cmds = []
        while len(cmds) < self.batch_size:
            # Re-calculating changes in small groups
            if len(changes) == 0:
                changes = random.choices(
                    list(SizeChange), weights=self.size_change_probs(), k=20)

            cmd, delta = self.make(changes.pop())
            if cmd is not None:
                cmds.append(cmd)
                self.key_cnt += delta

        return cmds, self.key_cnt/self.key_cnt_target


class DataCapture:
    """
    Captured state of single database.
    """

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
                continue
            eprint(line)
            if printed >= 20:
                eprint("... omitted ...")
                break
            printed += 1
        eprint("=== END DIFF ===")


class DflySeeder:
    """
    Data seeder with support for multiple types and commands.

    Usage:

    Create a seeder with target number of keys (100k) of specified size (200) and work on 5 dbs,

        seeder = new DflySeeder(keys=100_000, value_size=200, dbcount=5)

    Stop when we are in 5% of target number of keys (i.e. above 95_000)
    Because its probabilistic we might never reach exactly 100_000.

        await seeder.run(target_deviation=0.05)

    Run 3000 commands in stable state, crate a capture and compare it to
    replica on port 1112

        await seeder.run(target_op=3000)
        capture = await seeder.capture()
        assert await seeder.compare(capture, port=1112)
    """

    def __init__(self, port=6379, keys=1000, val_size=50, batch_size=100, max_multikey=5, dbcount=1, multi_transaction_probability=0.3, log_file=None, unsupported_types=[]):
        self.gen = CommandGenerator(
            keys, val_size, batch_size, max_multikey, unsupported_types
        )
        self.port = port
        self.dbcount = dbcount
        self.multi_transaction_probability = multi_transaction_probability
        self.stop_flag = False

        self.log_file = log_file
        if self.log_file is not None:
            open(self.log_file, 'w').close()

    async def run(self, target_ops=None, target_deviation=None):
        """
        Run a seeding cycle on all dbs either until stop(), a fixed number of commands (target_ops)
        or until reaching an allowed deviation from the target number of keys (target_deviation)
        """
        print(f"Running ops:{target_ops} deviation:{target_deviation}")
        self.stop_flag = False
        queues = [asyncio.Queue(maxsize=3) for _ in range(self.dbcount)]
        producer = asyncio.create_task(self._generator_task(
            queues, target_ops=target_ops, target_deviation=target_deviation))
        consumers = [
            asyncio.create_task(self._executor_task(i, queue))
            for i, queue in enumerate(queues)
        ]

        time_start = time.time()

        cmdcount = await producer
        for consumer in consumers:
            await consumer

        took = time.time() - time_start
        qps = round(cmdcount * self.dbcount / took, 2)
        print(f"Filling took: {took}, QPS: {qps}")

    def stop(self):
        """Stop all invocations to run"""
        self.stop_flag = True

    def reset(self):
        """ Reset internal state. Needs to be called after flush or restart"""
        self.gen.reset()

    async def capture(self, port=None):
        """Create DataCapture for all dbs"""

        if port is None:
            port = self.port
        keys = sorted(list(self.gen.keys_and_types()))

        captures = await asyncio.gather(*(
            self._capture_db(port=port, target_db=db, keys=keys) for db in range(self.dbcount)
        ))
        return captures

    async def compare(self, initial_captures, port=6379):
        """Compare data capture with all dbs of instance and return True if all dbs are correct"""
        print(f"comparing capture to {port}")
        target_captures = await self.capture(port=port)

        for db, target_capture, initial_capture in zip(range(self.dbcount), target_captures, initial_captures):
            print(f"comparing capture to {port}, db: {db}")
            if not initial_capture.compare(target_capture):
                eprint(f">>> Inconsistent data on port {port}, db {db}")
                return False
        return True

    def target(self, key_cnt):
        self.gen.key_cnt_target = key_cnt

    async def _capture_db(self, port, target_db, keys):
        eprint(f"Capture data on port {port}, db {target_db}")
        client = aioredis.Redis(port=port, db=target_db)
        capture = DataCapture(await self._capture_entries(client, keys))
        await client.connection_pool.disconnect()
        return capture

    async def _generator_task(self, queues, target_ops=None, target_deviation=None):
        cpu_time = 0
        submitted = 0
        batches = 0
        deviation = 0.0

        file = None
        if self.log_file:
            file = open(self.log_file, 'a')

        def should_run():
            if self.stop_flag:
                return False
            if target_ops is not None and submitted >= target_ops:
                return False
            if target_deviation is not None and abs(1-deviation) < target_deviation:
                return False
            return True

        def stringify_cmd(cmd):
            if isinstance(cmd, tuple):
                return " ".join(str(c) for c in cmd)
            else:
                return str(cmd)

        while should_run():
            start_time = time.time()
            blob, deviation = self.gen.generate()
            is_multi_transaction = random.random() < self.multi_transaction_probability
            tx_data = (blob, is_multi_transaction)
            cpu_time += (time.time() - start_time)

            await asyncio.gather(*(q.put(tx_data) for q in queues))
            submitted += len(blob)
            batches += 1

            if file is not None:
                pattern = "MULTI\n{}\nEXEC\n" if is_multi_transaction else "{}\n"
                file.write(pattern.format('\n'.join(stringify_cmd(cmd)
                           for cmd in blob)))

            print('.', end='', flush=True)
            await asyncio.sleep(0.0)

        print("\ncpu time", cpu_time, "batches",
              batches, "commands", submitted)

        await asyncio.gather(*(q.put(None) for q in queues))
        for q in queues:
            await q.join()

        if file is not None:
            file.flush()

        return submitted

    async def _executor_task(self, db, queue):
        client = aioredis.Redis(port=self.port, db=db)

        while True:
            tx_data = await queue.get()
            if tx_data is None:
                queue.task_done()
                break

            pipe = client.pipeline(transaction=tx_data[1])
            for cmd in tx_data[0]:
                if isinstance(cmd, str):
                    pipe.execute_command(cmd)
                else:
                    pipe.execute_command(*cmd)

            try:
                await pipe.execute()
            except Exception as e:
                raise SystemExit(e)
            queue.task_done()
        await client.connection_pool.disconnect()

    CAPTURE_COMMANDS = {
        ValueType.STRING: lambda pipe, k: pipe.get(k),
        ValueType.LIST: lambda pipe, k: pipe.lrange(k, 0, -1),
        ValueType.SET: lambda pipe, k: pipe.smembers(k),
        ValueType.HSET: lambda pipe, k: pipe.hgetall(k),
        ValueType.ZSET: lambda pipe, k: pipe.zrange(
            k, start=0, end=-1, withscores=True),
        ValueType.JSON: lambda pipe, k: pipe.execute_command(
            "JSON.GET", k, "$")
    }

    CAPTURE_EXTRACTORS = {
        ValueType.STRING: lambda res, tostr: (tostr(res),),
        ValueType.LIST: lambda res, tostr: (tostr(s) for s in res),
        ValueType.SET: lambda res, tostr: sorted(tostr(s) for s in res),
        ValueType.HSET: lambda res, tostr: sorted(tostr(k)+"="+tostr(v) for k, v in res.items()),
        ValueType.ZSET: lambda res, tostr: (
            tostr(s)+"-"+str(f) for (s, f) in res),
        ValueType.JSON: lambda res, tostr: (tostr(res),)
    }

    async def _capture_entries(self, client, keys):
        def tostr(b):
            return b.decode("utf-8") if isinstance(b, bytes) else str(b)

        entries = []
        for group in chunked(self.gen.batch_size * 2, keys):
            pipe = client.pipeline(transaction=False)
            for k, t in group:
                self.CAPTURE_COMMANDS[t](pipe, f"k{k}")

            results = await pipe.execute()
            for (k, t), res in zip(group, results):
                out = f"{t.name} k{k}: " + \
                    ' '.join(self.CAPTURE_EXTRACTORS[t](res, tostr))
                entries.append(out)

        return entries


class DflySeederFactory:
    """
    Used to pass params to a DflySeeder.
    """

    def __init__(self, log_file=None):
        self.log_file = log_file

    def create(self, **kwargs):
        return DflySeeder(log_file=self.log_file, **kwargs)
