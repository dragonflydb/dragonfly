import asyncio
import random
import logging
import re
import typing
import math
import redis
import redis.asyncio as aioredis
from dataclasses import dataclass
import time
import sys

import numpy as np

try:
    from importlib import resources as impresources
except ImportError:
    # CI runs on python < 3.8
    import importlib_resources as impresources


class SeederBase:
    UID_COUNTER = 1  # multiple generators should not conflict on keys
    CACHED_SCRIPTS = {}
    DEFAULT_TYPES = ["STRING", "LIST", "SET", "HASH", "ZSET", "JSON", "STREAM"]

    def __init__(self, types: typing.Optional[typing.List[str]] = None, seed=None):
        self.uid = SeederBase.UID_COUNTER
        SeederBase.UID_COUNTER += 1
        self.types = types if types is not None else SeederBase.DEFAULT_TYPES

        self.seed = random.randrange(sys.maxsize)
        if seed is not None:
            self.seed = seed

        random.seed(int(self.seed))
        logging.debug(f"Random seed: {self.seed}, check: {random.randrange(100)}")

    @classmethod
    async def capture(
        clz, client: aioredis.Redis, types: typing.Optional[typing.List[str]] = None
    ) -> typing.Tuple[int]:
        """Generate hash capture for all data stored in instance pointed by client"""

        sha = await client.script_load(clz._load_script("hash"))
        types_to_capture = types if types is not None else clz.DEFAULT_TYPES
        return tuple(
            await asyncio.gather(
                *(clz._run_capture(client, sha, data_type) for data_type in types_to_capture)
            )
        )

    @staticmethod
    async def _run_capture(client, sha, data_type):
        s = time.time()
        res = await client.evalsha(sha, 0, data_type)
        logging.debug(f"hash capture of {data_type} took {time.time() - s}")
        return res

    @staticmethod
    def _read_file(fname):
        try:
            script_file = impresources.files(__package__) / fname
            with script_file.open("rt") as f:
                return f.read()
        except AttributeError:
            return impresources.read_text(__package__, fname)

    @classmethod
    def _load_script(clz, fname):
        if fname in clz.CACHED_SCRIPTS:
            return clz.CACHED_SCRIPTS[fname]

        script = clz._read_file(f"script-{fname}.lua")
        requested = re.findall(r"-- import:(.*?) --", script)
        for request in requested:
            lib = clz._read_file(f"script-{request}.lua")
            script = script.replace(f"-- import:{request} --", lib)

        clz.CACHED_SCRIPTS[fname] = script
        return script


class DebugPopulateSeeder(SeederBase):
    """Wrapper around DEBUG POPULATE with fuzzy key sizes and a balanced type mix"""

    def __init__(
        self,
        key_target=10_000,
        data_size=100,
        variance=5,
        samples=10,
        collection_size=None,
        types: typing.Optional[typing.List[str]] = None,
        seed=None,
    ):
        SeederBase.__init__(self, types, seed)
        self.key_target = key_target
        self.data_size = data_size
        self.variance = variance
        self.samples = samples

        if collection_size is None:
            self.collection_size = data_size ** (1 / 3)
        else:
            self.collection_size = collection_size

    async def run(self, client: aioredis.Redis):
        """Run with specified options until key_target is met"""
        samples = [
            (dtype, f"k-s{self.uid}u{i}-") for i, dtype in enumerate(self.types * self.samples)
        ]

        # Handle samples in chuncks of 24 to not overload client pool and instance
        chunk_size = 24
        for i in range(0, len(samples), chunk_size):
            await asyncio.gather(
                *(
                    self._run_unit(client, dtype, prefix)
                    for dtype, prefix in samples[i : i + chunk_size]
                )
            )

    async def _run_unit(self, client: aioredis.Redis, dtype: str, prefix: str):
        key_target = self.key_target // (self.samples * len(self.types))
        if dtype == "STRING":
            dsize = random.uniform(self.data_size / self.variance, self.data_size * self.variance)
            csize = 1
        else:
            csize = self.collection_size
            csize = math.ceil(random.uniform(csize / self.variance, csize * self.variance))
            dsize = self.data_size // csize

        args = ["DEBUG", "POPULATE", key_target, prefix, math.ceil(dsize)]
        args += ["RAND", "TYPE", dtype, "ELEMENTS", csize]
        return await client.execute_command(*args)


class Seeder(SeederBase):
    @dataclass
    class Unit:
        prefix: str
        type: str
        counter: int
        stop_key: str

    units: typing.List[Unit]

    def __init__(
        self,
        units=10,
        key_target=10_000,
        data_size=100,
        collection_size=None,
        types: typing.Optional[typing.List[str]] = None,
        huge_value_target=5,
        huge_value_size=100000,
        seed=None,
        huge_value_add_only=False,
    ):
        SeederBase.__init__(self, types, seed)
        self.key_target = key_target
        self.data_size = data_size
        if collection_size is None:
            self.collection_size = math.ceil(data_size ** (1 / 3))
        else:
            self.collection_size = collection_size

        self.huge_value_add_only = huge_value_add_only
        self.huge_value_target = huge_value_target
        self.huge_value_size = huge_value_size

        self.units = [
            Seeder.Unit(
                prefix=f"k-s{self.uid}u{i}-",
                type=self.types[i % len(self.types)],
                counter=0,
                stop_key=f"_s{self.uid}u{i}-stop",
            )
            for i in range(units)
        ]

    async def run(self, client: aioredis.Redis, target_ops=None, target_deviation=None):
        """Run seeder until one of the targets or until stopped if none are set"""

        using_stopkey = target_ops is None and target_deviation is None
        args = [
            self.key_target / len(self.units),
            target_ops if target_ops is not None else 0,
            target_deviation if target_deviation is not None else -1,
            self.data_size,
            self.collection_size,
            int(self.huge_value_add_only),
            self.huge_value_target / len(self.units),
            self.huge_value_size,
            self.seed,
        ]

        sha = await client.script_load(Seeder._load_script("generate"))
        for unit in self.units:
            # Must be serial, otherwise cluster clients throws an exception
            await self._run_unit(client, sha, unit, using_stopkey, args)

    async def stop(self, client: aioredis.Redis):
        """Request seeder seeder if it's running without a target, future returned from start() must still be awaited"""

        for unit in self.units:
            # Must be serial, otherwise cluster clients throws an exception
            await client.set(unit.stop_key, "X")

    def change_key_target(self, target: int):
        """Change key target, applied only on succeeding runs"""

        self.key_target = max(target, 100)  # math breaks with low values

    @staticmethod
    async def _run_unit(client: aioredis.Redis, sha: str, unit: Unit, using_stopkey, args):
        await client.delete(unit.stop_key)

        s = time.time()

        args = [
            unit.prefix,
            unit.type,
            unit.counter,
            unit.stop_key if using_stopkey else "",
        ] + args

        result = await client.evalsha(sha, 0, *args)
        result = result.split()
        unit.counter = int(result[0])
        huge_entries = int(result[1])

        msg = f"running unit {unit.prefix}/{unit.type} took {time.time() - s}, target {args[4+0]}"
        if huge_entries > 0:
            msg = f"{msg}. Total huge entries {huge_entries} added."

        logging.debug(msg)


class HnswSearchSeeder:

    def __init__(
        self,
        index_name="hnsw_idx",
        prefix="doc:",
        num_dims=4,
        num_initial_docs=200,
        seed=42,
    ):
        self.index_name = index_name
        self.prefix = prefix
        self.num_dims = num_dims
        self.num_initial_docs = num_initial_docs
        self.seed = seed

        self._doc_counter = 0
        self._stop_event = asyncio.Event()

    def _make_embedding(self):
        return np.random.uniform(-10, 10, self.num_dims).astype(np.float32)

    async def create_index(self, client: aioredis.Redis):
        await client.execute_command(
            "FT.CREATE",
            self.index_name,
            "ON",
            "HASH",
            "PREFIX",
            "1",
            self.prefix,
            "SCHEMA",
            "title",
            "TEXT",
            "embedding",
            "VECTOR",
            "HNSW",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            str(self.num_dims),
            "DISTANCE_METRIC",
            "L2",
        )

    async def seed_initial_docs(self, client: aioredis.Redis):
        pipe = client.pipeline(transaction=False)
        for i in range(self.num_initial_docs):
            emb = self._make_embedding()
            pipe.hset(
                f"{self.prefix}{i}",
                mapping={
                    "title": f"Product {i}",
                    "embedding": emb.tobytes(),
                },
            )
        await pipe.execute()
        self._doc_counter = self.num_initial_docs

    def stop(self):
        self._stop_event.set()

    async def verify(self, *clients: aioredis.Redis, num_queries=10):
        if len(clients) < 2:
            raise ValueError("Need at least two clients to compare")

        sizes = [await c.dbsize() for c in clients]
        for i in range(1, len(sizes)):
            assert (
                sizes[0] == sizes[i]
            ), f"dbsize mismatch: client[0]={sizes[0]} vs client[{i}]={sizes[i]}"

        # Verify HNSW search works and returns consistent results on all clients.
        # For now we suppose that results should be 100% the same on all clients,
        # in the future we can relax this assertion and check for some overlap in results instead,
        # as HNSW is not deterministic and can return different results on different runs
        for _ in range(num_queries):
            query_vec = self._make_embedding().tobytes()
            results = []
            for c in clients:
                r = await c.execute_command(
                    "FT.SEARCH",
                    self.index_name,
                    "*=>[KNN 5 @embedding $vec]",
                    "PARAMS",
                    "2",
                    "vec",
                    query_vec,
                    "LIMIT",
                    "0",
                    "5",
                )
                doc_ids = sorted(r[i] for i in range(1, len(r), 2))
                results.append((r[0], doc_ids))
            for i in range(1, len(results)):
                assert results[0] == results[i]
            assert results[0][0] > 0, "KNN search returned no results"

    async def run_traffic(self, client: aioredis.Redis, sleep_interval=0.01):
        self._stop_event.clear()
        while not self._stop_event.is_set():
            op = random.choice(["insert", "update", "delete"])
            try:
                if op == "insert":
                    emb = self._make_embedding()
                    await client.hset(
                        f"{self.prefix}{self._doc_counter}",
                        mapping={
                            "title": f"Product {self._doc_counter}",
                            "embedding": emb.tobytes(),
                        },
                    )
                    self._doc_counter += 1
                elif op == "update":
                    key_id = random.randint(0, max(self._doc_counter - 1, 0))
                    key = f"{self.prefix}{key_id}"
                    if not await client.exists(key):
                        continue
                    emb = self._make_embedding()
                    await client.hset(key, mapping={"embedding": emb.tobytes()})
                elif op == "delete":
                    key_id = random.randint(0, max(self._doc_counter - 1, 0))
                    await client.delete(f"{self.prefix}{key_id}")
            except (redis.exceptions.ConnectionError, redis.exceptions.ResponseError):
                await asyncio.sleep(sleep_interval)
            await asyncio.sleep(sleep_interval)

    async def run_search_queries(self, client: aioredis.Redis, sleep_interval=0.05):
        while not self._stop_event.is_set():
            try:
                query_vec = self._make_embedding().tobytes()
                await client.execute_command(
                    "FT.SEARCH",
                    self.index_name,
                    "*=>[KNN 5 @embedding $vec]",
                    "PARAMS",
                    "2",
                    "vec",
                    query_vec,
                    "LIMIT",
                    "0",
                    "5",
                )
            except (redis.exceptions.ConnectionError, redis.exceptions.ResponseError):
                pass
            await asyncio.sleep(sleep_interval)
