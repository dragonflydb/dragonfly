import asyncio
import random
import logging
import re
import typing
import math
import redis.asyncio as aioredis
from dataclasses import dataclass
import time

try:
    from importlib import resources as impresources
except ImportError:
    # CI runs on python < 3.8
    import importlib_resources as impresources


class SeederBase:
    UID_COUNTER = 1  # multiple generators should not conflict on keys
    CACHED_SCRIPTS = {}
    DEFAULT_TYPES = ["STRING", "LIST", "SET", "HASH", "ZSET", "JSON"]
    BIG_VALUE_TYPES = ["LIST", "SET", "HASH", "ZSET"]

    def __init__(self, types: typing.Optional[typing.List[str]] = None):
        self.uid = SeederBase.UID_COUNTER
        SeederBase.UID_COUNTER += 1
        self.types = types if types is not None else SeederBase.DEFAULT_TYPES

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


class StaticSeeder(SeederBase):
    """Wrapper around DEBUG POPULATE with fuzzy key sizes and a balanced type mix"""

    def __init__(
        self,
        key_target=10_000,
        data_size=100,
        variance=5,
        samples=10,
        collection_size=None,
        types: typing.Optional[typing.List[str]] = None,
    ):
        SeederBase.__init__(self, types)
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
        for i in range(0, len(samples), 24):
            await asyncio.gather(
                *(self._run_unit(client, dtype, prefix) for dtype, prefix in samples[i : i + 32])
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
        logging.debug(args)
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
    ):
        SeederBase.__init__(self, types)
        self.key_target = key_target
        self.data_size = data_size
        if collection_size is None:
            self.collection_size = math.ceil(data_size ** (1 / 3))
        else:
            self.collection_size = collection_size

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
            self.huge_value_target / len(self.units),
            self.huge_value_size,
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
