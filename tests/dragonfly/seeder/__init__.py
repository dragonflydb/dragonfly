import asyncio
import random
import re
import typing
import math
import redis.asyncio as aioredis
from dataclasses import dataclass

try:
    from importlib import resources as impresources
except ImportError:
    # CI runs on python < 3.8
    import importlib_resources as impresources


class SeederBase:
    UID_COUNTER = 1  # multiple generators should not conflict on keys
    CACHED_SCRIPTS = {}
    TYPES = ["STRING", "LIST", "SET", "HASH", "ZSET", "JSON"]

    def __init__(self):
        self.uid = SeederBase.UID_COUNTER
        SeederBase.UID_COUNTER += 1

    @classmethod
    async def capture(clz, client: aioredis.Redis) -> typing.List[int]:
        """Generate hash capture for all data stored in instance pointed by client"""

        sha = await client.script_load(clz._load_script("hash"))
        return await asyncio.gather(*(client.evalsha(sha, 0, data_type) for data_type in clz.TYPES))

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

    def __init__(self, key_target=10_000, data_size=100, variance=5, samples=10):
        SeederBase.__init__(self)
        self.key_target = key_target
        self.data_size = data_size
        self.variance = variance
        self.samples = samples

    async def run(self, client: aioredis.Redis):
        """Run with specified options until key_target is met"""
        samples = [
            (dtype, f"k-s{self.uid}u{i}-") for i, dtype in enumerate(self.TYPES * self.samples)
        ]

        # Handle samples in chuncks of 24 to not overload client pool and instance
        for i in range(0, len(samples), 24):
            await asyncio.gather(
                *(self._run_unit(client, dtype, prefix) for dtype, prefix in samples[i : i + 32])
            )

    async def _run_unit(self, client: aioredis.Redis, dtype: str, prefix: str):
        key_target = self.key_target // (self.samples * len(self.TYPES))
        if dtype == "STRING":
            dsize = random.uniform(self.data_size / self.variance, self.data_size * self.variance)
            csize = 1
        else:
            dsize = self.data_size ** (1 / 3)
            dsize = random.uniform(dsize / self.variance, dsize * self.variance)
            csize = int(self.data_size // dsize) + 1

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

    def __init__(self, units=10, key_target=10_000, data_size=10):
        SeederBase.__init__(self)
        self.key_target = key_target
        self.data_size = data_size
        self.units = [
            Seeder.Unit(
                prefix=f"k-s{self.uid}u{i}-",
                type="STRING",
                # type=random.choice(Seeder.TYPES),
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
        ]

        sha = await client.script_load(Seeder._load_script("generate"))
        await asyncio.gather(
            *(self._run_unit(client, sha, unit, using_stopkey, args) for unit in self.units)
        )

    async def stop(self, client: aioredis.Redis):
        """Reqeust seeder seeder if it's running without a target, future returned from start() must still be awaited"""

        await asyncio.gather(*(client.set(unit.stop_key, "X") for unit in self.units))

    def change_key_target(self, target: int):
        """Change key target, applied only on succeeding runs"""

        self.key_target = max(target, 100)  # math breaks with low values

    @staticmethod
    async def _run_unit(client: aioredis.Redis, sha: str, unit: Unit, using_stopkey, args):
        await client.delete(unit.stop_key)

        args = [
            unit.prefix,
            unit.type,
            unit.counter,
            unit.stop_key if using_stopkey else "",
        ] + args

        unit.counter = await client.evalsha(sha, 0, *args)
