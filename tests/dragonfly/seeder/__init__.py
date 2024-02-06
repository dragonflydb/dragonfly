import asyncio
import random
import re
import typing
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
    TYPES = ["string"]

    @classmethod
    def _next_id(clz):
        clz.UID_COUNTER += 1
        return clz.UID_COUNTER

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


class Seeder(SeederBase):
    @dataclass
    class Unit:
        prefix: str
        type: str
        counter: int
        stop_key: str

    units: typing.List[Unit]

    def __init__(self, units=10, key_target=10_000, data_size=10):
        self.uid = SeederBase._next_id()
        self.key_target = key_target
        self.data_size = data_size
        self.units = [
            Seeder.Unit(
                prefix=f"k-s{self.uid}u{i}-",
                type=random.choice(Seeder.TYPES),
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

        self.key_target = max(target, 10)

    @classmethod
    async def capture(clz, client: aioredis.Redis) -> typing.List[int]:
        """Generate hash capture for all data stored in instance pointed by client"""

        sha = await client.script_load(Seeder._load_script("hash"))
        return await asyncio.gather(*(client.evalsha(sha, 0, data_type) for data_type in clz.TYPES))

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
