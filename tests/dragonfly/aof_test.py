import asyncio
import re

import pytest
import redis

from . import dfly_args
from .instance import DflyInstanceFactory, DflyStartException
from .seeder import Seeder as SeederV2


@dfly_args({"proactor_threads": 4})
async def test_replay_restores_data(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path))
    df.start()
    client = df.client()

    # The seeder sets no TTLs, so the data cannot change between the captures.
    seeder = SeederV2(key_target=500_000)
    await seeder.run(client, target_deviation=0.1)
    before = await SeederV2.capture(client)
    keys = await client.dbsize()
    assert keys >= 400_000

    # A graceful stop seals, writes and syncs every shard's AOF.
    df.stop()
    df.start()
    client = df.client()
    assert await client.dbsize() == keys
    assert await SeederV2.capture(client) == before


@pytest.mark.debug_only
@dfly_args({"proactor_threads": 2, "num_shards": 1})
async def test_kill_loses_only_unwritten_tail(df_factory: DflyInstanceFactory, tmp_path):
    num_keys = 50_000
    keys = [f"key:{i}" for i in range(num_keys)]

    for attempt in range(10):
        # A slow heartbeat leaves the open block unsealed for up to a second.
        aof_dir = tmp_path / f"attempt{attempt}"
        aof_dir.mkdir()
        df = df_factory.create(aof=True, hz=1, dir=str(aof_dir))
        df.start()
        client = df.client()

        for i in range(0, num_keys, 1000):
            pipe = client.pipeline(transaction=False)
            for j in range(i, i + 1000):
                pipe.set(keys[j], j)
            await pipe.execute()

        info = await client.info("persistence")
        df.stop(kill=True)
        # The heartbeat sealed the open block first, so the kill tested no loss.
        if info["aof_open_block_bytes"] == 0:
            continue

        df.start()
        client = df.client()
        vals = await client.mget(keys)
        df.stop()

        # Exactly the first m writes survived, with their values.
        m = sum(v is not None for v in vals)
        assert vals[:m] == [str(i) for i in range(m)], "not a prefix of the writes"
        # Each SET is one record, so lsn n is key n-1.
        assert info["aof_written_lsn"] <= m <= info["aof_appended_lsn"]
        # Otherwise the heartbeat wrote the open block between INFO and the kill.
        if m < info["aof_appended_lsn"]:
            return
    pytest.fail("the open block was written before the kill in all attempts; no loss tested")


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 & -(crc & 1))
    return crc ^ 0xFFFFFFFF


def read_manifest(aof_dir) -> str:
    return (aof_dir / "appendonly.manifest").read_text()


def write_manifest(aof_dir, body: str):
    # The crc32c line covers every byte before it.
    (aof_dir / "appendonly.manifest").write_text(body + f"crc32c {crc32c(body.encode()):08x}\n")


@dfly_args({"proactor_threads": 2})
async def test_bootstrap_adopts_dump(df_factory: DflyInstanceFactory, tmp_path):
    plain = df_factory.create(dir=str(tmp_path), dbfilename="dump-{{timestamp}}")
    plain.start()
    client = plain.client()
    await client.mset({f"key:{i}": i for i in range(100)})
    await client.set("counter", 10)
    await client.execute_command("SAVE")
    plain.stop()

    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump-{{timestamp}}")
    df.start()
    client = df.client()
    assert await client.dbsize() == 101
    assert re.search(r"base dfs user dump-.*-summary\.dfs\n", read_manifest(tmp_path))
    await client.incr("counter")
    await client.mset({"new:1": "a", "new:2": "b"})
    df.stop()

    # The log replays on top of the adopted dump, once.
    df.start()
    client = df.client()
    assert await client.dbsize() == 103
    assert await client.get("counter") == "11"
    assert await client.mget("new:1", "new:2") == ["a", "b"]


async def test_aof_files_without_manifest_refuse_start(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path))
    df.start()
    df.stop()

    (tmp_path / "appendonly.manifest").unlink()
    with pytest.raises(DflyStartException):
        df.start()


async def test_interrupted_bootstrap_is_redone(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path))
    df.start()
    await df.client().set("logged", 1)
    df.stop()

    # Turn the manifest back to bootstrapping, as if the first start had crashed.
    text = read_manifest(tmp_path)
    body = text[: text.rindex("crc32c ")]
    assert f"crc32c {crc32c(body.encode()):08x}\n" == text[len(body) :]
    write_manifest(tmp_path, body.replace("state active", "state bootstrapping"))

    # The redo drops the old log, and there is no dump.
    df.start()
    assert await df.client().dbsize() == 0
    assert "state active" in read_manifest(tmp_path)


@dfly_args({"proactor_threads": 1, "num_shards": 1})
async def test_replay_starts_at_cut(df_factory: DflyInstanceFactory, tmp_path):
    # One shard, so k<n> is lsn n. Each start resumes in a new segment: k1..k5 in 0, k6..k10 in 1.
    df = df_factory.create(aof=True, dir=str(tmp_path))
    for first in (1, 6):
        df.start()
        client = df.client()
        for i in range(first, first + 5):
            await client.set(f"k{i}", i)
        df.stop()

    # TODO(#8410): take a real checkpoint instead of faking one.
    # Fake a checkpoint at segment 1: k1..k5 count as in the (empty) base.
    text = read_manifest(tmp_path)
    body = text[: text.rindex("crc32c ")]
    assert "cut 0 0 1\n" in body

    # A cut that is not where its segment starts is rejected.
    write_manifest(tmp_path, body.replace("cut 0 0 1\n", "cut 0 1 7\n"))
    with pytest.raises(DflyStartException):
        df.start()

    write_manifest(tmp_path, body.replace("cut 0 0 1\n", "cut 0 1 6\n"))
    df.start()
    client = df.client()
    assert await client.dbsize() == 5
    assert await client.mget([f"k{i}" for i in range(1, 11)]) == [None] * 5 + [
        str(i) for i in range(6, 11)
    ]
    # Segment 0 is below the cut, so startup deleted it.
    assert not (tmp_path / "appendonly-0-0.aof").exists()


@dfly_args({"proactor_threads": 2})
async def test_deleted_base_refuses_start(df_factory: DflyInstanceFactory, tmp_path):
    plain = df_factory.create(dir=str(tmp_path), dbfilename="dump-{{timestamp}}")
    plain.start()
    await plain.client().set("key", 1)
    await plain.client().execute_command("SAVE")
    plain.stop()

    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump-{{timestamp}}")
    df.start()
    df.stop()

    for summary in tmp_path.glob("dump-*-summary.dfs"):
        summary.unlink()
    with pytest.raises(DflyStartException):
        df.start()


async def wait_log_written(client):
    # So a kill loses nothing: every record is sealed and written.
    while True:
        info = await client.info("persistence")
        if info["aof_open_block_bytes"] == 0 and info["aof_buffered_bytes"] == 0:
            return
        await asyncio.sleep(0.05)


@dfly_args({"proactor_threads": 2})
async def test_save_is_checkpoint(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump-{{timestamp}}")
    df.start()
    client = df.client()
    await client.mset({f"key:{i}": i for i in range(100)})
    await client.set("counter", 10)
    await client.execute_command("SAVE")

    # The save is the base, and each shard's log continues in segment 1.
    manifest = read_manifest(tmp_path)
    assert "checkpoint 1\n" in manifest
    assert re.search(r"base dfs user dump-.*-summary\.dfs\n", manifest)
    assert set(re.findall(r"^cut \d+ (\d+) ", manifest, re.M)) == {"1"}
    assert not list(tmp_path.glob("appendonly-*-0.aof"))

    await client.incr("counter")
    df.stop()
    df.start()
    client = df.client()
    assert await client.dbsize() == 101
    # The log replays on top of the save, once.
    assert await client.get("counter") == "11"


async def test_save_must_keep_the_base(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump-{{timestamp}}")
    df.start()
    client = df.client()
    with pytest.raises(redis.exceptions.ResponseError, match="timestamp"):
        await client.execute_command("SAVE", "DF", "fixed")
    with pytest.raises(redis.exceptions.ResponseError, match="DFS"):
        await client.execute_command("SAVE", "RDB")

    # {timestamp} has whole seconds: back-to-back saves land in the same second, and the later one
    # must fail instead of renaming over the base.
    await client.set("key", 1)
    for _ in range(10):
        try:
            await client.execute_command("SAVE")
        except redis.exceptions.ResponseError as e:
            assert "already exists" in str(e)
            break
    else:
        pytest.fail("no two saves landed in the same second")
    df.stop()
    df.start()
    assert await df.client().get("key") == "1"


async def test_fixed_dbfilename_refuses_start(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump")
    with pytest.raises(DflyStartException):
        df.start()


@dfly_args({"proactor_threads": 2})
async def test_crash_during_and_after_checkpoint(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump-{{timestamp}}")
    df.start()
    client = df.client()
    value = "x" * 500
    for i in range(0, 20_000, 1000):
        await client.mset({f"key:{j}": value for j in range(i, i + 1000)})
    await client.set("counter", 10)
    await wait_log_written(client)

    # Killed while the save runs: the old manifest and the whole log still hold the data.
    await client.execute_command("BGSAVE")
    df.stop(kill=True)
    df.start()
    client = df.client()
    assert await client.dbsize() == 20_001
    assert await client.get("counter") == "10"

    # Killed right after a save committed: the new base plus the log after its cut.
    await client.execute_command("SAVE")
    await client.incr("counter")
    await wait_log_written(client)
    df.stop(kill=True)
    df.start()
    client = df.client()
    assert await client.dbsize() == 20_001
    assert await client.get("counter") == "11"


@dfly_args({"proactor_threads": 4})
async def test_checkpoint_then_log_restores_data(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump-{{timestamp}}")
    df.start()
    client = df.client()

    seeder = SeederV2(key_target=100_000)
    await seeder.run(client, target_deviation=0.1)
    await client.execute_command("SAVE")
    assert "checkpoint 1\n" in read_manifest(tmp_path)
    at_cut = await SeederV2.capture(client)

    # Changes after the cut live only in the log, on top of the base.
    await seeder.run(client, target_ops=50_000)
    before = await SeederV2.capture(client)
    assert before != at_cut
    df.stop()

    df.start()
    assert await SeederV2.capture(df.client()) == before
