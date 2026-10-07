import pytest

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
    plain = df_factory.create(dir=str(tmp_path), dbfilename="dump")
    plain.start()
    client = plain.client()
    await client.mset({f"key:{i}": i for i in range(100)})
    await client.set("counter", 10)
    await client.execute_command("SAVE")
    plain.stop()

    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump")
    df.start()
    client = df.client()
    assert await client.dbsize() == 101
    assert "base dfs user dump-summary.dfs" in read_manifest(tmp_path)
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
    plain = df_factory.create(dir=str(tmp_path), dbfilename="dump")
    plain.start()
    await plain.client().set("key", 1)
    await plain.client().execute_command("SAVE")
    plain.stop()

    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump")
    df.start()
    df.stop()

    (tmp_path / "dump-summary.dfs").unlink()
    with pytest.raises(DflyStartException):
        df.start()
