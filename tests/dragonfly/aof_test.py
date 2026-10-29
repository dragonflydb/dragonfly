import pytest

from . import dfly_args
from .instance import DflyInstanceFactory, DflyStartException
from .seeder import Seeder as SeederV2


# --aof needs io_uring, so startup fails under --force_epoll.
@pytest.mark.exclude_epoll
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
    await client.connection_pool.disconnect()

    # A graceful stop seals, writes and syncs every shard's AOF.
    df.stop()
    df.start()
    client = df.client()
    assert await client.dbsize() == keys
    assert await SeederV2.capture(client) == before


# TODO: a test with TTLs: keys whose deadline passes during replay must survive until a later
# record (PERSIST, PEXPIRE) changes them. Deadlines that passed while the server was down need the
# time-based commands fix first, see docs/aof.md.


@pytest.mark.exclude_epoll
@dfly_args({"proactor_threads": 4})
async def test_replay_across_restarts(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path))
    expected = {}

    # Every restart replays the whole chain, then resumes each shard's log in a new segment.
    for run in range(3):
        df.start()
        client = df.client()
        assert await client.dbsize() == len(expected)
        for key, value in expected.items():
            assert await client.get(key) == value

        batch = {f"run{run}:{i}": f"{run}-{i}" for i in range(100)}
        await client.mset(batch)
        expected.update(batch)
        await client.connection_pool.disconnect()
        df.stop()

    df.start()
    client = df.client()
    assert await client.dbsize() == len(expected)
    for key, value in expected.items():
        assert await client.get(key) == value

    # Three runs before the last start: segments 0, 1 and 2 on every shard.
    first = sorted(p.name.split("-")[1] for p in tmp_path.glob("appendonly-*-0.aof"))
    last = sorted(p.name.split("-")[1] for p in tmp_path.glob("appendonly-*-2.aof"))
    assert first and first == last


@pytest.mark.exclude_epoll
@dfly_args({"proactor_threads": 4})
async def test_replay_global_commands(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path))
    df.start()
    client = df.client()

    # FLUSHALL is a global command: every shard's chain meets at it during replay.
    await client.mset({f"old{i}": i for i in range(100)})
    await client.flushall()
    await client.mset({f"new{i}": i for i in range(100)})
    await client.connection_pool.disconnect()
    df.stop()

    df.start()
    client = df.client()
    assert await client.dbsize() == 100
    assert await client.get("new0") == "0"
    assert await client.get("old0") is None

    # A global command after a restart gets a txid past the replayed ones, so the next replay
    # pairs it correctly across shards.
    await client.flushall()
    await client.set("last", "1")
    await client.connection_pool.disconnect()
    df.stop()

    df.start()
    client = df.client()
    assert await client.dbsize() == 1
    assert await client.get("last") == "1"


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


@pytest.mark.exclude_epoll
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


@pytest.mark.exclude_epoll
async def test_aof_files_without_manifest_refuse_start(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path))
    df.start()
    df.stop()

    (tmp_path / "appendonly.manifest").unlink()
    with pytest.raises(DflyStartException):
        df.start()


@pytest.mark.exclude_epoll
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


@pytest.mark.exclude_epoll
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


@pytest.mark.exclude_epoll
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
