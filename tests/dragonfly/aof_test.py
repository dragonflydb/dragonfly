import pytest

from . import dfly_args
from .instance import DflyInstanceFactory
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
