import pytest

from . import dfly_args
from .instance import DflyInstanceFactory
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
