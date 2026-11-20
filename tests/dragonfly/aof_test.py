import asyncio
import re

import async_timeout
import pytest
import redis

from . import dfly_args
from .instance import DflyInstanceFactory, DflyStartException
from .seeder import Seeder as SeederV2
from .utility import check_all_replicas_finished, wait_for_replicas_state


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
    # So a kill loses nothing: every record is sealed and written. The buffered bytes, listed per
    # shard, include the open block.
    async with async_timeout.timeout(10):
        while True:
            info = await client.info("persistence")
            if all(int(v) == 0 for v in str(info["aof_buffered_bytes"]).split(",")):
                return
            await asyncio.sleep(0.05)


@pytest.mark.exclude_epoll
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


@pytest.mark.exclude_epoll
async def test_save_must_keep_the_base(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump-{{timestamp}}")
    df.start()
    client = df.client()
    with pytest.raises(redis.exceptions.ResponseError, match="timestamp"):
        await client.execute_command("SAVE DF fixed")
    with pytest.raises(redis.exceptions.ResponseError, match="DFS"):
        await client.execute_command("SAVE RDB")
    # The manifest names the base on one line; strftime turns %n into a newline.
    with pytest.raises(redis.exceptions.ResponseError, match="newlines"):
        await client.execute_command("SAVE DF dump-{timestamp}%n")

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


@pytest.mark.exclude_epoll
async def test_fixed_dbfilename_refuses_start(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump")
    with pytest.raises(DflyStartException):
        df.start()


@pytest.mark.exclude_epoll
async def test_dbfilename_with_newline_refuses_start(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump-{{timestamp}}%n")
    with pytest.raises(DflyStartException):
        df.start()


@pytest.mark.exclude_epoll
@dfly_args({"proactor_threads": 2})
async def test_search_metadata_survives_checkpoint_and_replay(
    df_factory: DflyInstanceFactory, tmp_path
):
    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump-{{timestamp}}")
    df.start()
    client = df.client()
    await client.execute_command("FT.CREATE idx ON HASH SCHEMA f TEXT")

    # Aliases and synonyms live only in the summary: the ones before the save come from the base,
    # the ones after it from the log.
    await client.execute_command("FT.ALIASADD before idx")
    await client.execute_command("FT.SYNUPDATE idx g1 car auto")
    await client.execute_command("SAVE")
    await client.execute_command("FT.ALIASADD after idx")
    await client.execute_command("FT.SYNUPDATE idx g2 bike cycle")
    df.stop()

    df.start()
    client = df.client()
    for alias in ("before", "after"):
        await client.execute_command(f"FT.INFO {alias}")
    synonyms = await client.execute_command("FT.SYNDUMP idx")
    assert {"car", "auto", "bike", "cycle"} <= set(synonyms[::2])


@pytest.mark.exclude_epoll
@dfly_args({"proactor_threads": 2})
async def test_crash_during_and_after_checkpoint(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path), dbfilename="dump-{{timestamp}}")
    df.start()
    client = df.client()

    # Enough data that the kill lands while BGSAVE still runs.
    seeder = SeederV2(key_target=20_000, data_size=500)
    await seeder.run(client, target_deviation=0.1)
    await client.set("counter", 10)
    before = await SeederV2.capture(client)
    await wait_log_written(client)

    # Killed while the save runs: the old manifest and the whole log still hold the data.
    await client.execute_command("BGSAVE")
    df.stop(kill=True)
    df.start()
    client = df.client()
    assert await SeederV2.capture(client) == before

    # Killed right after a save committed: the new base plus the log after its cut. Its own name, as
    # the BGSAVE may have finished before the kill within the same second.
    await client.execute_command("SAVE DF after-{timestamp}")
    await client.incr("counter")
    before = await SeederV2.capture(client)
    await wait_log_written(client)
    df.stop(kill=True)
    df.start()
    client = df.client()
    assert await SeederV2.capture(client) == before
    # The log replays on top of the save, once.
    assert await client.get("counter") == "11"


@pytest.mark.exclude_epoll
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


# TODO: test a record cut at the end of the log, once replay can resume after it: kill during a
# burst, so the last written block ends mid-record, and check that the restart keeps every whole
# record and that a later restart still replays the resumed segment.
@pytest.mark.exclude_epoll
@pytest.mark.debug_only
@dfly_args({"proactor_threads": 2, "num_shards": 1})
async def test_kill_loses_only_unwritten_tail(df_factory: DflyInstanceFactory, tmp_path):
    # The bulk is written before the kill; the batch fits one block, so it stays in memory until
    # the next heartbeat seals it. Every record is whole: a size seal could split one across blocks,
    # and replay cannot resume after a record cut at the end of the log yet.
    bulk, batch = 50_000, 100
    keys = [f"key:{i}" for i in range(bulk + batch)]

    for attempt in range(10):
        # A slow heartbeat leaves the batch unsealed for up to a second.
        aof_dir = tmp_path / f"attempt{attempt}"
        aof_dir.mkdir()
        df = df_factory.create(aof=True, hz=1, dir=str(aof_dir))
        df.start()
        client = df.client()

        for i in range(0, bulk, 1000):
            pipe = client.pipeline(transaction=False)
            for j in range(i, i + 1000):
                pipe.set(keys[j], j)
            await pipe.execute()
        await wait_log_written(client)

        pipe = client.pipeline(transaction=False)
        for j in range(bulk, bulk + batch):
            pipe.set(keys[j], j)
        await pipe.execute()
        df.stop(kill=True)

        df.start()
        client = df.client()
        vals = await client.mget(keys)
        df.stop()

        # The first m writes survived, with their values, and nothing after them.
        m = sum(v is not None for v in vals)
        assert vals[:m] == [str(i) for i in range(m)], "not a prefix of the writes"
        assert m >= bulk, "lost writes that were already written"
        # Otherwise the heartbeat wrote the batch before the kill, and nothing was at risk.
        if m < bulk + batch:
            return
    pytest.fail("the batch was written before the kill in all attempts; no loss tested")


async def wait_for_manifest(aof_dir, text: str, timeout=10):
    # The checkpoint fiber wakes once a second.
    for _ in range(timeout * 10):
        if text in read_manifest(aof_dir):
            return
        await asyncio.sleep(0.1)
    pytest.fail(f"the manifest never had {text!r}:\n{read_manifest(aof_dir)}")


@pytest.mark.exclude_epoll
@dfly_args({"proactor_threads": 2, "aof_rewrite_min_size": 100_000})
async def test_auto_checkpoint(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path))
    df.start()
    client = df.client()

    # The log outgrows --aof_rewrite_min_size, so a checkpoint runs without any save.
    await client.mset({f"a:{i}": "x" * 100 for i in range(2000)})
    await wait_for_manifest(tmp_path, "base dfs owned appendonly-base-1-summary.dfs\n")
    info = await client.info("persistence")
    assert info["aof_base_file"] == "appendonly-base-1-summary.dfs"
    assert info["aof_checkpoint_failures"] == 0

    # The next one becomes the base, and the previous owned base is deleted.
    await client.mset({f"b:{i}": "y" * 100 for i in range(4000)})
    await wait_for_manifest(tmp_path, "base dfs owned appendonly-base-2-summary.dfs\n")
    assert not list(tmp_path.glob("appendonly-base-1-*"))
    assert list(tmp_path.glob("appendonly-base-2-*"))

    # The owned base plus the log after its cut restore the data.
    await client.set("last", 1)
    before = await SeederV2.capture(client)
    df.stop()
    df.start()
    assert await SeederV2.capture(df.client()) == before


@pytest.mark.exclude_epoll
@dfly_args({"proactor_threads": 2, "aof_rewrite_min_size": 100_000, "aof_rewrite_percentage": 0})
async def test_auto_checkpoint_disabled(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(aof=True, dir=str(tmp_path))
    df.start()
    client = df.client()

    await client.mset({f"a:{i}": "x" * 100 for i in range(2000)})
    await asyncio.sleep(3)
    assert "checkpoint 0\n" in read_manifest(tmp_path)
    assert not list(tmp_path.glob("appendonly-base-*"))
    assert (await client.info("persistence"))["aof_rewrite_trigger_size"] == 0


@pytest.mark.exclude_epoll
async def test_replica_full_sync_rebases(df_factory: DflyInstanceFactory, tmp_path):
    master = df_factory.create(proactor_threads=2)
    replica = df_factory.create(proactor_threads=2, aof=True, dir=str(tmp_path))
    df_factory.start_all([master, replica])
    c_master, c_replica = master.client(), replica.client()
    seeder = SeederV2(key_target=20_000)
    await seeder.run(c_master, target_deviation=0.1)

    # The full sync loads data past the journal; the checkpoint after it covers that data.
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica)
    await wait_for_manifest(tmp_path, "base dfs owned appendonly-base-1-summary.dfs\n")
    assert (await c_replica.info("persistence"))["aof_state"] == "active"

    # Stable sync goes through the replica's journal into its AOF.
    await seeder.run(c_master, target_ops=5_000)
    await check_all_replicas_finished([c_replica], c_master)
    assert await SeederV2.capture(c_replica) == await SeederV2.capture(c_master)

    # Promotion keeps the journal LSNs the AOF chain continues from.
    await c_replica.execute_command("REPLICAOF NO ONE")
    await c_replica.set("after_promotion", 1)
    before = await SeederV2.capture(c_replica)
    replica.stop()
    replica.start()
    assert await SeederV2.capture(replica.client()) == before


@pytest.mark.exclude_epoll
async def test_restart_while_rebasing_starts_empty(df_factory: DflyInstanceFactory, tmp_path):
    df = df_factory.create(proactor_threads=2, aof=True, dir=str(tmp_path))
    df.start()
    await df.client().set("old", 1)
    df.stop()

    # As if a full sync had started, and the replica crashed before its new base committed.
    text = read_manifest(tmp_path)
    body = text[: text.rindex("crc32c ")]
    write_manifest(tmp_path, body.replace("state active", "state rebasing"))
    old_files = [p.name for p in tmp_path.glob("appendonly-*.aof")]
    assert old_files

    # The old log holds the data the sync was replacing: nothing is loaded, nothing is deleted.
    df.start()
    client = df.client()
    assert await client.dbsize() == 0
    assert (await client.info("persistence"))["aof_state"] == "rebasing"
    assert all((tmp_path / name).exists() for name in old_files)

    # A full sync gives the node its base; only then do the old files go.
    master = df_factory.create(proactor_threads=2)
    master.start()
    await master.client().set("new", 1)
    await client.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_manifest(tmp_path, "base dfs owned appendonly-base-1-summary.dfs\n")
    assert (await client.info("persistence"))["aof_state"] == "active"
    assert not any((tmp_path / name).exists() for name in old_files)
    assert await client.get("new") == "1"


@pytest.mark.exclude_epoll
async def test_replay_into_different_shard_count(df_factory: DflyInstanceFactory, tmp_path):
    def create(shards):
        return df_factory.create(aof=True, dir=str(tmp_path), proactor_threads=4, num_shards=shards)

    df = create(4)
    df.start()
    client = df.client()
    # A global command in the log, so the chains still meet at the barrier.
    await SeederV2(key_target=10_000).run(client, target_deviation=0.1)
    await client.flushall()
    seeder = SeederV2(key_target=20_000)
    await seeder.run(client, target_deviation=0.1)
    before = await SeederV2.capture(client)
    df.stop()

    # Fewer shards, then more: each start replays the old chains, then a checkpoint replaces them.
    for shards in (2, 3):
        df = create(shards)
        df.start()
        client = df.client()
        assert await SeederV2.capture(client) == before
        assert f"shard_count {shards}\n" in read_manifest(tmp_path)
        assert all(int(p.name.split("-")[1]) < shards for p in tmp_path.glob("appendonly-*-*.aof"))

        # The new chains log what comes next.
        await client.set(f"after:{shards}", 1)
        before = await SeederV2.capture(client)
        df.stop()

    df = create(3)
    df.start()
    assert await SeederV2.capture(df.client()) == before


@pytest.mark.exclude_epoll
@pytest.mark.parametrize("master_shards, replica_shards", [(4, 2), (2, 4)])
async def test_replica_with_different_shard_count(
    df_factory: DflyInstanceFactory, tmp_path, master_shards, replica_shards
):
    master = df_factory.create(proactor_threads=4, num_shards=master_shards)
    replica = df_factory.create(
        proactor_threads=4, num_shards=replica_shards, aof=True, dir=str(tmp_path)
    )
    df_factory.start_all([master, replica])
    c_master, c_replica = master.client(), replica.client()
    seeder = SeederV2(key_target=20_000)
    await seeder.run(c_master, target_deviation=0.1)

    # The replica's AOF follows its own shard count, whatever the master's is.
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica)
    await wait_for_manifest(tmp_path, "base dfs owned appendonly-base-1-summary.dfs\n")
    assert f"shard_count {replica_shards}\n" in read_manifest(tmp_path)

    # Stable sync, global commands included, reaches the replica's AOF.
    await c_master.flushall()
    await seeder.run(c_master, target_deviation=0.1)
    await check_all_replicas_finished([c_replica], c_master)
    assert await SeederV2.capture(c_replica) == await SeederV2.capture(c_master)

    await c_replica.execute_command("REPLICAOF NO ONE")
    await c_replica.set("after_promotion", 1)
    before = await SeederV2.capture(c_replica)
    replica.stop()
    replica.start()
    assert await SeederV2.capture(replica.client()) == before
