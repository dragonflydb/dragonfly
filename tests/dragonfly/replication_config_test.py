import asyncio
import logging
import os
import platform
import shutil
import struct
import tarfile

import async_timeout
import pymemcache
import pytest
import redis

from . import dfly_args
from .instance import DflyInstanceFactory
from .replication_utils import (
    ADMIN_PORT,
    assert_debug_populate_synced,
    setup_replication,
    start_replication,
    wait_for_replica_status,
)
from .utility import (
    assert_eventually,
    batch_fill_data,
    check_all_replicas_finished,
    download_with_retries,
    gen_test_data,
    info_tick_timer,
    wait_available_async,
    wait_for_replicas_state,
)

DISCONNECT_CRASH_FULL_SYNC = 0
DISCONNECT_CRASH_STABLE_SYNC = 1
DISCONNECT_NORMAL_STABLE_SYNC = 2

M_OPT = [pytest.mark.opt_only]
M_SLOW = [pytest.mark.large]


@dfly_args({"proactor_threads": 4})
async def test_auth_master(df_factory, n_keys=20):
    masterpass = "requirepass"
    replicapass = "replicapass"
    master = df_factory.create(requirepass=masterpass)
    replica = df_factory.create(masterauth=masterpass, requirepass=replicapass)

    df_factory.start_all([master, replica])

    c_master = master.client(password=masterpass)
    c_replica = replica.client(password=replicapass)

    # Connect replica to master
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    # Set keys
    pipe = c_master.pipeline(transaction=False)
    batch_fill_data(pipe, gen_test_data(n_keys))
    await pipe.execute()

    # Check replica finished executing the replicated commands
    await check_all_replicas_finished([c_replica], c_master)
    # Check keys are on replica
    res = await c_replica.mget(k for k, _ in gen_test_data(n_keys))
    assert all(v is not None for v in res)
    await c_master.connection_pool.disconnect()
    await c_replica.connection_pool.disconnect()


# 1. Number of master threads
# 2. Number of threads for each replica
replication_cases = [(8, 8)]


@pytest.mark.parametrize("t_master, t_replica", replication_cases)
async def test_no_tls_on_admin_port(
    df_factory: DflyInstanceFactory,
    df_seeder_factory,
    t_master,
    t_replica,
    with_tls_server_args,
):
    # 1. Spin up dragonfly without tls, debug populate
    master = df_factory.create(
        no_tls_on_admin_port="true",
        admin_port=ADMIN_PORT,
        **with_tls_server_args,
        requirepass="XXX",
        proactor_threads=t_master,
    )
    master.start()
    c_master = master.admin_client(password="XXX")
    await c_master.execute_command("DEBUG POPULATE 100")
    db_size = await c_master.execute_command("DBSIZE")
    assert 100 == db_size

    # 2. Spin up a replica and initiate a REPLICAOF
    replica = df_factory.create(
        no_tls_on_admin_port="true",
        admin_port=ADMIN_PORT + 1,
        **with_tls_server_args,
        proactor_threads=t_replica,
        requirepass="XXX",
        masterauth="XXX",
    )
    replica.start()
    c_replica = replica.admin_client(password="XXX")
    res = await c_replica.execute_command("REPLICAOF localhost " + str(master.admin_port))
    assert "OK" == res

    # 3. Verify that replica dbsize == debug populate key size -- replication works
    await assert_debug_populate_synced(c_master, c_replica, 100, populate=False)


# 1. Number of master threads
# 2. Number of threads for each replica
# 3. Admin port
replication_cases = [(8, 8, False), (8, 8, True)]


@pytest.mark.parametrize("t_master, t_replica, test_admin_port", replication_cases)
async def test_tls_replication(
    df_factory,
    df_seeder_factory,
    t_master,
    t_replica,
    test_admin_port,
    with_ca_tls_server_args,
    with_ca_tls_client_args,
    proxy_factory,
):
    # 1. Spin up dragonfly tls enabled, debug populate
    master = df_factory.create(
        tls_replication="true",
        **with_ca_tls_server_args,
        port=1111,
        admin_port=ADMIN_PORT,
        proactor_threads=t_master,
    )
    master.start()
    c_master = master.client(**with_ca_tls_client_args)
    await c_master.execute_command("DEBUG POPULATE 100")
    db_size = await c_master.execute_command("DBSIZE")
    assert 100 == db_size

    proxy = await proxy_factory(master.port if not test_admin_port else master.admin_port)

    # 2. Spin up a replica and initiate a REPLICAOF
    replica = df_factory.create(
        tls_replication="true",
        **with_ca_tls_server_args,
        proactor_threads=t_replica,
    )
    replica.start()
    c_replica = replica.client(**with_ca_tls_client_args)
    res = await c_replica.execute_command("REPLICAOF localhost " + str(proxy.port))
    assert "OK" == res

    # 3. Verify that replica dbsize == debug populate key size -- replication works
    await assert_debug_populate_synced(c_master, c_replica, 100, populate=False)

    # 4. Break the connection between master and replica
    await proxy.close()
    await asyncio.sleep(3)
    await proxy.start_serving()

    # Check replica gets new keys
    await c_master.execute_command("SET MY_KEY 1")
    db_size = await c_master.execute_command("DBSIZE")
    assert 101 == db_size

    await check_all_replicas_finished([c_replica], c_master)
    db_size = await c_replica.execute_command("DBSIZE")
    assert 101 == db_size


@dfly_args({"proactor_threads": 2})
async def test_tls_replication_without_ca(
    df_factory,
    df_seeder_factory,
    with_tls_server_args,
    with_ca_tls_client_args,
):
    # 1. Spin up dragonfly tls enabled, debug populate
    master = df_factory.create(tls_replication="true", **with_tls_server_args, requirepass="hi")
    master.start()
    # Somehow redis-py forces to verify the certificate and it fails
    # TODO investigate why and remove with_ca_tls_clients_args
    c_master = master.client(password="hi", **with_ca_tls_client_args)
    await c_master.execute_command("DEBUG POPULATE 100")

    # 2. Spin up a replica and initiate a REPLICAOF
    replica = df_factory.create(
        tls_replication="true", **with_tls_server_args, masterauth="hi", requirepass="hi"
    )
    replica.start()

    c_replica = replica.client(password="hi", **with_ca_tls_client_args)

    res = await c_replica.execute_command("REPLICAOF localhost " + str(master.port))
    assert "OK" == res
    await assert_debug_populate_synced(c_master, c_replica, 100, populate=False)


@pytest.mark.exclude_epoll
async def test_ipv6_replication(df_factory: DflyInstanceFactory):
    """Test that IPV6 addresses work for replication, ::1 is 127.0.0.1 localhost"""
    master = df_factory.create(proactor_threads=1, bind="::1", port=1111)
    replica = df_factory.create(proactor_threads=1, bind="::1", port=1112)

    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()

    assert await c_master.ping()
    assert await c_replica.ping()
    assert await c_replica.execute_command("REPLICAOF", master["bind"], master["port"]) == "OK"


@pytest.mark.parametrize(
    "start_replica_first, disconnect_at_end",
    [
        (False, False),
        (True, False),
        (False, True),
    ],
)
async def test_replicaof_flag(df_factory, start_replica_first, disconnect_at_end):
    if start_replica_first:
        BASE_PORT = 1111
        # Replica starts before master, uses a fixed port so it knows where to connect
        replica = df_factory.create(
            proactor_threads=2,
            replicaof=f"localhost:{BASE_PORT}",
        )
        replica.start()
        c_replica = replica.client()
        await wait_for_replica_status(c_replica, status="down")

        # Check that it is in replica mode but status is down
        info = await c_replica.info("replication")
        assert info["role"] == "slave"
        assert info["master_host"] == "localhost"
        assert info["master_port"] == BASE_PORT
        assert info["master_link_status"] == "down"

        master = df_factory.create(
            port=BASE_PORT,
            proactor_threads=2,
        )
        master.start()
        c_master = master.client()
        await c_master.set("KEY", "VALUE")
        db_size = await c_master.dbsize()
        assert 1 == db_size

        await wait_for_replica_status(c_replica, status="up")
        await check_all_replicas_finished([c_replica], c_master)
    else:
        master = df_factory.create(proactor_threads=2)
        master.start()
        c_master = master.client()
        if disconnect_at_end:
            await wait_available_async(c_master)
        await c_master.set("KEY", "VALUE")
        db_size = await c_master.dbsize()
        assert 1 == db_size

        replica = df_factory.create(
            proactor_threads=2,
            replicaof=f"localhost:{master.port}",
        )
        replica.start()
        c_replica = replica.client()
        await wait_available_async(c_replica)
        await check_all_replicas_finished([c_replica], c_master)

    dbsize = await c_replica.dbsize()
    assert 1 == dbsize

    val = await c_replica.get("KEY")
    assert "VALUE" == val

    if disconnect_at_end:
        await c_replica.replicaof("no", "one")
        role = await c_replica.role()
        assert role[0] == "master"


async def test_df_crash_on_memcached_error(df_factory):
    master = df_factory.create(
        memcached_port=11211,
        proactor_threads=2,
    )

    replica = df_factory.create(
        memcached_port=master.mc_port + 1,
        proactor_threads=2,
    )

    master.start()
    replica.start()

    c_master = master.client()
    await wait_available_async(c_master)

    c_replica = replica.client()
    await start_replication(c_replica, master.port)

    memcached_client = pymemcache.Client(f"127.0.0.1:{replica.mc_port}")

    with pytest.raises(pymemcache.exceptions.MemcacheServerError):
        memcached_client.set("key", "data", noreply=False)


async def test_df_crash_on_replicaof_flag(df_factory):
    master = df_factory.create(
        proactor_threads=2,
    )
    master.start()

    replica = df_factory.create(proactor_threads=2, replicaof=f"127.0.0.1:{master.port}")
    replica.start()

    c_master = master.client()
    c_replica = replica.client()

    await wait_available_async(c_master)
    await wait_available_async(c_replica)

    res = await c_replica.execute_command("SAVE DF myfile")
    assert "OK" == res

    res = await c_replica.execute_command("DBSIZE")
    assert res == 0


async def test_user_acl_replication(df_factory):
    master = df_factory.create(proactor_threads=4)
    replica = df_factory.create(proactor_threads=4)
    df_factory.start_all([master, replica])

    c_master = master.client()
    await c_master.execute_command("ACL SETUSER tmp >tmp ON +ping +dfly +replconf")
    await c_master.execute_command("SET foo bar")
    assert 1 == await c_master.execute_command("DBSIZE")

    c_replica = replica.client()
    await c_replica.execute_command("CONFIG SET masteruser tmp")
    await c_replica.execute_command("CONFIG SET masterauth tmp")
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    await wait_available_async(c_replica)
    assert 1 == await c_replica.execute_command("DBSIZE")

    # revoke acl's from tmp
    await c_master.execute_command("ACL SETUSER tmp -replconf")
    async for info, breaker in info_tick_timer(c_replica, section="REPLICATION"):
        with breaker:
            assert info["master_link_status"] == "down"

    await c_master.execute_command("SET bar foo")

    # reinstate and let replication continue
    await c_master.execute_command("ACL SETUSER tmp +replconf")
    await check_all_replicas_finished([c_replica], c_master, 5)
    assert 2 == await c_replica.execute_command("DBSIZE")


@pytest.mark.replication(
    replica_args={"replica_announce_ip": "overrode-host", "announce_port": "1337"}
)
async def test_announce_ip_port(replication):
    master, [replica], c_master, [c_replica] = replication

    role, node = await c_master.execute_command("role")
    assert role == "master"
    host, port, _ = node[0]
    assert host == "overrode-host"
    assert port == "1337"


def download_dragonfly_release(version):
    arch = platform.machine()  # e.g. "x86_64" or "aarch64"
    path = f"/tmp/old_df/{version}"
    binary = f"{path}/dragonfly-{arch}"
    if os.path.isfile(binary):
        return binary

    # Cleanup in case there's partial files
    if os.path.exists(path):
        shutil.rmtree(path)

    os.makedirs(path)
    gzfile = f"{path}/dragonfly.tar.gz"
    url = f"https://github.com/dragonflydb/dragonfly/releases/download/{version}/dragonfly-{arch}.tar.gz"
    logging.debug(f"Downloading Dragonfly release into {gzfile}...")
    download_with_retries(url, gzfile)

    # Extract
    file = tarfile.open(gzfile)
    file.extractall(path)
    file.close()

    # Return path
    return binary


@pytest.mark.parametrize(
    "cluster_mode, announce_ip, announce_port",
    [
        ("", "localhost", 7000),
        ("emulated", "", 0),
        ("emulated", "localhost", 7000),
    ],
)
async def test_replicate_old_master(
    df_factory: DflyInstanceFactory, cluster_mode, announce_ip, announce_port
):
    cpu = platform.processor()
    if cpu != "x86_64":
        pytest.skip(f"Supported only on x64, running on {cpu}")

    dfly_version = "v1.19.2"
    released_dfly_path = download_dragonfly_release(dfly_version)
    master = df_factory.create(
        version=1.19,
        path=released_dfly_path,
        cluster_mode=cluster_mode,
    )
    replica = df_factory.create(
        cluster_mode=cluster_mode,
        cluster_announce_ip=announce_ip,
        announce_port=announce_port,
    )

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    assert (
        f"df-{dfly_version}"
        == (await c_master.execute_command("info", "server"))["dragonfly_version"]
    )
    assert dfly_version != (await c_replica.execute_command("info", "server"))["dragonfly_version"]

    await c_master.execute_command("set", "k1", "v1")

    assert await c_replica.execute_command(f"REPLICAOF localhost {master.port}") == "OK"
    await wait_available_async(c_replica)

    assert await c_replica.execute_command("get", "k1") == "v1"


# This Test was intorduced in response to a bug when replicating empty hashmaps (encoded as
# ziplists) created with HSET, HSETEX, HDEL and then replicated 2 times.
# For more information plz refer to the issue on gh:
# https://github.com/dragonflydb/dragonfly/issues/3504
@dfly_args({"proactor_threads": 1})
async def test_empty_hash_map_replicate_old_master(df_factory):
    cpu = platform.processor()
    if cpu != "x86_64":
        pytest.skip(f"Supported only on x64, running on {cpu}")

    dfly_version = "v1.21.2"
    released_dfly_path = download_dragonfly_release(dfly_version)
    # old versions
    instances = [df_factory.create(path=released_dfly_path, version=1.21) for i in range(3)]
    # new version
    instances.append(df_factory.create())

    df_factory.start_all(instances)

    old_c_master = instances[0].client()
    # Create an empty hashmap
    await old_c_master.execute_command("HSET foo a_field a_value")
    await old_c_master.execute_command("HSETEX foo 2 b_field b_value")
    await old_c_master.execute_command("HDEL foo a_field")

    @assert_eventually
    async def check_if_empty():
        assert await old_c_master.execute_command("HGETALL foo") == []

    await check_if_empty()
    assert await old_c_master.execute_command("EXISTS foo") == 1
    await old_c_master.aclose()

    async def assert_body(client, result=1, state="online", node_role="slave"):
        async with async_timeout.timeout(10):
            await wait_for_replicas_state(client, state=state, node_role=node_role)

        assert await client.execute_command("EXISTS foo") == result
        assert await client.execute_command("REPLTAKEOVER 1") == "OK"

    index = 0
    last_old_replica = 2

    # Adjacent pairs
    for a, b in zip(instances, instances[1:]):
        logging.debug(index)
        client_b = b.client()
        assert await client_b.execute_command(f"REPLICAOF localhost {a.port}") == "OK"

        if index != last_old_replica:
            await assert_body(client_b, state="stable_sync", node_role="replica")
        else:
            await assert_body(client_b, result=0)

        index = index + 1
        await client_b.aclose()


# This Test was intorduced in response to a bug when replicating empty hash maps with
# HSET, HSETEX, HDEL and then loaded via replication.
# For more information plz refer to the issue on gh:
# https://github.com/dragonflydb/dragonfly/issues/3504
@dfly_args({"proactor_threads": 1})
async def test_empty_hashmap_loading_bug(df_factory: DflyInstanceFactory):
    cpu = platform.processor()
    if cpu != "x86_64":
        pytest.skip(f"Supported only on x64, running on {cpu}")

    dfly_version = "v1.21.2"
    released_dfly_path = download_dragonfly_release(dfly_version)
    master = df_factory.create(path=released_dfly_path, version=1.21)
    master.start()

    c_master = master.client()
    # Create an empty hashmap
    await c_master.execute_command("HSET foo a_field a_value")
    await c_master.execute_command("HSETEX foo 2 b_field b_value")
    await c_master.execute_command("HDEL foo a_field")

    @assert_eventually
    async def check_if_empty():
        assert await c_master.execute_command("HGETALL foo") == []

    await check_if_empty()
    assert await c_master.execute_command("EXISTS foo") == 1

    replica = df_factory.create()
    replica.start()
    c_replica = replica.client()

    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_for_replicas_state(c_replica)
    assert await c_replica.execute_command("dbsize") == 0


async def test_replicate_search_index_to_old_replica(df_factory: DflyInstanceFactory):
    """
    Test that a new master with search indices (including HNSW vector index) can
    replicate to a v1.35 replica. This verifies backward compatibility of replication
    when search indices are defined, ensuring the replica receives the data without
    errors from new RDB AUX fields (search-index, hnsw-index-metadata, HNSW opcodes).
    """
    cpu = platform.processor()
    if cpu != "x86_64":
        pytest.skip(f"Supported only on x64, running on {cpu}")

    dfly_version = "v1.35.1"
    released_dfly_path = download_dragonfly_release(dfly_version)

    # New master (current version) with search index
    master = df_factory.create(proactor_threads=2)
    # Old replica (v1.35)
    replica = df_factory.create(
        version=1.35,
        path=released_dfly_path,
        proactor_threads=2,
    )

    df_factory.start_all([master, replica])

    c_master = master.client()
    c_replica = replica.client()

    # Create a search index with HNSW vector field on the new master
    await c_master.execute_command(
        "FT.CREATE",
        "test_idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "item:",
        "SCHEMA",
        "name",
        "TEXT",
        "price",
        "NUMERIC",
        "SORTABLE",
        "category",
        "TAG",
        "embedding",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "2",
        "DISTANCE_METRIC",
        "L2",
    )

    # Insert test data with vector embeddings
    for i in range(100):
        category = "electronics" if i % 2 == 0 else "clothing"
        embedding = struct.pack("<2f", float(i), float(i * 2))
        await c_master.hset(
            f"item:{i}",
            mapping={
                "name": f"Product {i}",
                "price": str(i * 10),
                "category": category,
                "embedding": embedding,
            },
        )

    # Verify data and index on master
    assert await c_master.dbsize() == 100
    master_idx = c_master.ft("test_idx")
    text_result = await master_idx.search("Product 50")
    assert text_result.total >= 1

    # Verify KNN search on master. NOCONTENT: only the doc keys are checked, and it
    # avoids returning the raw binary embedding that a decode_responses client cannot decode.
    query_vec = struct.pack("<2f", 50.0, 100.0)
    knn_result = await c_master.execute_command(
        "FT.SEARCH",
        "test_idx",
        "*=>[KNN 2 @embedding $vec]",
        "NOCONTENT",
        "PARAMS",
        "2",
        "vec",
        query_vec,
    )
    assert knn_result[0] >= 1
    assert "item:50" in knn_result

    # Start replication from new master to old replica
    await start_replication(c_replica, master.port)

    # Verify data replicated successfully
    assert await c_replica.dbsize() == 100
    assert await c_replica.hget("item:0", "name") == "Product 0"
    assert await c_replica.hget("item:99", "name") == "Product 99"

    # Verify KNN search works on old replica (index rebuilt from replicated data)
    knn_result = await c_replica.execute_command(
        "FT.SEARCH",
        "test_idx",
        "*=>[KNN 2 @embedding $vec]",
        "NOCONTENT",
        "PARAMS",
        "2",
        "vec",
        query_vec,
    )
    assert knn_result[0] >= 1
    assert "item:50" in knn_result


@dfly_args({"proactor_threads": 2})
async def test_replicaof_does_not_flush_if_it_fails_to_connect(df_factory):
    master = df_factory.create(proactor_threads=2)
    replica = df_factory.create(proactor_threads=2)

    df_factory.start_all([master, replica])
    c_master = master.client()
    c_replica = replica.client()

    await c_master.execute_command("SET foo bar")
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await check_all_replicas_finished([c_replica], c_master)

    res = await c_replica.execute_command("dbsize")
    assert res == 1
    with pytest.raises(redis.exceptions.ResponseError):
        await c_replica.execute_command(f"REPLICAOF localhost {replica.port}")
    res = await c_replica.execute_command("dbsize")
    assert res == 1


@dfly_args({"proactor_threads": 2})
async def test_replicaof_inside_multi(df_factory):
    master = df_factory.create()
    replica = df_factory.create()
    df_factory.start_all([master, replica])

    async def replicate_inside_multi():
        try:
            c_master = master.client()
            p = c_master.pipeline(transaction=True)
            for i in range(5):
                p.execute_command("dbsize")
            p.execute_command(f"replicaof localhost {replica.port}")
            await p.execute()
            return True
        except redis.exceptions.ResponseError:
            return False

    MULTI_COMMANDS_TO_ISSUE = 30
    replication_commands = [
        asyncio.create_task(replicate_inside_multi()) for _ in range(MULTI_COMMANDS_TO_ISSUE)
    ]

    num_successes = 0
    for result in asyncio.as_completed(replication_commands, timeout=80):
        num_successes += await result

    logging.info(f"succeses: {num_successes}")
    assert MULTI_COMMANDS_TO_ISSUE == num_successes


"""
Test replication with mismatched dbnum between master and replica.
"""


@pytest.mark.parametrize("master_dbnum, replica_dbnum", [(8, 4), (4, 8)])
@dfly_args({"proactor_threads": 2})
async def test_replication_mismatched_dbnum(
    df_factory: DflyInstanceFactory, master_dbnum, replica_dbnum
):
    """
    Test replication with mismatched dbnum between master and replica.
    When the replica has fewer DBs than the master, replication should still succeed for
    the shared DBs (0..min-1). When the replica has more DBs, the extra DBs remain empty.
    """
    shared_dbs = min(master_dbnum, replica_dbnum)
    master, [replica], c_master, [c_replica] = await setup_replication(
        df_factory,
        master_args={"dbnum": master_dbnum},
        replica_args={"dbnum": replica_dbnum},
        connect=False,
    )

    # Populate data only in shared DBs (within both master's and replica's range)
    for db in range(shared_dbs):
        c = master.client(db=db)
        for i in range(50):
            await c.set(f"key:{db}:{i}", f"val:{db}:{i}")
        await c.close()

    # Start replication
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")

    async with async_timeout.timeout(10):
        await wait_for_replicas_state(c_replica)

    await check_all_replicas_finished([c_replica], c_master)

    # Verify all data is present in the replica across shared DBs
    for db in range(shared_dbs):
        c_m = master.client(db=db)
        c_r = replica.client(db=db)
        for i in range(50):
            assert await c_r.get(f"key:{db}:{i}") == await c_m.get(f"key:{db}:{i}")
        await c_m.close()
        await c_r.close()

    # Verify extra DBs on replica are empty (when replica has more DBs than master)
    if replica_dbnum > master_dbnum:
        for db in range(master_dbnum, replica_dbnum):
            c_r = replica.client(db=db)
            assert await c_r.dbsize() == 0
            await c_r.close()


async def test_v134_replica_crashes_v138_master_on_reconnect(df_factory: DflyInstanceFactory):
    dfly_version = "v1.34.2"
    released_dfly_path = download_dragonfly_release(dfly_version)

    old_master = df_factory.create(path=released_dfly_path, version=1.34, proactor_threads=2)
    new_master = df_factory.create(proactor_threads=2)
    old_replica = df_factory.create(path=released_dfly_path, version=1.34, proactor_threads=2)
    df_factory.start_all([old_master, new_master, old_replica])

    c_old_master = old_master.client()
    c_new_master = new_master.client()
    c_old_replica = old_replica.client()

    await c_old_master.set("key", "value")

    # Both new_master and old_replica replicate from old_master and reach stable sync.
    await start_replication(c_new_master, old_master.port)
    await start_replication(c_old_replica, old_master.port)

    await c_new_master.execute_command("REPLTAKEOVER 5")

    await c_old_replica.execute_command(f"REPLICAOF localhost {new_master.port}")

    @assert_eventually(timeout=10)
    async def replica_connected():
        info = await c_new_master.info("replication")
        assert info.get("connected_slaves", 0) == 1

    await replica_connected()

    # Trigger cleanup: disconnect old_replica so new_master runs ~SliceSnapshot.
    await c_old_replica.execute_command("REPLICAOF NO ONE")

    @assert_eventually(timeout=5)
    async def new_master_alive():
        assert new_master.proc.poll() is None, (
            f"new_master (v1.38+) crashed (exit={new_master.proc.poll()}) when a v1.34 replica "
            "reconnected after promotion — regression of the v1.34→v1.38 rolling-upgrade crash."
        )

    await new_master_alive()
