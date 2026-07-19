"""Shared helpers and fixtures for the replication test suites.

This module centralizes the boilerplate that is repeated across the (many)
replication tests: spinning up a master with one or more replicas, wiring them
up with ``REPLICAOF`` and waiting for stable sync, comparing datasets and
asserting roles.

The main entry points are:

* :func:`setup_replication` -- imperative helper, use it directly when the
  topology depends on values only known at runtime (e.g. parametrized thread
  counts).
* :data:`replication` -- a pytest fixture wrapping :func:`setup_replication`.
  It is *decomposable* (the yielded :class:`ReplicationSetup` unpacks into
  ``master, replicas, c_master, c_replicas``) and *annotated* via the
  ``@pytest.mark.replication(...)`` marker, e.g.::

      @pytest.mark.replication(master_args={"proactor_threads": 4}, replicas=2)
      async def test_x(replication):
          master, replicas, c_master, c_replicas = replication
          ...
"""

import asyncio
import dataclasses
import logging
import time
from typing import List, Optional, Union

from redis import asyncio as aioredis

from .instance import DflyInstance, DflyInstanceFactory
from .seeder import Seeder as SeederV2
from .utility import check_all_replicas_finished, wait_available_async

ADMIN_PORT = 1211


# ---------------------------------------------------------------------------
# Dataset comparison
# ---------------------------------------------------------------------------


async def compare_datasets(c_master, c_replica):
    r_port = c_replica.connection_pool.connection_kwargs.get("port", "unknown")
    hash_script = """
    local type = ARGV[1]
    local res = {}
    for i, key in ipairs(KEYS) do
        local hash = 0
        if type == 'STRING' then
            hash = dragonfly.ihash(0, false, 'GET', key)
        elseif type == 'LIST' then
            hash = dragonfly.ihash(0, false, 'LRANGE', key, 0, -1)
        elseif type == 'SET' then
            hash = dragonfly.ihash(0, true, 'SMEMBERS', key)
        elseif type == 'ZSET' then
            hash = dragonfly.ihash(0, false, 'ZRANGE', key, 0, -1, 'WITHSCORES')
        elseif type == 'HASH' then
            hash = dragonfly.ihash(0, true, 'HGETALL', key)
        elseif type == 'JSON' then
            hash = dragonfly.ihash(0, false, 'JSON.GET', key)
        elseif type == 'STREAM' then
            hash = dragonfly.ihash(0, false, 'XRANGE', key, '-', '+')
        elseif type == 'SBF' then
            hash = dragonfly.ihash(0, false, 'BF.INFO', key)
        elseif type == 'CMS' then
            hash = dragonfly.ihash(0, false, 'CMS.INFO', key)
        elseif type == 'TOPK' then
            hash = dragonfly.ihash(0, true, 'TOPK.LIST', key)
        elseif type == 'CF' then
            hash = dragonfly.ihash(0, false, 'CF.INFO', key)
        end
        table.insert(res, hash)
    end
    return res
    """
    sha = await c_master.script_load(hash_script)
    await c_replica.script_load(hash_script)

    for t in SeederV2.DEFAULT_TYPES:
        m_keys, r_keys = set(), set()

        _SCAN_TYPE = {
            "JSON": "ReJSON-RL",
            "SBF": "MBbloom--",
            "CMS": "CMSk-TYPE",
            "TOPK": "TopK-TYPE",
            "CF": "MBbloomCF",
        }
        scan_type = _SCAN_TYPE.get(t, t)
        logging.info(f"Scanning keys for type {t}")
        cursor = "0"
        while True:
            res = await c_master.execute_command("SCAN", cursor, "TYPE", scan_type, "COUNT", 5000)
            cursor, keys = res[0], res[1]
            m_keys.update(keys)
            if int(cursor) == 0:
                break

        cursor = "0"
        while True:
            res = await c_replica.execute_command("SCAN", cursor, "TYPE", scan_type, "COUNT", 5000)
            cursor, keys = res[0], res[1]
            r_keys.update(keys)
            if int(cursor) == 0:
                break

        if m_keys != r_keys:
            logging.error(f"[{t}] P{r_port}: Key mismatch!")
            logging.error(f"[{t}] P{r_port}: Master only: {m_keys - r_keys}")
            logging.error(f"[{t}] P{r_port}: Replica only: {r_keys - m_keys}")

        common = sorted(list(m_keys & r_keys))
        if not common:
            continue

        logging.info(f"Comparing hashes for {len(common)} keys of type {t}")
        m_hashes = []
        for i in range(0, len(common), 500):
            batch = common[i : i + 500]
            res = await c_master.evalsha(sha, len(batch), *batch, t)
            m_hashes.extend(res)

        r_hashes = []
        for i in range(0, len(common), 500):
            batch = common[i : i + 500]
            res = await c_replica.evalsha(sha, len(batch), *batch, t)
            r_hashes.extend(res)

        for k, mh, rh in zip(common, m_hashes, r_hashes):
            if mh != rh:
                logging.error(
                    f"[{t}] P{r_port}: hash mismatch for key {k}: master_hash={mh}, replica_hash={rh}"
                )
                if t == "STRING":
                    m_v, r_v = await c_master.get(k), await c_replica.get(k)
                elif t == "LIST":
                    m_v, r_v = await c_master.lrange(k, 0, -1), await c_replica.lrange(k, 0, -1)
                elif t == "SET":
                    m_v, r_v = await c_master.smembers(k), await c_replica.smembers(k)
                elif t == "HASH":
                    m_v, r_v = await c_master.hgetall(k), await c_replica.hgetall(k)
                elif t == "ZSET":
                    m_v, r_v = await c_master.zrange(
                        k, 0, -1, withscores=True
                    ), await c_replica.zrange(k, 0, -1, withscores=True)
                elif t == "JSON":
                    m_v, r_v = await c_master.execute_command(
                        "JSON.GET", k
                    ), await c_replica.execute_command("JSON.GET", k)
                else:
                    m_v, r_v = None, None
                logging.error(f"[{t}] P{r_port}: Mismatch for key {k}: master={m_v}, replica={r_v}")


async def assert_replica_data_matches(c_master, c_replicas):
    """Assert all replicas match master; runs compare_datasets for detailed diff on failure."""
    if not isinstance(c_replicas, list):
        c_replicas = [c_replicas]
    await check_all_replicas_finished(c_replicas, c_master)
    hashes = await asyncio.gather(*(SeederV2.capture(c) for c in [c_master] + c_replicas))
    if len(set(hashes)) > 1:
        for c_replica in c_replicas:
            await compare_datasets(c_master, c_replica)
        assert False, "Replica data does not match master"


# ---------------------------------------------------------------------------
# Replication setup
# ---------------------------------------------------------------------------


async def start_replication(c_replica, master_port):
    """Issue REPLICAOF and wait until replica reaches stable sync."""
    await c_replica.execute_command(f"REPLICAOF localhost {master_port}")
    await wait_available_async(c_replica)


@dataclasses.dataclass
class ReplicationSetup:
    """Result of :func:`setup_replication`.

    Supports tuple decomposition::

        master, replicas, c_master, c_replicas = setup

    as well as the ``master``/``replica`` and ``c_master``/``c_replica``
    single-instance shortcuts.
    """

    master: DflyInstance
    replicas: List[DflyInstance]
    c_master: "object"
    c_replicas: List["object"]

    def __iter__(self):
        return iter((self.master, self.replicas, self.c_master, self.c_replicas))

    @property
    def replica(self) -> DflyInstance:
        return self.replicas[0]

    @property
    def c_replica(self):
        return self.c_replicas[0]


async def setup_replication(
    df_factory: DflyInstanceFactory,
    *,
    master_args: Optional[dict] = None,
    replica_args: Optional[dict] = None,
    replicas: Union[int, List[dict]] = 1,
    connect: bool = True,
    wait: bool = True,
) -> ReplicationSetup:
    """Create a master with ``replicas`` replicas and return their clients.

    ``replicas`` is either the number of replicas (each created with
    ``replica_args``) or a list of per-replica argument dicts.

    By default each replica is connected with ``REPLICAOF`` and the call waits
    for stable sync. Pass ``connect=False`` to only create/start the instances
    and their clients (useful for full-sync tests that populate the master
    before issuing ``REPLICAOF`` themselves). Returns a decomposable
    :class:`ReplicationSetup`.
    """
    if isinstance(replicas, int):
        replica_arg_list = [dict(replica_args or {}) for _ in range(replicas)]
    else:
        replica_arg_list = [dict(a) for a in replicas]

    master = df_factory.create(**(master_args or {}))
    replica_insts = [df_factory.create(**a) for a in replica_arg_list]
    df_factory.start_all([master] + replica_insts)

    c_master = master.client()
    c_replicas = [r.client() for r in replica_insts]
    if connect:
        for c_replica in c_replicas:
            await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
        if wait:
            for c_replica in c_replicas:
                await wait_available_async(c_replica)

    return ReplicationSetup(master, replica_insts, c_master, c_replicas)


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------


async def assert_debug_populate_synced(
    c_master, c_replica, n: int = 100, populate: bool = True, populate_args: str = ""
):
    """Assert master and replica both hold ``n`` keys.

    When ``populate`` is true, ``DEBUG POPULATE n [populate_args]`` is issued on
    the master first (useful for stable-sync tests). Pass ``populate=False`` to
    only verify an already-populated dataset (e.g. after a full sync).
    """
    if populate:
        cmd = f"DEBUG POPULATE {n}"
        if populate_args:
            cmd += f" {populate_args}"
        await c_master.execute_command(cmd)
    assert await c_master.execute_command("DBSIZE") == n
    await check_all_replicas_finished([c_replica], c_master)
    assert await c_replica.execute_command("DBSIZE") == n


def master_role_reply(replicas, state: str = "online", ip: str = "127.0.0.1"):
    """Expected reply of ``ROLE`` on a master with the given replica instances."""
    if not isinstance(replicas, list):
        replicas = [replicas]
    return ["master", [[ip, str(r.port), state] for r in replicas]]


def replica_role_reply(master, state: str = "online", host: str = "localhost"):
    """Expected reply of ``ROLE`` on a replica of the given master instance."""
    return ["slave", host, str(master.port), state]


# ---------------------------------------------------------------------------
# Metrics / status helpers
# ---------------------------------------------------------------------------


async def get_metric_value(inst, metric_name, sample_index=0):
    return (await inst.metrics())[metric_name].samples[sample_index].value


async def wait_for_replica_status(
    replica: aioredis.Redis, status: str, wait_for_seconds=0.01, timeout=20
):
    start = time.time()
    while (time.time() - start) < timeout:
        await asyncio.sleep(wait_for_seconds)

        info = await replica.info("replication")
        if info["master_link_status"] == status:
            return
    raise RuntimeError("Client did not become available in time!")
