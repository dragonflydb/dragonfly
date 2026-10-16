"""
Partial sync (psync) across failovers and cascaded replication topologies.

Each test re-points replicas and asserts which syncs must be partial, then checks that all
nodes hold the same data. Writes use APPEND, since SET is omit-optimized during full sync.
"""

import asyncio
import logging
import random
import re
import time
from collections import Counter
from contextlib import asynccontextmanager

import pytest
import redis

from .instance import DflyInstance
from .seeder import Seeder
from .utility import check_all_replicas_finished, wait_for_replicas_state

FLAGS = {"proactor_threads": 2, "experimental_cascaded_partial_sync": None}


class Node:
    def __init__(self, inst: DflyInstance):
        self.inst = inst
        self.c = inst.client()

    @property
    def port(self):
        return self.inst.port

    async def replicaof(self, up, psync: bool | None = None):
        """Re-points to `up`; if `psync` is set, asserts whether the sync was partial."""
        await self.c.execute_command(f"REPLICAOF localhost {up.port}")
        await wait_for_replicas_state(self.c)
        if psync is not None:
            await self.expect_psync(psync)

    async def expect_psync(self, psync: bool, timeout=5):
        # psync_successes is per REPLICAOF and is bumped right after STARTSTABLE, so poll briefly
        deadline = time.time() + timeout
        while True:
            got = int((await self.c.info("replication"))["psync_successes"])
            if got == int(psync) or time.time() > deadline:
                break
            await asyncio.sleep(0.05)
        assert got == int(psync), f"{self.port}: expected psync={psync}"

    async def promote(self, how="REPLICAOF NO ONE"):
        await self.c.execute_command(how)

    async def caught_up(self, up: "Node"):
        await check_all_replicas_finished([self.c], up.c)

    def stop(self):
        self.inst.stop()

    def restart(self):
        """Restarts on the same port, so replicas pointing at us reconnect."""
        port = self.port
        self.stop()
        self.inst.dynamic_port, self.inst.args["port"] = False, port
        self.inst.start()
        self.c = self.inst.client()


@pytest.fixture
def nodes(df_factory):
    def make(n, **kwargs):
        insts = [df_factory.create(**{**FLAGS, **kwargs}) for _ in range(n)]
        df_factory.start_all(insts)
        return [Node(i) for i in insts]

    return make


async def write(node: Node, n=50, prefix="k", val="v"):
    p = node.c.pipeline(transaction=False)
    for i in range(n):
        p.append(f"{prefix}{i}", val)
    await p.execute()


async def special_ops(node: Node, tag: str):
    """Commands with non-trivial journaling: Lua, MULTI, expiry, DEL and FLUSHDB in another db."""
    c, c1 = node.c, node.inst.client(db=1)
    await c.eval("return redis.call('APPEND', KEYS[1], ARGV[1])", 1, f"{tag}:lua", "x")
    async with c.pipeline(transaction=True) as p:
        await p.append(f"{tag}:multi", "x").incr(f"{tag}:cnt").execute()
    await c.set(f"{tag}:ttl", "v", ex=1000)
    await c.set(f"{tag}:expired", "v", px=1)
    await c.append(f"{tag}:del", "v")
    await c.delete(f"{tag}:del")
    await c1.append(f"{tag}:flushed", "v")
    await c1.flushdb()
    await c1.append(f"{tag}:db1", "v")
    await asyncio.sleep(0.01)
    await c.exists(f"{tag}:expired")  # deletes the expired key on the master


async def assert_special(node: Node, tag: str):
    c, c1 = node.c, node.inst.client(db=1)
    assert await c.get(f"{tag}:lua") == "x" and await c.get(f"{tag}:cnt") == "1"
    assert 0 < await c.ttl(f"{tag}:ttl") <= 1000
    assert not await c.exists(f"{tag}:expired", f"{tag}:del")
    assert await c1.keys(f"{tag}:*") == [f"{tag}:db1"]


@asynccontextmanager
async def traffic(node: Node, keys=1_000):
    """Runs mixed-type Seeder traffic on `node` (a master) for the duration of the block."""
    seeder = Seeder(units=4, key_target=keys, data_size=50)
    task = asyncio.create_task(seeder.run(node.c))
    await asyncio.sleep(0.2)
    try:
        yield
    finally:
        await seeder.stop(node.c)
        await task


async def settle(*nodes: Node):
    """Waits until every link of the chain a -> b -> c is caught up."""
    for up, down in zip(nodes, nodes[1:]):
        await down.caught_up(up)


async def chain(*nodes: Node):
    """chain(a, b, c) builds a -> b -> c and waits until all links are caught up."""
    for up, down in zip(nodes, nodes[1:]):
        await down.replicaof(up)
    await settle(*nodes)


async def assert_same_data(*nodes: Node):
    hashes = await asyncio.gather(*(Seeder.capture(n.c) for n in nodes))
    assert all(h == hashes[0] for h in hashes), hashes


SYNC_RE = re.compile(r"Started (partial|full) sync with localhost:(\d+)")


def assert_syncs(node: Node, partial=(), full=()):
    """Stops `node` and asserts the exact multiset of syncs it started, per upstream node."""
    node.stop()
    got = Counter(
        (kind, int(port))
        for line in node.inst.find_in_logs(SYNC_RE.pattern)
        for kind, port in SYNC_RE.findall(line)
    )
    expected = Counter([("partial", n.port) for n in partial] + [("full", n.port) for n in full])
    assert got == expected


@pytest.mark.parametrize("promote", ["REPLTAKEOVER 5", "REPLICAOF NO ONE"])
async def test_failover(nodes, promote):
    """
    Plain failover: a sibling replica follows the promoted one with a partial sync.

    M -> R1
      |-> R2

    R1 is promoted, R2 follows it:

    R1 -> R2 (partial)
    """
    m, r1, r2 = nodes(3)
    await write(m)
    await chain(m, r1)
    await chain(m, r2)
    async with traffic(m):
        await asyncio.sleep(0.5)
    await r1.caught_up(m)
    await r2.caught_up(m)

    await r1.promote(promote)
    async with traffic(r1):
        await r2.replicaof(r1, psync=True)
    await r2.caught_up(r1)
    await assert_same_data(r1, r2)

    assert_syncs(r2, full=[m], partial=[r1])


async def test_cascade(nodes):
    """
    Plain cascading replication: a leaf hops between its parent and grandparent with psync.

    M -> R1 -> R2

    Under traffic, R2 re-points to M (partial), then back to R1 (partial):

    M -> R1
      |-> R2 (partial)
    """
    m, r1, r2 = nodes(3)
    await write(m)
    await chain(m, r1, r2)

    async with traffic(m):
        await r2.replicaof(m, psync=True)
        await r2.replicaof(r1, psync=True)
    await settle(m, r1, r2)
    await assert_same_data(m, r1, r2)

    assert_syncs(r2, full=[r1], partial=[m, r1])


@pytest.mark.parametrize("reconnect_to", [0, 1])
async def test_deep_cascade(nodes, reconnect_to):
    """
    A deep leaf can skip any number of intermediate nodes with a partial sync.

    M -> R1 -> R2 -> R3

    - R3 re-points to M or R1 under traffic (partial)
    - Lua, MULTI, expiry, DEL and FLUSHDB propagate through the chain and across the psync
    """
    ns = nodes(4)
    m, r2, r3, target = ns[0], ns[2], ns[3], ns[reconnect_to]
    await write(m)
    await chain(*ns)

    await special_ops(m, "pre")
    await settle(*ns)
    await assert_special(r3, "pre")

    async with traffic(m):
        await r3.replicaof(target, psync=True)
        await special_ops(m, "post")
    await settle(*ns[: reconnect_to + 1], r3)
    await assert_same_data(m, r3)
    await assert_special(r3, "post")

    assert_syncs(r3, full=[r2], partial=[target])


async def test_outage_middle(nodes):
    """
    The middle node dies; its orphan re-attaches to the master with a partial sync.

    M -> R1 (dies) -> R2

    M keeps writing, R2 re-points:

    M -> R2 (partial)
    """
    m, r1, r2 = nodes(3)
    await write(m)
    await chain(m, r1, r2)

    r1.stop()
    async with traffic(m):
        await r2.replicaof(m, psync=True)
    await r2.caught_up(m)
    await assert_same_data(m, r2)

    assert_syncs(r2, full=[r1], partial=[m])


async def test_outage_middle_deep(nodes):
    """
    Same as above, one level deeper: R3 loses R2 and re-attaches to R1 with a partial sync.

    M -> R1 -> R2 (dies) -> R3

    M keeps writing, R3 re-points:

    M -> R1 -> R3 (partial)
    """
    m, r1, r2, r3 = nodes(4)
    await write(m)
    await chain(m, r1, r2, r3)

    r2.stop()
    async with traffic(m):
        await r3.replicaof(r1, psync=True)
    await settle(m, r1, r3)
    await assert_same_data(m, r1, r3)

    assert_syncs(r3, full=[r2], partial=[r1])


@pytest.mark.parametrize("cause", ["restart", "stale"])
async def test_middle_resync(nodes, proxy_factory, cause):
    """
    The middle node full syncs again from the same master, so its subtree must full sync too.

    M -> R1 (full again) -> R2 (full again)

    - restart: M restarts from a snapshot (new replid)
    - stale: R1's link is down until M's backlog is exhausted (same replid)
    """
    (m,) = nodes(
        1,
        shard_repl_backlog_time_ms=0,
        shard_repl_backlog_max_bytes="1b",
        dbfilename=f"psync-{cause}",
    )
    r1, r2 = nodes(2)
    up = await proxy_factory(m.port) if cause == "stale" else m
    await write(m)
    await r1.replicaof(up)
    await r2.replicaof(r1)
    await settle(m, r1, r2)

    if cause == "restart":
        await m.c.execute_command("SAVE")
        m.restart()
    else:
        await up.close()
        await write(m, prefix="lost")
        await up.start_serving()
    await write(m, prefix="more")
    await wait_for_replicas_state(r1.c, r2.c)
    await settle(m, r1, r2)
    await assert_same_data(m, r1, r2)

    assert_syncs(r1, full=[up, up])
    assert_syncs(r2, full=[r1, r1])


async def test_master_outage_new_master(nodes):
    """
    A new master is seeded from the surviving chain with a single full sync.

    M (dies) -> R1 -> R2

    R1 stays a replica, no writes, M2 joins under R1 (the only extra full sync):

    R1 -> R2
       |-> M2 (full)

    M2 is promoted, R1 re-points to it:

    M2 -> R1 (partial) -> R2 (untouched)
    """
    m, r1, r2, m2 = nodes(4)
    await write(m)
    await chain(m, r1, r2)

    m.stop()
    await chain(r1, m2)
    await m2.promote()
    async with traffic(m2):
        await r1.replicaof(m2, psync=True)
    await settle(m2, r1, r2)
    await assert_same_data(m2, r1, r2)

    assert_syncs(m2, full=[r1])
    assert_syncs(r1, full=[m], partial=[m2])
    assert_syncs(r2, full=[r1])


async def test_move_subtree(nodes):
    """
    Moving a subtree to an unrelated master replaces the data of the whole subtree.

    M1 -> R1 -> R2 -> R3

    R1 re-points to M2 with another dataset:

    M2 -> R1 (full) -> R2 (full) -> R3 (full)
    """
    m1, r1, r2, r3, m2 = nodes(5)
    await write(m1, prefix="old")
    await write(m2, prefix="new")
    await chain(m1, r1, r2, r3)

    async with traffic(m2):
        await r1.replicaof(m2, psync=False)
    await settle(m2, r1, r2, r3)
    await assert_same_data(m2, r1, r2, r3)

    assert_syncs(r1, full=[m1, m2])
    assert_syncs(r2, full=[r1, r1])
    assert_syncs(r3, full=[r2, r2])


async def test_replace_master(nodes):
    """
    A fresh node joins under the master and takes over; the chain follows it with psync.

    M -> R1 -> R2
      |-> N

    N takes over M, R1 re-points to it:

    N -> R1 (partial) -> R2
    """
    m, r1, r2, n = nodes(4)
    await write(m)
    await chain(m, r1, r2)
    async with traffic(m):
        await n.replicaof(m)
    await settle(m, n)

    await n.promote("REPLTAKEOVER 5")
    async with traffic(n):
        await r1.replicaof(n, psync=True)
    await settle(n, r1, r2)
    await assert_same_data(n, r1, r2)

    assert_syncs(r1, full=[m], partial=[n])
    assert_syncs(r2, full=[r1])


async def test_replace_replica(nodes):
    """
    A fresh node takes over a replica; it roots a new lineage that diverges from M.

    M -> R1 -> R2
            |-> N

    N takes over R1, R2 re-points to it:

    N -> R2 (partial)

    - R2 can't psync back to M afterwards (full)
    """
    m, r1, r2, n = nodes(4)
    await write(m)
    await chain(m, r1, r2)
    await chain(r1, n)

    await n.promote("REPLTAKEOVER 5")
    async with traffic(n):
        await r2.replicaof(n, psync=True)
    await r2.caught_up(n)
    await assert_same_data(n, r2)

    await r2.replicaof(m, psync=False)
    await r2.caught_up(m)
    await assert_same_data(m, r2)

    assert_syncs(r2, full=[r1, m], partial=[n])


@pytest.mark.parametrize("promote", ["REPLTAKEOVER 5", "REPLICAOF NO ONE"])
async def test_promoted_lineage_propagates(nodes, promote):
    """
    The new lineage of a promoted node propagates down its chain, enabling psync to it.

    M -> R1 -> R2 -> R3

    R1 is promoted, R3 adopts its lineage through R2 and re-points to it:

    R1 -> R2
       |-> R3 (partial)
    """
    m, r1, r2, r3 = nodes(4)
    await write(m)
    await chain(m, r1, r2, r3)

    await r1.promote(promote)
    async with traffic(r1):
        await asyncio.sleep(0.5)
    await settle(r1, r2, r3)
    async with traffic(r1):
        await r3.replicaof(r1, psync=True)
    await r3.caught_up(r1)
    await assert_same_data(r1, r3)

    assert_syncs(r3, full=[r2], partial=[r1])


async def test_split_brain(nodes):
    """
    After a split brain, a replica psyncs only within its own lineage.

    M -> R1 -> R2 -> R3

    R1 is promoted, M and R1 diverge on the same key:

    M
    R1 (promoted) -> R2 -> R3

    - R3 under R1: partial (shared new lineage), keeps R1's value
    - R3 under M: full (other lineage), converges to M's value
    - M rejoins under R1: full, and so is R3's resync from M, all converge to R1's value
    """
    m, r1, r2, r3 = nodes(4)
    await write(m)
    await m.c.append("diverge", "base")
    await chain(m, r1, r2, r3)

    await r1.promote()
    await m.c.append("diverge", "_M")
    await r1.c.append("diverge", "_R1")
    await settle(r1, r2, r3)

    await r3.replicaof(r1, psync=True)
    await r3.caught_up(r1)
    assert await r3.c.get("diverge") == "base_R1"

    await r3.replicaof(m, psync=False)
    await r3.caught_up(m)
    assert await r3.c.get("diverge") == "base_M"

    await m.replicaof(r1, psync=False)
    await wait_for_replicas_state(r3.c)
    await settle(r1, m, r3)
    assert await r3.c.get("diverge") == "base_R1"
    await assert_same_data(r1, m, r3)

    assert_syncs(m, full=[r1])
    assert_syncs(r3, full=[r2, m, m], partial=[r1])


@pytest.mark.opt_only
async def test_rotating_trees(nodes):
    """
    Random subtree re-attachments under traffic: every move is a psync and all nodes converge.

    M -> R -> R ...
      |-> R ...

    - repeatedly move a random R (with its subtree) under a node outside of its subtree
    """
    rng = random.Random()
    seed = rng.randrange(1 << 32)
    logging.info(f"rotating trees seed {seed}")
    rng.seed(seed)

    m, *rs = nodes(6)
    parent = {}
    for i, r in enumerate(rs):
        parent[r] = rng.choice([m, *rs[:i]])
        await r.replicaof(parent[r])

    def subtree(root):
        sub = {root}
        while added := {r for r, p in parent.items() if p in sub and r not in sub}:
            sub |= added
        return sub

    def top_down():
        order = [m]
        while len(order) <= len(rs):
            order += [r for r, p in parent.items() if p in order and r not in order]
        return order

    async with traffic(m, keys=20_000):
        for _ in range(30):
            moves = [
                (r, up)
                for r in rs
                for up in [m, *rs]
                if up not in subtree(r) and up is not parent[r]
            ]
            r, up = rng.choice(moves)
            await r.replicaof(up, psync=True)
            parent[r] = up
            await asyncio.sleep(0.2)

    for r in top_down()[1:]:
        await r.caught_up(parent[r])
    await assert_same_data(m, *rs)


async def test_cycle(nodes):
    """
    A replication cycle is stable: no data loss, no resync loop, and it can be broken.

    A -> B

    A re-points under B, forming a cycle:

    A <-> B

    B is promoted:

    B -> A

    - A full syncs from B, B psyncs back from A, then both stay online and read-only
    """
    a, b = nodes(2)
    await write(a)
    await chain(a, b)

    await a.replicaof(b, psync=False)
    await wait_for_replicas_state(b.c)
    await asyncio.sleep(1)
    with pytest.raises(redis.exceptions.ReadOnlyError):
        await a.c.append("k0", "v")
    assert [r[3] for r in await asyncio.gather(a.c.role(), b.c.role())] == ["online"] * 2

    await b.promote()
    await write(b, prefix="new")
    await a.caught_up(b)
    await assert_same_data(a, b)
    assert await b.c.dbsize() == 100

    assert_syncs(a, full=[b])
    assert_syncs(b, full=[a], partial=[a])
