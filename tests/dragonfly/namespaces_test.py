# Namespace isolation and per-namespace stats through a real client library.
# Expectations follow specs/namespaces-testing-report.md; some tests fail until its gaps are fixed.

import asyncio
import copy
import random
import uuid

import fakeredis
import pytest
from redis import asyncio as aioredis

from .instance import DflyInstance
from .seeder import DebugPopulateSeeder, Seeder
from .utility import DflySeeder, ValueType

TYPES = ["STRING", "LIST", "SET", "HASH", "ZSET"]

# ACL SETUSER upper-cases NAMESPACE:<id>, so generated ids are upper-case to stay addressable.


class Tenants:
    """Creates ACL users bound to namespaces and hands out authenticated clients."""

    def __init__(self, df: DflyInstance):
        self.df = df
        self.admin = df.client()
        self.users = 0

    async def creds(self, ns: str) -> dict:
        """Connection kwargs for a fresh user in `ns` (empty for the default namespace)."""
        if not ns:
            return {}
        self.users += 1
        user = f"u{self.users}_{ns}"
        await self.admin.execute_command(f"ACL SETUSER {user} NAMESPACE:{ns} ON >pw +@all ~* &*")
        return dict(username=user, password="pw")

    async def client(self, ns: str, **kwargs) -> aioredis.Redis:
        return self.df.client(**await self.creds(ns), **kwargs)

    @staticmethod
    def ids(n: int) -> list[str]:
        """Default namespace plus n fresh ones, unique per call."""
        tag = uuid.uuid4().hex[:6].upper()
        return [""] + [f"NS{tag}{i}" for i in range(n)]


@pytest.fixture
async def tenants(df_server: DflyInstance):
    t = Tenants(df_server)
    await t.admin.flushall()
    yield t


# --------------------------------------------------------------------------- Isolation


async def test_mirrored_seeders(df_server: DflyInstance, tenants: Tenants):
    """Old seeder per namespace, all on the same key names, each mirrored to its own fakeredis:
    every reply must match the mirror and the final capture must equal the mirror's."""

    async def run(ns):
        seeder = DflySeeder(
            port=df_server.port,
            keys=500,
            mirror_to_fake_redis=True,
            unsupported_types=[ValueType.JSON],
            client_kwargs=await tenants.creds(ns),
        )
        seeder.fake_redis = fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer())
        await seeder.run(target_deviation=0.1)
        await seeder.run(target_ops=2000)
        assert await seeder.compare(await seeder.capture_fake_redis(), port=df_server.port), ns

    await asyncio.gather(*(run(ns) for ns in Tenants.ids(3)))


async def test_concurrent_seeders(tenants: Tenants):
    """Lua seeders with one seed (same key names, random server-side values) run concurrently in
    every namespace. Then each namespace in turn stays idle while all others keep seeding, and
    its capture must not change."""
    template = Seeder(units=8, key_target=2000, types=TYPES, seed=random.randrange(1 << 30))
    clients = [await tenants.client(ns) for ns in Tenants.ids(3)]
    seeders = [copy.deepcopy(template) for _ in clients]
    await asyncio.gather(*(s.run(c, target_deviation=0.1) for s, c in zip(seeders, clients)))

    # Key creation is seeded, so counts match; values differ
    assert len({await c.dbsize() for c in clients}) == 1
    caps = [await Seeder.capture(c, TYPES) for c in clients]
    assert len(set(caps)) == len(caps)

    for idle in range(len(clients)):
        busy = [i for i in range(len(clients)) if i != idle]
        await asyncio.gather(*(seeders[i].run(clients[i], target_ops=1000) for i in busy))
        assert await Seeder.capture(clients[idle], TYPES) == caps[idle]
        caps = [await Seeder.capture(c, TYPES) for c in clients]


@pytest.mark.skip(
    reason="RDB save/load only persists the default namespace today; see "
    "specs/namespaces-maintenance-report.md section on Persistence (RDB)."
)
async def test_seeders_survive_reload(df_factory):
    """Different data in every namespace; DEBUG RELOAD must restore each one exactly."""
    df = df_factory.create(dbfilename="ns-reload")
    df.start()
    tenants = Tenants(df)
    clients = [await tenants.client(ns) for ns in Tenants.ids(3)]
    seeders = [Seeder(units=4, key_target=1000, types=TYPES) for _ in clients]
    await asyncio.gather(*(s.run(c, target_deviation=0.1) for s, c in zip(seeders, clients)))
    before = [await Seeder.capture(c, TYPES) for c in clients]
    assert len(set(before)) == len(before)

    await tenants.admin.execute_command("DEBUG", "RELOAD")
    assert [await Seeder.capture(c, TYPES) for c in clients] == before


async def test_keyspace_commands_isolated(tenants: Tenants):
    """KEYS/SCAN/DBSIZE/RANDOMKEY/FLUSHALL never cross namespaces."""
    ids = Tenants.ids(3)
    clients = {ns: await tenants.client(ns) for ns in ids}
    for i, (ns, c) in enumerate(clients.items()):
        await c.mset({f"{ns}:{j}": j for j in range(10 * (i + 1))})

    for i, (ns, c) in enumerate(clients.items()):
        n = 10 * (i + 1)
        assert await c.dbsize() == n
        assert len([k async for k in c.scan_iter(count=7)]) == n
        assert all(k.startswith(f"{ns}:") for k in await c.keys())
        assert (await c.randomkey()).startswith(f"{ns}:")

    victim = ids[1]
    await clients[victim].flushall()
    assert [await c.dbsize() for c in clients.values()] == [10, 0, 30, 40]


async def test_scripts_and_transactions(tenants: Tenants):
    """Concurrent EVAL/MULTI on the same key name in different namespaces stay separate,
    and WATCH only reacts to writes in its own namespace."""
    ids = Tenants.ids(2)
    clients = {ns: await tenants.client(ns) for ns in ids}
    incr = "return redis.call('INCRBY', KEYS[1], ARGV[1])"

    async def bump(ns, times):
        c = clients[ns]
        for _ in range(times):
            await c.eval(incr, 1, "ctr", 1)
            async with c.pipeline(transaction=True) as p:
                await p.incrby("ctr", 10).execute()

    await asyncio.gather(*(bump(ns, 5 * (i + 1)) for i, ns in enumerate(ids)))
    assert [int(await c.get("ctr")) for c in clients.values()] == [55, 110, 165]

    a, b = clients[ids[1]], clients[ids[2]]
    async with a.pipeline(transaction=True) as p:
        await p.watch("ctr")
        await b.set("ctr", "other")  # same key, other namespace
        p.multi()
        p.set("ctr", "mine")
        assert await p.execute() == [True]
    assert await a.get("ctr") == "mine"


async def test_blocking_isolated(tenants: Tenants):
    ids = Tenants.ids(2)
    clients = {ns: await tenants.client(ns) for ns in ids}
    waiter = await tenants.client(ids[2])

    blpop = asyncio.create_task(waiter.blpop("q", timeout=5))
    await asyncio.sleep(0.1)
    await clients[ids[0]].lpush("q", "default")
    await clients[ids[1]].lpush("q", "other")
    await asyncio.sleep(0.2)
    assert not blpop.done()

    await clients[ids[2]].lpush("q", "own")
    assert tuple(await blpop) == ("q", "own")
    assert await clients[ids[1]].lrange("q", 0, -1) == ["other"]


@pytest.mark.skip(
    reason="ChannelStore is a single global registry today (I5 in "
    "specs/namespaces-testing-report.md): PUBLISH/SUBSCRIBE leak across namespaces."
)
async def test_pubsub_isolated(tenants: Tenants):
    """I5: ChannelStore is global today, so this leaks across namespaces."""
    ids = Tenants.ids(2)
    pub = {ns: await tenants.client(ns) for ns in ids}
    sub = (await tenants.client(ids[2])).pubsub()
    await sub.subscribe("ch")
    await sub.get_message(timeout=1)  # subscribe confirmation

    assert await pub[ids[0]].publish("ch", "leak") == 0
    assert await pub[ids[1]].publish("ch", "leak") == 0
    assert await pub[ids[2]].publish("ch", "own") == 1
    msg = await sub.get_message(ignore_subscribe_messages=True, timeout=1)
    assert msg["data"] == "own"


# --------------------------------------------------------------------------- Info / metrics


async def test_info_per_namespace(tenants: Tenants):
    """INFO keyspace/stats report the caller's namespace only."""
    rng = random.Random()
    ids = Tenants.ids(3)
    clients = {ns: await tenants.client(ns) for ns in ids}
    expected = {}
    for c in clients.values():
        await c.config_resetstat()
    for ns, c in clients.items():
        keys, ttl, hits, misses = rng.randint(1, 50), rng.randint(0, 20), 0, 0
        await c.mset({f"k{i}": "v" for i in range(keys)})
        for i in range(ttl):
            await c.expire(f"k{i}", 1000)
        for i in range(rng.randint(0, 40)):
            if await c.get(f"k{rng.randrange(2 * keys)}") is None:
                misses += 1
            else:
                hits += 1
        expected[ns] = (keys, min(ttl, keys), hits, misses)

    for ns, c in clients.items():
        db0 = (await c.info("keyspace"))["db0"]
        stats = await c.info("stats")
        got = (db0["keys"], db0["expires"], stats["keyspace_hits"], stats["keyspace_misses"])
        assert got == expected[ns], ns


@pytest.mark.skip(
    reason="/metrics (Prometheus endpoint) always reports the default namespace only today "
    "(M3 in specs/namespaces-stats-metrics.md); it does not sum across namespaces."
)
async def test_metrics_endpoint_counts_all_namespaces(df_server: DflyInstance, tenants: Tenants):
    """DEBUG POPULATE lands in the caller's namespace; INFO matches DBSIZE per namespace and
    /metrics (M3) sums all namespaces."""
    clients = [await tenants.client(ns) for ns in Tenants.ids(3)]
    for i, c in enumerate(clients):
        await DebugPopulateSeeder(key_target=500 * (i + 1), types=TYPES).run(c)

    sizes = [await c.dbsize() for c in clients]
    assert len(set(sizes)) == len(sizes)
    assert [(await c.info("keyspace"))["db0"]["keys"] for c in clients] == sizes
    metrics = await df_server.metrics()
    assert sum(s.value for s in metrics["dragonfly_db_keys"].samples) == sum(sizes)
