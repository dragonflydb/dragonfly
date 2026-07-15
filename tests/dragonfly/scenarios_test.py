import asyncio
from pprint import pprint
from typing import Callable, Optional

import redis.asyncio as aioredis
from attr import dataclass
from attr import field

from .instance import DflyInstance


async def no_setup(_async_client: aioredis.Redis):
    pass


@dataclass
class Scenario:
    name: str
    action: Callable
    classify: Callable
    setup: Callable = no_setup


def delta_dicts(a: dict, b: dict):
    result = a.keys() | b.keys()
    return {f: a.get(f, 0) - b.get(f, 0) for f in result}


def non_zero(d) -> dict:
    return {k: v for k, v in d.items() if v != 0}


@dataclass
class Metrics:
    type_used: dict = field(factory=dict)
    cmd_type_delta: dict = field(factory=dict)
    memory_class: dict = field(factory=dict)

    def __sub__(self, other: "Metrics"):
        return Metrics(
            type_used=delta_dicts(self.type_used, other.type_used),
            cmd_type_delta=delta_dicts(self.cmd_type_delta, other.cmd_type_delta),
            memory_class=delta_dicts(self.memory_class, other.memory_class),
        )

    def pr(self) -> "Metrics":
        return Metrics(
            type_used=non_zero(self.type_used),
            cmd_type_delta=non_zero(self.cmd_type_delta),
            memory_class=non_zero(self.memory_class),
        )

    def cmd(self, type_name: str) -> float:
        return self.cmd_type_delta.get(type_name, 0)

    def type(self, type_name: str) -> float:
        return self.type_used.get(type_name, 0)

    def mem(self, class_name: str) -> float:
        return self.memory_class.get(class_name, 0)

    def obj_used(self):
        return self.mem("object_used")

    def cmd_total(self) -> float:
        return sum(self.cmd_type_delta.values())

    def type_total(self) -> float:
        return sum(self.type_used.values())


WANT_MEMORY_CLASSES = frozenset(
    {
        "object_used",
        "table_used",
        "search_used",
        "interned_string_pool",
        "interned_string_table",
    }
)


async def snapshot_metrics(df_server: DflyInstance):
    metrics = await df_server.metrics()
    m = Metrics()
    for k, v in metrics.items():
        if k in ("dragonfly_command_type_memory_delta_bytes", "dragonfly_type_used_memory"):
            for sample in v.samples:
                t = sample.labels.get("type")
                if k == "dragonfly_command_type_memory_delta_bytes":
                    m.cmd_type_delta[t] = sample.value
                else:
                    m.type_used[t] = sample.value
        elif k == "dragonfly_memory_by_class_bytes":
            for sample in v.samples:
                label = sample.labels["class"]
                if label not in WANT_MEMORY_CLASSES:
                    continue
                m.memory_class[label] = sample.value
    return m


def classify_and_report(
    scenario: Scenario, before: Metrics, after: Metrics, error: Optional[Exception]
):
    def print_m(label: str, m: Metrics):
        print(label)
        pprint(m.pr())

    delta = after - before
    status = "ERROR" if error else scenario.classify(delta)

    print(f"\nScenario: {scenario.name}")
    print(f">> {status}")

    if status != "OK":
        print_m("Before:", before)
        print_m("After:", after)
        print_m("Delta:", delta)

    if error:
        print(f"failed with {error=}")


def clean_memory_classes(m: Metrics) -> bool:
    return m.mem("table_used") == 0 and m.mem("search_used") == 0


async def set_string_simple(async_client: aioredis.Redis):
    value = "x" * 65536
    key_prefix = "k" * 256
    for i in range(32):
        assert await async_client.set(f"{key_prefix}-{i}", value)


def classify_simple_set(m: Metrics):
    """New string: command delta should include value + key."""
    expected = m.type("string") + m.type("key")

    if m.cmd("string") == expected and m.obj_used() == expected and clean_memory_classes(m):
        return "OK"
    elif m.cmd("string") == m.obj_used():
        return "CHECK"
    else:
        return "WRONG"


def classify_overwrite_hash(m: Metrics):
    """SET over hash: old hash free and new string alloc should split by type."""
    if (
        m.cmd("hash") == m.type("hash")
        and m.cmd("string") == m.type("string") + m.type("key")
        and m.cmd_total() == m.obj_used()
        and m.type_total() == m.obj_used()
        and clean_memory_classes(m)
    ):
        return "OK"
    elif m.cmd("hash") == 0 and m.cmd("string") == m.obj_used():
        return "CHECK hash accounted to string"
    else:
        return "WRONG"


def classify_overwrite_hash_with_z(m: Metrics):
    """A hash is used as dest for ZUNIONSTORE: old hash freed and new zset added"""
    if (
        m.cmd("zset") == m.type("zset")
        and m.cmd("hash") == m.cmd("hash")
        and m.cmd_total() == m.obj_used()
        and clean_memory_classes(m)
    ):
        return "OK"
    else:
        return "WRONG"


def classify_del_string(m: Metrics):
    """DEL string: command delta should match removed value + key."""
    if (
        m.cmd("string") == m.type("string") + m.type("key")
        and m.cmd_total() == m.obj_used()
        and m.cmd("string") < 0
        and clean_memory_classes(m)
    ):
        return "OK"
    else:
        return "WRONG"


def commands(*cmds):
    async def rc(cl):
        for cmd in cmds:
            assert await cl.execute_command(*cmd)

    return rc


def classify_mixed_delete(m: Metrics):
    """DEL mixed keys: each removed type should get its own negative delta."""
    if (
        all(m.cmd(t) < 0 for t in ("hash", "list", "string"))
        and m.cmd_total() == m.obj_used()
        and m.type_total() == m.obj_used()
        and clean_memory_classes(m)
    ):
        return "OK"
    else:
        return "WRONG"


def classify_rename_to_new(m: Metrics):
    """RENAME new key: move should account net key delta to value type."""
    if (
        m.cmd("string") == m.type("string") + m.type("key")
        and m.cmd_total() == m.obj_used()
        and m.cmd("string") > 0
        and clean_memory_classes(m)
    ):
        return "OK"
    else:
        return "WRONG"


def classify_rename_hash_over_string(m: Metrics):
    """RENAME over string: removed string and moved hash should split by type."""
    if (
        m.cmd("string") == m.type("string")
        and m.cmd("hash") == m.type("hash") + m.type("key")
        and m.cmd_total() == m.obj_used()
        and m.type_total() == m.obj_used()
        and clean_memory_classes(m)
    ):
        return "OK"
    else:
        return "WRONG"


def classify_observe(_m: Metrics):
    return "OBSERVE"


def classify_json_interned(m: Metrics):
    """JSON new docs: JSON+key accounted, interned pool stays separate."""
    expected = m.type("ReJSON-RL") + m.type("key")
    interned = m.mem("interned_string_pool") + m.mem("interned_string_table")
    if (
        m.cmd("ReJSON-RL") == expected
        and m.cmd("ReJSON-RL") == m.obj_used()
        and interned > 0
        and clean_memory_classes(m)
    ):
        return "OK"
    else:
        return "WRONG"


async def json_set_interned_strings(async_client: aioredis.Redis):
    # this string will be shared between docs
    shared_string = "shared-json-string-" + ("x" * 1024)
    for i in range(32):
        key = f"json-interned-{i}-" + ("j" * 1024)
        doc = '{"field":"' + shared_string + '","nested":{"field":"' + shared_string + '"}}'
        assert await async_client.execute_command("JSON.SET", key, "$", doc)


def classify_ro(m: Metrics):
    """Reads should not change command memory deltas."""
    if (
        all(v == 0 for v in m.cmd_type_delta.values())
        and m.obj_used() == 0
        and clean_memory_classes(m)
    ):
        return "OK"
    else:
        return "WRONG"


async def expire_string(cl: aioredis.Redis):
    await asyncio.sleep(0.6)
    assert await cl.get("a" * 1024) is None


def classify_expiry(m: Metrics):
    """Expiry delete should look like DEL for the expired value type."""
    if (
        m.cmd("string") == m.type("string") + m.type("key")
        and m.cmd_total() == m.obj_used()
        and m.cmd("string") < 0
        and clean_memory_classes(m)
    ):
        return "OK"
    else:
        return "WRONG"


def classify_copy_replace(m: Metrics):
    """COPY REPLACE: removed dest and copied source should split by type."""
    if (
        m.cmd("string") == m.type("string")
        and m.cmd("hash") == m.type("hash")
        and m.cmd_total() == m.obj_used()
        and m.type_total() == m.obj_used()
        and clean_memory_classes(m)
    ):
        return "OK"
    else:
        return "WRONG"


class RestoreScenario:
    def __init__(self):
        self.payload = None

    async def setup(self, cl: aioredis.Redis):
        src = "restore-src-" + ("a" * 1024)
        assert await cl.hset(src, "f", "x" * 1024)
        self.payload = await cl.dump(src)
        assert self.payload is not None

    async def action(self, cl: aioredis.Redis):
        assert await cl.restore("d" * 1024, 0, self.payload)


def classify_restore_hash(m: Metrics):
    """RESTORE hash: decoded runtime type should receive the delta."""
    if (
        m.cmd("hash") == m.type("hash") + m.type("key")
        and m.cmd_total() == m.obj_used()
        and m.type_total() == m.obj_used()
        and clean_memory_classes(m)
    ):
        return "OK"
    else:
        return "WRONG"


def classify_hset_enc_change(m: Metrics):
    """Hash encoding growth should stay attributed to hash."""
    if (
        m.cmd("hash") == m.type("hash")
        and m.cmd_total() == m.obj_used()
        and m.cmd("hash") > 0
        and clean_memory_classes(m)
    ):
        return "OK"
    else:
        return "WRONG"


async def action_trigger_table_growth(cl: aioredis.Redis):
    pipe = cl.pipeline(transaction=False)
    for i in range(50_000_0):
        key = f"table-gr-{i}-" + ("k" * 64)
        pipe.set(key, "v")
    results = await pipe.execute()
    assert all(results)


def classify_table_growth(m: Metrics):
    "Currently table growth is added to new metric!"
    object_used = m.obj_used()
    table = m.mem("table_used")
    cmd = m.cmd("string")

    if table <= 0:
        return "WRONG table didnt grow"

    if cmd == object_used:
        return "OK table excluded"

    if cmd >= object_used + table:
        return "CHECK table included"

    if cmd > object_used:
        return "CHECK other bytes included?"

    return "WRONG"


async def test_scenarios(df_server: DflyInstance, async_client: aioredis.Redis):
    from functools import partial

    long_key = "a" * 1024
    long_key2 = "c" * 2048
    long_key3 = "d" * 1536
    long_val = "b" * 1024

    restore_scenario = RestoreScenario()
    scenarios = [
        Scenario("set string simple", action=set_string_simple, classify=classify_simple_set),
        Scenario(
            "set overwrites hash",
            action=commands(("SET", long_key, long_val)),
            classify=classify_overwrite_hash,
            setup=commands(("HSET", long_key, "field", long_val)),
        ),
        Scenario(
            "delete string",
            action=commands(("DEL", long_key)),
            classify=classify_del_string,
            setup=commands(("SET", long_key, long_val)),
        ),
        Scenario(
            "delete mixed types",
            setup=commands(
                ("SET", long_key, long_val),
                ("HSET", long_key + "h", "f", long_val),
                ("LPUSH", long_key + "l", long_val),
            ),
            action=commands(("DEL", long_key, long_key + "h", long_key + "l")),
            classify=classify_mixed_delete,
        ),
        Scenario(
            "rename string to new key",
            setup=commands(("SET", long_key, long_val)),
            action=commands(("RENAME", long_key, long_key2)),
            classify=classify_rename_to_new,
        ),
        Scenario(
            "rename hash over string",
            setup=commands(
                ("HSET", long_key, "field", long_val),
                ("SET", long_key3, long_val),
            ),
            action=commands(("RENAME", long_key, long_key3)),
            classify=classify_rename_hash_over_string,
        ),
        Scenario(
            "json set interned strings",
            action=json_set_interned_strings,
            classify=classify_json_interned,
        ),
        Scenario(
            "read only",
            setup=commands(
                ("SET", long_key, long_val),
                ("HSET", long_key2, "f", long_val),
                ("LPUSH", long_key3, long_val),
            ),
            action=commands(("GET", long_key), ("HGET", long_key2, "f"), ("LLEN", long_key3)),
            classify=classify_ro,
        ),
        # Scenario(
        #     "expire string",
        #     setup=commands(("SET", long_key, long_val, "PX", "500")),
        #     action=expire_string,
        #     classify=classify_expiry,
        # ),
        Scenario(
            "copy which replaces",
            setup=commands(("HSET", long_key, "f", long_val), ("SET", long_key2, long_val)),
            action=commands(("COPY", long_key, long_key2, "REPLACE")),
            classify=classify_copy_replace,
        ),
        Scenario(
            "restore hash",
            setup=partial(RestoreScenario.setup, restore_scenario),
            action=partial(RestoreScenario.action, restore_scenario),
            classify=classify_restore_hash,
        ),
        Scenario(
            "hash encoding changes lp->strmap",
            setup=commands(("HSET", long_key, "f", "v")),
            action=commands(("HSET", long_key, "f1", long_val)),
            classify=classify_hset_enc_change,
        ),
        # Scenario(
        #     "table growth accounting",
        #     setup=no_setup,
        #     action=action_trigger_table_growth,
        #     classify=classify_table_growth,
        # ),
        Scenario(
            "Z-union store overwrite hash",
            setup=commands(
                ("HSET", long_key, "f", long_val),
                ("ZADD", long_key2, 1, "a", 2, "b"),
                ("ZADD", long_key3, 3, "c", 4, "d"),
            ),
            action=commands(("ZUNIONSTORE", long_key, 2, long_key2, long_key3)),
            classify=classify_overwrite_hash_with_z,
        ),
    ]

    for scenario in scenarios:
        await async_client.flushdb()

        await scenario.setup(async_client)
        before = await snapshot_metrics(df_server)
        error = None
        try:
            await scenario.action(async_client)
        except Exception as e:
            error = e

        after = await snapshot_metrics(df_server)
        classify_and_report(scenario, before, after, error)
