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

    print(f"\t\tran scenario: {scenario.name}")
    print_m("Before:", before)
    print_m("After:", after)
    delta = after - before
    print_m("Delta:", delta)

    scenario.classify(delta)

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
    expected = m.type("string") + m.type("key")

    if m.cmd("string") == expected and m.mem("object_used") == expected and clean_memory_classes(m):
        print(">> OK")
    elif m.cmd("string") == m.mem("object_used"):
        print(">> CHECK")
    else:
        print(">> WRONG")


def classify_overwrite_hash(m: Metrics):
    if (
        m.cmd("hash") == m.type("hash")
        and m.cmd("string") == m.type("string") + m.type("key")
        and m.cmd_total() == m.mem("object_used")
        and m.type_total() == m.mem("object_used")
        and clean_memory_classes(m)
    ):
        print(">> OK")
    elif m.cmd("hash") == 0 and m.cmd("string") == m.mem("object_used"):
        print(">> CHECK hash accounted to string")
    else:
        print(">> WRONG")


def classify_del_string(m: Metrics):
    if m.cmd("string") < 0 and clean_memory_classes(m):
        print(">> OK")
    else:
        print(">> WRONG")


def commands(*cmds):
    async def rc(cl):
        for cmd in cmds:
            assert await cl.execute_command(*cmd)

    return rc


def classify_mixed_delete(m: Metrics):
    if all(m.cmd(t) < 0 for t in ("hash", "list", "string")):
        print(">> OK")
    else:
        print(">> WRONG")


def classify_rename_to_new(m: Metrics):
    """
    This needs work. Currently we wrap delete in scope, so the delete will make string -ve
    But then the new string is added, which is under the RENAME cmd, which is generic. We
    are currently skipping generic, so we only find the -ve delta
    """
    if m.cmd("string") > 0:
        print(">> OK")
    else:
        print(">> WRONG")


def classify_rename_hash_over_string(m: Metrics):
    """
    Again, only observes delete operations, not the rename operation, so will only see
    -ve effects. Needs work on generic cmds
    """
    if m.cmd("string") == m.type("string") and m.cmd("hash") == m.type("hash") + m.type("key"):
        print(">> OK")
    else:
        print(">> WRONG")


def classify_observe(_m: Metrics):
    print(">> OBSERVE")


def classify_json_interned(m: Metrics):
    expected = m.type("ReJSON-RL") + m.type("key")
    interned = m.mem("interned_string_pool") + m.mem("interned_string_table")
    if (
        m.cmd("ReJSON-RL") == expected
        and m.cmd("ReJSON-RL") == m.mem("object_used")
        and interned > 0
        and clean_memory_classes(m)
    ):
        print(">> OK")
    else:
        print(">> WRONG")


async def json_set_interned_strings(async_client: aioredis.Redis):
    # this string will be shared between docs
    shared_string = "shared-json-string-" + ("x" * 1024)
    for i in range(32):
        key = f"json-interned-{i}-" + ("j" * 1024)
        doc = '{"field":"' + shared_string + '","nested":{"field":"' + shared_string + '"}}'
        assert await async_client.execute_command("JSON.SET", key, "$", doc)


def classify_ro(m: Metrics):
    if all(v == 0 for v in m.cmd_type_delta.values()):
        print(">> OK")
    else:
        print(">> WRONG")


async def test_scenarios(df_server: DflyInstance, async_client: aioredis.Redis):
    long_key = "a" * 1024
    long_key2 = "c" * 2048
    long_key3 = "d" * 1536
    long_val = "b" * 1024

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
