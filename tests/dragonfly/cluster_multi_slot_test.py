"""Tests for --experimental_cluster_allow_multi_slot_same_node.

The flag relaxes the multi-key gate from slot identity to node ownership: a
command whose keys span several slots is permitted when every one of those
slots is owned by the receiving node. With the flag off, behaviour must be
identical to the Redis-Cluster-compatible CROSSSLOT rejection.
"""

import pytest
from redis.exceptions import MovedError, ResponseError

from . import dfly_args
from .cluster_test_utils import (
    MigrationInfo,
    apply_config,
    create_cluster,
    finalize_migration,
    key_slot,
    wait_for_status,
)
from .instance import DflyInstanceFactory

# Slots are split in half; node 0 owns the low range, node 1 the high range.
SPLIT = 8191


def slot_of(key: str) -> int:
    """The slot the server will use: the hash tag decides it when one is present.

    cluster_test_utils.key_slot() hashes the whole key, which differs from the
    server for any tagged key.
    """
    start = key.find("{")
    if start != -1:
        end = key.find("}", start + 1)
        if end > start + 1:
            key = key[start + 1 : end]
    return key_slot(key)


def keys_in_distinct_slots(count, lo, hi):
    """`count` keys hashing to distinct slots, all inside [lo, hi].

    Distinct slots are the whole point: keys sharing a slot would pass the
    cross-slot gate unchanged and would not exercise the flag at all.
    """
    found, seen, i = [], set(), 0
    while len(found) < count:
        key = f"mk:{i}"
        slot = slot_of(key)
        if lo <= slot <= hi and slot not in seen:
            seen.add(slot)
            found.append(key)
        i += 1
    return found


async def setup_two_node_cluster(df_factory, **kwargs):
    instances, nodes = await create_cluster(df_factory, 2, **kwargs)
    nodes[0].slots = [(0, SPLIT)]
    nodes[1].slots = [(SPLIT + 1, 16383)]
    await apply_config(nodes)
    return instances, nodes


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_multi_slot_rejected_when_flag_off(df_factory: DflyInstanceFactory):
    """Default must stay Redis-compatible: multi-slot is CROSSSLOT even on one node."""
    _, nodes = await setup_two_node_cluster(df_factory)
    c0 = nodes[0].client
    keys = keys_in_distinct_slots(4, 0, SPLIT)

    with pytest.raises(ResponseError, match="hash to the same slot"):
        await c0.mset({k: "v" for k in keys})
    with pytest.raises(ResponseError, match="hash to the same slot"):
        await c0.mget(keys)


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_multi_slot_allowed_when_flag_on(df_factory: DflyInstanceFactory):
    """Keys spanning slots owned by this node are served, in request order."""
    _, nodes = await setup_two_node_cluster(
        df_factory, experimental_cluster_allow_multi_slot_same_node="true"
    )
    c0 = nodes[0].client
    keys = keys_in_distinct_slots(5, 0, SPLIT)
    assert len({slot_of(k) for k in keys}) == 5, "keys must span 5 distinct slots"

    assert await c0.mset({k: f"v-{k}" for k in keys})

    # Shuffled on purpose: replies must follow the request order, not slot or
    # shard order, even though the keys are split across slots internally.
    shuffled = [keys[3], keys[0], keys[4], keys[1], keys[2]]
    assert await c0.mget(shuffled) == [f"v-{k}" for k in shuffled]


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_multi_slot_missing_key_keeps_position(df_factory: DflyInstanceFactory):
    """A missing key yields nil in its own position, not a shifted reply."""
    _, nodes = await setup_two_node_cluster(
        df_factory, experimental_cluster_allow_multi_slot_same_node="true"
    )
    c0 = nodes[0].client
    keys = keys_in_distinct_slots(3, 0, SPLIT)

    await c0.mset({k: f"v-{k}" for k in keys})
    assert await c0.delete(keys[1]) == 1
    assert await c0.mget(keys) == [f"v-{keys[0]}", None, f"v-{keys[2]}"]


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_foreign_slot_moved_and_nothing_applied(df_factory: DflyInstanceFactory):
    """A key owned elsewhere yields MOVED, and the write applies nothing."""
    instances, nodes = await setup_two_node_cluster(
        df_factory, experimental_cluster_allow_multi_slot_same_node="true"
    )
    c0 = nodes[0].client
    mine = keys_in_distinct_slots(1, 0, SPLIT)[0]
    theirs = keys_in_distinct_slots(1, SPLIT + 1, 16383)[0]

    await c0.set(mine, "original")

    with pytest.raises(MovedError) as read_err:
        await c0.mget([mine, theirs])
    assert f"{slot_of(theirs)} 127.0.0.1:{instances[1].port}" == str(read_err.value)

    with pytest.raises(MovedError):
        await c0.mset({mine: "overwritten", theirs: "nope"})

    # The rejected MSET must not have applied the owned half of the batch.
    assert await c0.get(mine) == "original"


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_multi_slot_del_and_exists(df_factory: DflyInstanceFactory):
    """Counting commands report across slots, and actually remove the keys."""
    _, nodes = await setup_two_node_cluster(
        df_factory, experimental_cluster_allow_multi_slot_same_node="true"
    )
    c0 = nodes[0].client
    keys = keys_in_distinct_slots(3, 0, SPLIT)

    await c0.mset({k: "v" for k in keys})
    assert await c0.exists(*keys) == 3
    assert await c0.delete(*keys) == 3
    assert await c0.mget(keys) == [None, None, None]


@pytest.mark.parametrize("flag_on", [False, True])
@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_single_slot_unaffected_by_flag(df_factory: DflyInstanceFactory, flag_on: bool):
    """Single-slot multi-key behaviour must not change in either flag state."""
    kwargs = {"experimental_cluster_allow_multi_slot_same_node": "true"} if flag_on else {}
    _, nodes = await setup_two_node_cluster(df_factory, **kwargs)
    c0 = nodes[0].client

    tag = keys_in_distinct_slots(1, 0, SPLIT)[0]
    keys = [f"{{{tag}}}:a", f"{{{tag}}}:b", f"{{{tag}}}:c"]
    assert len({slot_of(k) for k in keys}) == 1, "hash tag must collapse these to one slot"

    await c0.mset({k: f"v-{k}" for k in keys})
    assert await c0.mget(keys) == [f"v-{k}" for k in keys]


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes"})
async def test_multi_slot_after_slot_migrates_away(df_factory: DflyInstanceFactory):
    """A batch is never half-served once one of its slots moves to another node.

    This is the case the relaxed gate has to get right: ownership is resolved
    against one ClusterConfig snapshot, so after a cutover the whole command
    must be refused with MOVED rather than serving the slots that stayed.
    """
    instances, nodes = await create_cluster(
        df_factory, 2, experimental_cluster_allow_multi_slot_same_node="true"
    )
    nodes[0].slots = [(0, 16383)]
    nodes[1].slots = []
    await apply_config(nodes)

    c0, c1 = nodes[0].client, nodes[1].client
    keys = keys_in_distinct_slots(3, 1, 16382)
    await c0.mset({k: f"v-{k}" for k in keys})
    assert await c0.mget(keys) == [f"v-{k}" for k in keys]

    # Move exactly one of the batch's slots to the other node.
    moving = keys[1]
    moved_slot = slot_of(moving)
    nodes[0].migrations.append(
        MigrationInfo(
            "127.0.0.1", nodes[1].instance.admin_port, [(moved_slot, moved_slot)], nodes[1].id
        )
    )
    await apply_config(nodes)
    await wait_for_status(nodes[0].admin_client, nodes[1].id, "FINISHED")
    await finalize_migration(
        nodes,
        0,
        1,
        src_slots=[(0, moved_slot - 1), (moved_slot + 1, 16383)],
        dst_slots=[(moved_slot, moved_slot)],
    )

    # The batch now straddles two nodes: refused whole, naming the slot that left.
    with pytest.raises(MovedError) as err:
        await c0.mget(keys)
    assert f"{moved_slot} 127.0.0.1:{instances[1].port}" == str(err.value)

    # The slots that stayed are still served, and the migrated key moved with its data.
    remaining = [keys[0], keys[2]]
    assert await c0.mget(remaining) == [f"v-{k}" for k in remaining]
    assert await c1.get(moving) == f"v-{moving}"
