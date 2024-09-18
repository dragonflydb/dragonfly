import pytest
from redis import asyncio as aioredis
from .utility import *
import logging


@pytest.mark.opt_only
@pytest.mark.parametrize(
    "type, keys, val_size, elements",
    [
        ("JSON", 300_000, 100, 100),
        ("SET", 500_000, 100, 100),
        ("HASH", 400_000, 100, 100),
        ("ZSET", 400_000, 100, 100),
        ("LIST", 500_000, 100, 100),
        ("STRING", 6_000_000, 1000, 1),
    ],
)
async def test_rss_used_mem_gap(df_factory, type, keys, val_size, elements):
    # Create a Dragonfly and fill it up with `type` until it reaches `min_rss`, then make sure that
    # the gap between used_memory and rss is no more than `max_unaccounted_ratio`.
    min_rss = 5 * 1024 * 1024 * 1024  # 5gb
    max_unaccounted = 200 * 1024 * 1024  # 200mb

    df_server = df_factory.create()
    df_factory.start_all([df_server])
    client = df_server.client()
    await asyncio.sleep(1)  # Wait for another RSS heartbeat update in Dragonfly

    cmd = f"DEBUG POPULATE {keys} {type} {val_size} RAND TYPE {type} ELEMENTS {elements}"
    print(f"Running {cmd}")
    await client.execute_command(cmd)

    await asyncio.sleep(2)  # Wait for another RSS heartbeat update in Dragonfly

    info = await client.info("memory")
    logging.info(f'Used memory {info["used_memory"]}, rss {info["used_memory_rss"]}')
    assert info["used_memory"] > min_rss, "Weak testcase: too little used memory"
    delta = info["used_memory_rss"] - info["used_memory"]
    # It could be the case that the machine is configured to use swap if this assertion fails
    assert delta > 0
    assert delta < max_unaccounted
    delta = info["used_memory_rss"] - info["object_used_memory"]
    # TODO investigate why it fails on string
    if type == "json":
        assert delta > 0
        assert delta < max_unaccounted

    await disconnect_clients(client)
