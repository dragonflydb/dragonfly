import pytest
from redis import asyncio as aioredis
from .instance import DflyInstance, DflyInstanceFactory
import logging
import asyncio


@pytest.mark.asyncio
async def test_sscan_regression(df_factory: DflyInstanceFactory):
    df = df_factory.create(
        proactor_threads=2,
    )
    df.start()

    client = df.client()

    await client.execute_command(f"SADD key el1 el2")

    element = "a*" * 3

    cursor = await client.execute_command(f"SSCAN key 0 match {element}.pt")
    length = len(cursor[1])
    # Takes 3 seconds
    res = await client.execute_command("SLOWLOG GET 100")
    assert res == []


@pytest.mark.asyncio
async def test_sinter_regression(df_factory: DflyInstanceFactory):
    df = df_factory.create(proactor_threads=4, shard_round_robin_prefix="prefix-")
    df.start()

    client = df.client()

    await client.execute_command("DEBUG POPULATE 1 prefix- 5 RAND ELEMENTS 5000 TYPE SET")
    # add a common element to SINTER on a small set
    await client.execute_command("SADD prefix-:0 common")
    # create another key prefix-foo with 3 elements
    await client.execute_command("SADD prefix-foo bar hello common")
    await client.execute_command("SINTER prefix-foo prefix-:0")

    res = await client.execute_command("SLOWLOG GET 100")
    assert res == []
