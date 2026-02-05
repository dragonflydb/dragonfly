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
async def test_spop_with_null_byte_members(df_factory: DflyInstanceFactory):
    df = df_factory.create(proactor_threads=1)

    df.start()

    client = df.client()

    num_members = 10

    for i in range(num_members):
        await client.sadd("set", "b'MEMBER\x01\x02\x00_KEY{i}'".format(i=i))

    assert await client.scard("set") == num_members

    await client.spop("set")

    assert await client.scard("set") == num_members - 1
