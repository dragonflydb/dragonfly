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
