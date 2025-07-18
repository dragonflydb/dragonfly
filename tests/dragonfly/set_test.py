import pytest
from redis import asyncio as aioredis
from .instance import DflyInstance, DflyInstanceFactory
import logging
import asyncio


@pytest.mark.asyncio
async def test_slow_sscan(df_factory: DflyInstanceFactory):
    df = df_factory.create(
        proactor_threads=2,
    )
    df.start()

    client = df.client()
    elements = ""
    for i in range(1, 100):
        # 5mb
        element = "a" * 5_000_000
        elements = elements + f" " + element + f"{i}"

    await client.execute_command(f"SADD key {elements}")

    element = "a" * 5_000_000

    cursor = await client.execute_command(f"SSCAN key 0 match {element}.pt")
    length = len(cursor[1])
    # Takes 3 seconds
    res = await client.execute_command("SLOWLOG GET 100")
    logging.debug(res)
