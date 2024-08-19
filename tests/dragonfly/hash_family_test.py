import pytest
import asyncio
from .utility import *


@pytest.mark.asyncio
async def test_empty_hash_as_zipmap_bug(async_client):
    await async_client.execute_command("HSET foo a_field a_value")
    await async_client.execute_command("HSETEX foo 1 b_field b_value")
    await async_client.execute_command("HDEL foo a_field")

    @assert_eventually
    async def check_if_empty():
        assert await async_client.execute_command("HGETALL foo") == []

    await check_if_empty()

    # Key does not exist and it's empty
    assert await async_client.execute_command(f"EXISTS foo") == 0
