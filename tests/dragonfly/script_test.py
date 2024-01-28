import pytest
from redis import asyncio as aioredis

lua_script = """
return "Hello, world!"
"""

"""
Test SCRIPT FLUSH command.
"""


@pytest.mark.asyncio
async def test_script_flush(async_client):
    sha1 = await async_client.script_load(lua_script)
    exists = await async_client.script_exists(sha1)
    assert exists[0] == True

    result = await async_client.script_flush()
    exists = await async_client.script_exists(sha1)
    assert exists[0] == False
