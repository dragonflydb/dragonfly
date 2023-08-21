import pytest
from redis import asyncio as aioredis
from . import DflyInstanceFactory
from .utility import disconnect_clients


@pytest.mark.asyncio
async def test_acl_list_default_user(df_factory: DflyInstanceFactory):
    """
    make sure that the default created user is printed correctly
    """
    instance = df_factory.create(port=1111)
    instance.start()
    client = aioredis.Redis(port=instance.port)

    result = await client.execute_command("ACL LIST")
    assert 1 == len(result)
    assert "user default on nopass +@all" == result[0]

    await disconnect_clients(client)
