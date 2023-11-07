import pytest
import redis
from redis.asyncio import Redis as RedisClient
from .utility import *
from .instance import DflyStartException


async def test_maxclients(df_factory):
    # Needs some authentication
    with df_factory.create(port=1111, maxclients=1, admin_port=1112) as server:
        async with server.client() as client1:
            assert ["maxclients", "1"] == await client1.execute_command("CONFIG GET maxclients")

            with pytest.raises(redis.exceptions.ConnectionError):
                async with server.client() as client2:
                    await client2.get("test")

            # Check that admin connections are not limited.
            async with RedisClient(port=server.admin_port) as admin_client:
                await admin_client.get("test")

            await client1.execute_command("CONFIG SET maxclients 3")
            assert ["maxclients", "3"] == await client1.execute_command("CONFIG GET maxclients")
            async with server.client() as client2:
                await client2.get("test")
