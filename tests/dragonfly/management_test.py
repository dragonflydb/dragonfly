import pytest
import redis


@pytest.mark.asyncio
async def test_config_cmd_single_config(df_factory):
    with df_factory.create() as server:
        async with server.client() as client:
            await client.config_set("requirepass", "foobar") == "OK"

            # Verify the config was set.
            res = await client.config_get("*")
            assert len(res) > 0
            assert res["requirepass"] == "foobar"

        # Verify the config was applied.
        with pytest.raises(redis.exceptions.ConnectionError):
            async with server.client() as client:
                await client.ping()


@pytest.mark.asyncio
async def test_config_cmd_multi_config(df_factory):
    with df_factory.create() as server:
        async with server.client() as client:
            await client.config_set("requirepass", "foobar", "maxclients", "100") == "OK"

            # Verify the config was set.
            res = await client.config_get("*")
            assert len(res) > 0
            assert res["requirepass"] == "foobar"
            assert res["maxclients"] == "100"

        # Verify the config was applied.
        with pytest.raises(redis.exceptions.ConnectionError):
            async with server.client() as client:
                await client.ping()


@pytest.mark.asyncio
async def test_config_roll_back_invalid_config(df_factory):
    with df_factory.create() as server:
        async with server.client() as client:
            # Set a valid password by invalid maxclients, neither config
            # should be set.
            with pytest.raises(redis.exceptions.ResponseError):
                await client.config_set("requirepass", "foobar", "maxclients", "invalid")

            # Verify the config was NOT set.
            res = await client.config_get("*")
            assert len(res) > 0
            assert res["requirepass"] == ""
            assert res["maxclients"] == "64000"

        # Verify requirepass was not applied.
        async with server.client() as client:
            await client.ping()
