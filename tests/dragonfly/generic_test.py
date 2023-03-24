import os
import aioredis
import pytest
from . import dfly_multi_test_args
from .utility import batch_fill_data, gen_test_data


@dfly_multi_test_args({'keys_output_limit': 512}, {'keys_output_limit': 1024})
class TestKeys:
    async def test_max_keys(self, async_client: aioredis.Redis, df_server):
        max_keys = df_server['keys_output_limit']
        pipe = async_client.pipeline()
        batch_fill_data(pipe, gen_test_data(max_keys * 3))
        await pipe.execute()
        keys = await async_client.keys()
        assert len(keys) in range(max_keys, max_keys+512)

@pytest.fixture(scope="function")
def export_dfly_password() -> str:
    pwd = 'flypwd'
    os.environ['DFLY_PASSWORD'] = pwd
    yield pwd
    del os.environ['DFLY_PASSWORD']

async def test_password(df_local_factory, export_dfly_password):
    dfly = df_local_factory.create()
    dfly.start()

    # Expect password form environment variable
    with pytest.raises(aioredis.exceptions.AuthenticationError):
        client = aioredis.Redis()
        await client.ping()
    client = aioredis.Redis(password=export_dfly_password)
    await client.ping()
    dfly.stop()

    # --requirepass should take precedence over environment variable
    requirepass = 'requirepass'
    dfly = df_local_factory.create(requirepass=requirepass)
    dfly.start()

    # Expect password form flag
    with pytest.raises(aioredis.exceptions.ResponseError):
        client = aioredis.Redis(password=export_dfly_password)
        await client.ping()
    client = aioredis.Redis(password=requirepass)
    await client.ping()
    dfly.stop()
