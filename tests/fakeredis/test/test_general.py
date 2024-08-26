import pytest
import redis

from test.testtools import raw_command


def test_asyncioio_is_used():
    """Redis 4.2+ has support for asyncio and should be preferred over aioredis"""
    from fakeredis import aioredis

    assert not hasattr(aioredis, "__version__")


def test_unknown_command(r: redis.Redis):
    with pytest.raises(redis.ResponseError):
        raw_command(r, "0 3 3")
