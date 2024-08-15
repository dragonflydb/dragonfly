from unittest.mock import patch

import redis

from fakeredis import FakeRedis


def test_mock():
    # Mock Redis connection
    def bar(redis_host: str, redis_port: int):
        redis.Redis(redis_host, redis_port)

    with patch("redis.Redis", FakeRedis):
        # Call function
        bar("localhost", 6000)

        # Related to #36 - this should fail if mocking Redis does not work
