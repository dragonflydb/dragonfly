import pytest
import redis
import redis.client
from fakeredis import _msgs as msgs
from redis.exceptions import ResponseError

from test import testtools
from test.testtools import raw_command


def test_ping(r: redis.Redis):
    assert r.ping()
    assert testtools.raw_command(r, "ping", "test") == b"test"
    with pytest.raises(
        redis.ResponseError, match=msgs.WRONG_ARGS_MSG6.format("ping")[4:]
    ):
        raw_command(r, "ping", "arg1", "arg2")


def test_echo(r: redis.Redis):
    assert r.echo(b"hello") == b"hello"
    assert r.echo("hello") == b"hello"


def test_unknown_command(r: redis.Redis):
    with pytest.raises(redis.ResponseError):
        raw_command(r, "0 3 3")


@pytest.mark.decode_responses
class TestDecodeResponses:
    def test_decode_str(self, r):
        r.set("foo", "bar")
        assert r.get("foo") == "bar"

    def test_decode_set(self, r):
        r.sadd("foo", "member1")
        assert set(r.smembers("foo")) == {"member1"}

    def test_decode_list(self, r):
        r.rpush("foo", "a", "b")
        assert r.lrange("foo", 0, -1) == ["a", "b"]

    def test_decode_dict(self, r):
        r.hset("foo", "key", "value")
        assert r.hgetall("foo") == {"key": "value"}

    def test_decode_error(self, r):
        r.set("foo", "bar")
        with pytest.raises(ResponseError) as exc_info:
            r.hset("foo", "bar", "baz")
        assert isinstance(exc_info.value.args[0], str)
