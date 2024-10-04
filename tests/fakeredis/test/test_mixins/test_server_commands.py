from datetime import datetime
from time import sleep

import pytest
import redis
from redis.exceptions import ResponseError


@pytest.mark.unsupported_server_types("dragonfly")
def test_swapdb(r, create_redis):
    r1 = create_redis(3)
    r.set("foo", "abc")
    r.set("bar", "xyz")
    r1.set("foo", "foo")
    r1.set("baz", "baz")
    assert r.swapdb(2, 3)
    assert r.get("foo") == b"foo"
    assert r.get("bar") is None
    assert r.get("baz") == b"baz"
    assert r1.get("foo") == b"abc"
    assert r1.get("bar") == b"xyz"
    assert r1.get("baz") is None


@pytest.mark.unsupported_server_types("dragonfly")
def test_swapdb_same_db(r: redis.Redis):
    assert r.swapdb(1, 1)


def test_save(r: redis.Redis):
    assert r.save()


@pytest.mark.unsupported_server_types("dragonfly")
def test_bgsave(r: redis.Redis):
    assert r.bgsave()
    with pytest.raises(ResponseError):
        r.execute_command("BGSAVE", "SCHEDULE", "FOO")
    with pytest.raises(ResponseError):
        r.execute_command("BGSAVE", "FOO")


def test_lastsave(r: redis.Redis):
    assert isinstance(r.lastsave(), datetime)


@pytest.mark.unsupported_server_types("dragonfly")
@pytest.mark.slow
def test_bgsave_timestamp_update(r: redis.Redis):
    early_timestamp = r.lastsave()
    sleep(1)
    assert r.bgsave()
    sleep(1)
    late_timestamp = r.lastsave()
    assert early_timestamp < late_timestamp


@pytest.mark.slow
def test_save_timestamp_update(r: redis.Redis):
    early_timestamp = r.lastsave()
    sleep(1)
    assert r.save()
    late_timestamp = r.lastsave()
    assert early_timestamp < late_timestamp


def test_dbsize(r: redis.Redis):
    assert r.dbsize() == 0
    r.set("foo", "bar")
    r.set("bar", "foo")
    assert r.dbsize() == 2


def test_flushdb_redispy4(r: redis.Redis):
    r.set("foo", "bar")
    assert r.keys() == [b"foo"]
    assert r.flushdb() is True
    assert r.keys() == []
