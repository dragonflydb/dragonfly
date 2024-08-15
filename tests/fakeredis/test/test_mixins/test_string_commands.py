from __future__ import annotations

import time
from datetime import timedelta

import pytest
import redis
import redis.client
from redis.exceptions import ResponseError

from ..testtools import raw_command


def test_append(r: redis.Redis):
    assert r.set("foo", "bar")
    assert r.append("foo", "baz") == 6
    assert r.get("foo") == b"barbaz"


def test_append_with_no_preexisting_key(r: redis.Redis):
    assert r.append("foo", "bar") == 3
    assert r.get("foo") == b"bar"


def test_append_wrong_type(r: redis.Redis):
    r.rpush("foo", b"x")
    with pytest.raises(redis.ResponseError):
        r.append("foo", b"x")


def test_decr(r: redis.Redis):
    r.set("foo", 10)
    assert r.decr("foo") == 9
    assert r.get("foo") == b"9"


def test_decr_newkey(r: redis.Redis):
    r.decr("foo")
    assert r.get("foo") == b"-1"


def test_decr_expiry(r: redis.Redis):
    r.set("foo", 10, ex=10)
    r.decr("foo", 5)
    assert r.ttl("foo") > 0


def test_decr_badtype(r: redis.Redis):
    r.set("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.decr("foo", 15)
    r.rpush("foo2", 1)
    with pytest.raises(redis.ResponseError):
        r.decr("foo2", 15)


def test_get_does_not_exist(r: redis.Redis):
    assert r.get("foo") is None


def test_get_with_non_str_keys(r: redis.Redis):
    assert r.set("2", "bar") is True
    assert r.get(2) == b"bar"


def test_get_invalid_type(r: redis.Redis):
    assert r.hset("foo", "key", "value") == 1
    with pytest.raises(redis.ResponseError):
        r.get("foo")


def test_getset_exists(r: redis.Redis):
    r.set("foo", "bar")
    val = r.getset("foo", b"baz")
    assert val == b"bar"
    val = r.getset("foo", b"baz2")
    assert val == b"baz"


def test_getset_wrong_type(r: redis.Redis):
    r.rpush("foo", b"x")
    with pytest.raises(redis.ResponseError):
        r.getset("foo", "bar")


def test_getdel(r: redis.Redis):
    r["foo"] = "bar"
    assert r.getdel("foo") == b"bar"
    assert r.get("foo") is None


def test_getdel_doesnt_exist(r: redis.Redis):
    assert r.getdel("foo") is None


def test_incr_with_no_preexisting_key(r: redis.Redis):
    assert r.incr("foo") == 1
    assert r.incr("bar", 2) == 2


def test_incr_by(r: redis.Redis):
    assert r.incrby("foo") == 1
    assert r.incrby("bar", 2) == 2


def test_incr_preexisting_key(r: redis.Redis):
    r.set("foo", 15)
    assert r.incr("foo", 5) == 20
    assert r.get("foo") == b"20"


def test_incr_expiry(r: redis.Redis):
    r.set("foo", 15, ex=10)
    r.incr("foo", 5)
    assert r.ttl("foo") > 0


def test_incr_bad_type(r: redis.Redis):
    r.set("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.incr("foo", 15)
    r.rpush("foo2", 1)
    with pytest.raises(redis.ResponseError):
        r.incr("foo2", 15)


def test_incr_with_float(r: redis.Redis):
    with pytest.raises(redis.ResponseError):
        r.incr("foo", 2.0)


def test_incr_followed_by_mget(r: redis.Redis):
    r.set("foo", 15)
    assert r.incr("foo", 5) == 20
    assert r.get("foo") == b"20"


def test_incr_followed_by_mget_returns_strings(r: redis.Redis):
    r.incr("foo", 1)
    assert r.mget(["foo"]) == [b"1"]


def test_incrbyfloat(r: redis.Redis):
    r.set("foo", 0)
    assert r.incrbyfloat("foo", 1.0) == 1.0
    assert r.incrbyfloat("foo", 1.0) == 2.0


def test_incrbyfloat_with_noexist(r: redis.Redis):
    assert r.incrbyfloat("foo", 1.0) == 1.0
    assert r.incrbyfloat("foo", 1.0) == 2.0


def test_incrbyfloat_expiry(r: redis.Redis):
    r.set("foo", 1.5, ex=10)
    r.incrbyfloat("foo", 2.5)
    assert r.ttl("foo") > 0


def test_incrbyfloat_bad_type(r: redis.Redis):
    r.set("foo", "bar")
    with pytest.raises(redis.ResponseError, match="not a valid float"):
        r.incrbyfloat("foo", 1.0)
    r.rpush("foo2", 1)
    with pytest.raises(redis.ResponseError):
        r.incrbyfloat("foo2", 1.0)


def test_incrbyfloat_precision(r: redis.Redis):
    x = 1.23456789123456789
    assert r.incrbyfloat("foo", x) == x
    assert float(r.get("foo")) == x


def test_mget(r: redis.Redis):
    r.set("foo", "one")
    r.set("bar", "two")
    assert r.mget(["foo", "bar"]) == [b"one", b"two"]
    assert r.mget(["foo", "bar", "baz"]) == [b"one", b"two", None]
    assert r.mget("foo", "bar") == [b"one", b"two"]


def test_mget_with_no_keys(r: redis.Redis):
    assert r.mget([]) == []


def test_mget_mixed_types(r: redis.Redis):
    r.hset("hash", "bar", "baz")
    r.zadd("zset", {"bar": 1})
    r.sadd("set", "member")
    r.rpush("list", "item1")
    r.set("string", "value")
    assert r.mget(["hash", "zset", "set", "string", "absent"]) == [None, None, None, b"value", None]


def test_mset_with_no_keys(r: redis.Redis):
    with pytest.raises(redis.ResponseError):
        r.mset({})


def test_mset(r: redis.Redis):
    assert r.mset({"foo": "one", "bar": "two"}) is True
    assert r.mset({"foo": "one", "bar": "two"}) is True
    assert r.mget("foo", "bar") == [b"one", b"two"]


def test_msetnx(r: redis.Redis):
    assert r.msetnx({"foo": "one", "bar": "two"})
    assert not r.msetnx({"bar": "two", "baz": "three"})
    assert r.mget("foo", "bar", "baz") == [b"one", b"two", None]


def test_setex(r: redis.Redis):
    assert r.setex("foo", 100, "bar") is True
    assert r.get("foo") == b"bar"


def test_setex_using_timedelta(r: redis.Redis):
    assert r.setex("foo", timedelta(seconds=100), "bar") is True
    assert r.get("foo") == b"bar"


def test_setex_using_float(r: redis.Redis):
    with pytest.raises(redis.ResponseError, match="integer"):
        r.setex("foo", 1.2, "bar")


@pytest.mark.min_server("6.2")
def test_setex_overflow(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.setex("foo", 18446744073709561, "bar")  # Overflows longlong in ms


def test_set_ex(r: redis.Redis):
    assert r.set("foo", "bar", ex=100) is True
    assert r.get("foo") == b"bar"


@pytest.mark.min_server("6.2")
def test_set_exat(r: redis.Redis):
    curr_time = int(time.time())
    assert r.set("foo", "bar", exat=curr_time + 100) is True
    assert r.get("foo") == b"bar"


@pytest.mark.min_server("6.2")
def test_set_pxat(r: redis.Redis):
    curr_time = int(time.time() * 1000)
    assert r.set("foo", "bar", pxat=curr_time + 100) is True
    assert r.get("foo") == b"bar"
    time.sleep(0.15)
    assert r.get("foo") is None


def test_set_ex_using_timedelta(r: redis.Redis):
    assert r.set("foo", "bar", ex=timedelta(seconds=100)) is True
    assert r.get("foo") == b"bar"


def test_set_ex_overflow(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.set("foo", "bar", ex=18446744073709561)  # Overflows longlong in ms


def test_set_px_overflow(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.set("foo", "bar", px=2**63 - 2)  # Overflows after adding current time


def test_set_px(r: redis.Redis):
    assert r.set("foo", "bar", px=100) is True
    assert r.get("foo") == b"bar"


def test_set_px_using_timedelta(r: redis.Redis):
    assert r.set("foo", "bar", px=timedelta(milliseconds=100)) is True
    assert r.get("foo") == b"bar"


def test_set_conflicting_expire_options(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.set("foo", "bar", ex=1, px=1)


def test_set_raises_wrong_ex(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.set("foo", "bar", ex=-100)
    with pytest.raises(ResponseError):
        r.set("foo", "bar", ex=0)
    assert not r.exists("foo")


def test_set_using_timedelta_raises_wrong_ex(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.set("foo", "bar", ex=timedelta(seconds=-100))
    with pytest.raises(ResponseError):
        r.set("foo", "bar", ex=timedelta(seconds=0))
    assert not r.exists("foo")


def test_set_raises_wrong_px(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.set("foo", "bar", px=-100)
    with pytest.raises(ResponseError):
        r.set("foo", "bar", px=0)
    assert not r.exists("foo")


def test_set_using_timedelta_raises_wrong_px(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.set("foo", "bar", px=timedelta(milliseconds=-100))
    with pytest.raises(ResponseError):
        r.set("foo", "bar", px=timedelta(milliseconds=0))
    assert not r.exists("foo")


def test_setex_raises_wrong_ex(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.setex("foo", -100, "bar")
    with pytest.raises(ResponseError):
        r.setex("foo", 0, "bar")
    assert not r.exists("foo")


def test_setex_using_timedelta_raises_wrong_ex(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.setex("foo", timedelta(seconds=-100), "bar")
    with pytest.raises(ResponseError):
        r.setex("foo", timedelta(seconds=-100), "bar")
    assert not r.exists("foo")


def test_setnx(r: redis.Redis):
    assert r.setnx("foo", "bar")
    assert r.get("foo") == b"bar"
    assert not r.setnx("foo", "baz")
    assert r.get("foo") == b"bar"


def test_set_nx(r: redis.Redis):
    assert r.set("foo", "bar", nx=True) is True
    assert r.get("foo") == b"bar"
    assert r.set("foo", "bar", nx=True) is None
    assert r.get("foo") == b"bar"


def test_set_xx(r: redis.Redis):
    assert r.set("foo", "bar", xx=True) is None
    r.set("foo", "bar")
    assert r.set("foo", "bar", xx=True) is True


@pytest.mark.min_server("6.2")
def test_set_get(r: redis.Redis):
    assert raw_command(r, "set", "foo", "bar", "GET") is None
    assert r.get("foo") == b"bar"
    assert raw_command(r, "set", "foo", "baz", "GET") == b"bar"
    assert r.get("foo") == b"baz"


@pytest.mark.min_server("6.2")
def test_set_get_xx(r: redis.Redis):
    assert raw_command(r, "set", "foo", "bar", "XX", "GET") is None
    assert r.get("foo") is None
    r.set("foo", "bar")
    assert raw_command(r, "set", "foo", "baz", "XX", "GET") == b"bar"
    assert r.get("foo") == b"baz"
    assert raw_command(r, "set", "foo", "baz", "GET") == b"baz"


@pytest.mark.min_server("6.2")
@pytest.mark.max_server("6.2.7")
def test_set_get_nx_redis6(r: redis.Redis):
    # Note: this will most likely fail on a 7.0 server, based on the docs for SET
    with pytest.raises(redis.ResponseError):
        raw_command(r, "set", "foo", "bar", "NX", "GET")


@pytest.mark.min_server("7")
def test_set_get_nx_redis7(r: redis.Redis):
    # Note: this will most likely fail on a 7.0 server, based on the docs for SET
    assert raw_command(r, "set", "foo", "bar", "NX", "GET") is None


@pytest.mark.min_server("6.2")
def set_get_wrongtype(r: redis.Redis):
    r.lpush("foo", "bar")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "set", "foo", "bar", "GET")


def test_substr(r: redis.Redis):
    r["foo"] = "one_two_three"
    assert r.substr("foo", 0) == b"one_two_three"
    assert r.substr("foo", 0, 2) == b"one"
    assert r.substr("foo", 4, 6) == b"two"
    assert r.substr("foo", -5) == b"three"
    assert r.substr("foo", -4, -5) == b""
    assert r.substr("foo", -5, -3) == b"thr"


def test_substr_noexist_key(r: redis.Redis):
    assert r.substr("foo", 0) == b""
    assert r.substr("foo", 10) == b""
    assert r.substr("foo", -5, -1) == b""


def test_substr_wrong_type(r: redis.Redis):
    r.rpush("foo", b"x")
    with pytest.raises(redis.ResponseError):
        r.substr("foo", 0)


def test_strlen(r: redis.Redis):
    r["foo"] = "bar"

    assert r.strlen("foo") == 3
    assert r.strlen("noexists") == 0


def test_strlen_wrong_type(r: redis.Redis):
    r.rpush("foo", b"x")
    with pytest.raises(redis.ResponseError):
        r.strlen("foo")


def test_setrange(r: redis.Redis):
    r.set("foo", "test")
    assert r.setrange("foo", 1, "aste") == 5
    assert r.get("foo") == b"taste"

    r.set("foo", "test")
    assert r.setrange("foo", 1, "a") == 4
    assert r.get("foo") == b"tast"

    assert r.setrange("bar", 2, "test") == 6
    assert r.get("bar") == b"\x00\x00test"


def test_setrange_expiry(r: redis.Redis):
    r.set("foo", "test", ex=10)
    r.setrange("foo", 1, "aste")
    assert r.ttl("foo") > 0


def test_large_command(r: redis.Redis):
    r.set("foo", "bar" * 10000)
    assert r.get("foo") == b"bar" * 10000


def test_saving_non_ascii_chars_as_value(r: redis.Redis):
    assert r.set("foo", "Ñandu") is True
    assert r.get("foo") == "Ñandu".encode()


def test_saving_unicode_type_as_value(r: redis.Redis):
    assert r.set("foo", "Ñandu") is True
    assert r.get("foo") == "Ñandu".encode()


def test_saving_non_ascii_chars_as_key(r: redis.Redis):
    assert r.set("Ñandu", "foo") is True
    assert r.get("Ñandu") == b"foo"


def test_saving_unicode_type_as_key(r: redis.Redis):
    assert r.set("Ñandu", "foo") is True
    assert r.get("Ñandu") == b"foo"


def test_future_newbytes(r: redis.Redis):
    # bytes = pytest.importorskip('builtins', reason='future.types not available').bytes
    r.set(bytes(b"\xc3\x91andu"), "foo")
    assert r.get("Ñandu") == b"foo"


def test_future_newstr(r: redis.Redis):
    # str = pytest.importorskip('builtins', reason='future.types not available').str
    r.set(str("Ñandu"), "foo")
    assert r.get("Ñandu") == b"foo"


def test_setitem_getitem(r: redis.Redis):
    assert r.keys() == []
    r["foo"] = "bar"
    assert r["foo"] == b"bar"


def test_getitem_non_existent_key(r: redis.Redis):
    assert r.keys() == []
    assert "noexists" not in r.keys()


@pytest.mark.slow
def test_getex(r: redis.Redis):
    # Exceptions
    with pytest.raises(redis.ResponseError):
        raw_command(r, "getex", "foo", "px", 1000, "ex", 1)
    with pytest.raises(redis.ResponseError):
        raw_command(r, "getex", "foo", "dsac", 1000, "ex", 1)

    r.set("foo", "val")
    assert r.getex("foo", ex=1) == b"val"
    time.sleep(1.5)
    assert r.get("foo") is None

    r.set("foo2", "val")
    assert r.getex("foo2", px=1000) == b"val"
    time.sleep(1.5)
    assert r.get("foo2") is None

    r.set("foo4", "val")
    r.getex("foo4", exat=int(time.time() + 1))
    time.sleep(1.5)
    assert r.get("foo4") is None

    r.set("foo2", "val")
    r.getex("foo2", pxat=int(time.time() + 1) * 1000)
    time.sleep(1.5)
    assert r.get("foo2") is None

    r.setex("foo5", 1, "val")
    r.getex("foo5", persist=True)
    assert r.ttl("foo5") == -1
    time.sleep(1.5)
    assert r.get("foo5") == b"val"


@pytest.mark.min_server("7")
def test_lcs(r: redis.Redis):
    r.mset({"key1": "ohmytext", "key2": "mynewtext"})
    assert r.lcs("key1", "key2") == b"mytext"
    assert r.lcs("key1", "key2", len=True) == 6

    assert r.lcs("key1", "key2", idx=True, minmatchlen=3, withmatchlen=True) == [
        b"matches",
        [[[4, 7], [5, 8], 4]],
        b"len",
        6,
    ]
    assert r.lcs("key1", "key2", idx=True, minmatchlen=3) == [b"matches", [[[4, 7], [5, 8]]], b"len", 6]

    with pytest.raises(redis.ResponseError):
        assert r.lcs("key1", "key2", len=True, idx=True)
    with pytest.raises(redis.ResponseError):
        raw_command(r, "lcs", "key1", "key2", "not_supported_arg")
