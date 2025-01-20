from __future__ import annotations

import os
from datetime import timedelta
from time import sleep

import pytest
import redis
import redis.client
from redis.exceptions import ResponseError


def test_sadd(r: redis.Redis):
    assert r.sadd("foo", "member1") == 1
    assert r.sadd("foo", "member1") == 0
    assert set(r.smembers("foo")) == {b"member1"}
    assert r.sadd("foo", "member2", "member3") == 2
    assert set(r.smembers("foo")) == {b"member1", b"member2", b"member3"}
    assert r.sadd("foo", "member3", "member4") == 1
    assert set(r.smembers("foo")) == {b"member1", b"member2", b"member3", b"member4"}


def test_sadd_redispy_5(r: redis.Redis):
    assert r.sadd("foo", "member1") == 1
    assert r.sadd("foo", "member1") == 0
    assert r.smembers("foo") == {b"member1"}
    assert r.sadd("foo", "member2", "member3") == 2
    assert r.smembers("foo") == {b"member1", b"member2", b"member3"}
    assert r.sadd("foo", "member3", "member4") == 1
    assert r.smembers("foo") == {b"member1", b"member2", b"member3", b"member4"}


def test_sadd_as_str_type(r: redis.Redis):
    assert r.sadd("foo", *range(3)) == 3
    assert set(r.smembers("foo")) == {b"0", b"1", b"2"}


def test_sadd_wrong_type(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    with pytest.raises(redis.ResponseError):
        r.sadd("foo", "member2")


def test_scard(r: redis.Redis):
    r.sadd("foo", "member1")
    r.sadd("foo", "member2")
    r.sadd("foo", "member2")
    assert r.scard("foo") == 2


def test_scard_wrong_type(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    with pytest.raises(redis.ResponseError):
        r.scard("foo")


def test_sdiff(r: redis.Redis):
    r.sadd("foo", "member1")
    r.sadd("foo", "member2")
    r.sadd("bar", "member2")
    r.sadd("bar", "member3")
    assert set(r.sdiff("foo", "bar")) == {b"member1"}
    # Original sets shouldn't be modified.
    assert set(r.smembers("foo")) == {b"member1", b"member2"}
    assert set(r.smembers("bar")) == {b"member2", b"member3"}


def test_sdiff_one_key(r: redis.Redis):
    r.sadd("foo", "member1")
    r.sadd("foo", "member2")
    assert set(r.sdiff("foo")) == {b"member1", b"member2"}


def test_sdiff_empty(r: redis.Redis):
    assert set(r.sdiff("foo")) == set()


def test_sdiff_wrong_type(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    r.sadd("bar", "member")
    with pytest.raises(redis.ResponseError):
        r.sdiff("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.sdiff("bar", "foo")


def test_sdiffstore(r: redis.Redis):
    r.sadd("foo", "member1")
    r.sadd("foo", "member2")
    r.sadd("bar", "member2")
    r.sadd("bar", "member3")
    assert r.sdiffstore("baz", "foo", "bar") == 1

    # Catch instances where we store bytes and strings inconsistently
    # and thus baz = {'member1', b'member1'}
    r.sadd("baz", "member1")
    assert r.scard("baz") == 1


def test_sinter(r: redis.Redis):
    r.sadd("foo", "member1")
    r.sadd("foo", "member2")
    r.sadd("bar", "member2")
    r.sadd("bar", "member3")
    assert set(r.sinter("foo", "bar")) == {b"member2"}
    assert set(r.sinter("foo")) == {b"member1", b"member2"}


def test_sinter_bytes_keys(r: redis.Redis):
    foo = os.urandom(10)
    bar = os.urandom(10)
    r.sadd(foo, "member1")
    r.sadd(foo, "member2")
    r.sadd(bar, "member2")
    r.sadd(bar, "member3")
    assert set(r.sinter(foo, bar)) == {b"member2"}
    assert set(r.sinter(foo)) == {b"member1", b"member2"}


def test_sinter_wrong_type(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    r.sadd("bar", "member")
    with pytest.raises(redis.ResponseError):
        r.sinter("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.sinter("bar", "foo")


def test_sinterstore(r: redis.Redis):
    r.sadd("foo", "member1")
    r.sadd("foo", "member2")
    r.sadd("bar", "member2")
    r.sadd("bar", "member3")
    assert r.sinterstore("baz", "foo", "bar") == 1

    # Catch instances where we store bytes and strings inconsistently
    # and thus baz = {'member2', b'member2'}
    r.sadd("baz", "member2")
    assert r.scard("baz") == 1


def test_sismember(r: redis.Redis):
    assert not r.sismember("foo", "member1")
    r.sadd("foo", "member1")
    assert r.sismember("foo", "member1")


def test_smismember(r: redis.Redis):
    assert r.smismember("foo", ["member1", "member2", "member3"]) == [0, 0, 0]
    r.sadd("foo", "member1", "member2", "member3")
    assert r.smismember("foo", ["member1", "member2", "member3"]) == [1, 1, 1]
    assert r.smismember("foo", ["member1", "member2", "member3", "member4"]) == [
        1,
        1,
        1,
        0,
    ]
    assert r.smismember("foo", ["member4", "member2", "member3"]) == [0, 1, 1]
    # should also work if provided values as arguments
    assert r.smismember("foo", "member4", "member2", "member3") == [0, 1, 1]


def test_smismember_wrong_type(r: redis.Redis):
    # verify that command fails when the key itself is not a SET
    r.zadd("foo", {"member": 1})
    with pytest.raises(redis.ResponseError):
        r.smismember("foo", "member")

    # verify that command fails if the input parameter is of wrong type
    r.sadd("foo2", "member1", "member2", "member3")
    with pytest.raises(redis.DataError, match="Invalid input of type"):
        r.smismember("foo2", [["member1", "member2"]])


def test_sismember_wrong_type(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    with pytest.raises(redis.ResponseError):
        r.sismember("foo", "member")


def test_smembers(r: redis.Redis):
    assert set(r.smembers("foo")) == set()


def test_smembers_copy(r: redis.Redis):
    r.sadd("foo", "member1")
    ret = r.smembers("foo")
    r.sadd("foo", "member2")
    assert r.smembers("foo") != ret


def test_smembers_wrong_type(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    with pytest.raises(redis.ResponseError):
        r.smembers("foo")


def test_smembers_runtime_error(r: redis.Redis):
    r.sadd("foo", "member1", "member2")
    for member in r.smembers("foo"):
        r.srem("foo", member)


def test_smove(r: redis.Redis):
    r.sadd("foo", "member1")
    r.sadd("foo", "member2")
    assert r.smove("foo", "bar", "member1")
    assert set(r.smembers("bar")) == {b"member1"}


def test_smove_non_existent_key(r: redis.Redis):
    assert not r.smove("foo", "bar", "member1")


def test_smove_wrong_type(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    r.sadd("bar", "member")
    with pytest.raises(redis.ResponseError):
        r.smove("bar", "foo", "member")
    # Must raise the error before removing member from bar
    assert set(r.smembers("bar")) == {b"member"}
    with pytest.raises(redis.ResponseError):
        r.smove("foo", "bar", "member")


def test_spop(r: redis.Redis):
    # This is tricky because it pops a random element.
    r.sadd("foo", "member1")
    assert r.spop("foo") == b"member1"
    assert r.spop("foo") is None


def test_spop_wrong_type(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    with pytest.raises(redis.ResponseError):
        r.spop("foo")


def test_srandmember(r: redis.Redis):
    r.sadd("foo", "member1")
    assert r.srandmember("foo") == b"member1"
    # Shouldn't be removed from the set.
    assert r.srandmember("foo") == b"member1"


def test_srandmember_number(r: redis.Redis):
    """srandmember works with the number argument."""
    assert r.srandmember("foo", 2) == []
    r.sadd("foo", b"member1")
    assert r.srandmember("foo", 2) == [b"member1"]
    r.sadd("foo", b"member2")
    assert set(r.srandmember("foo", 2)) == {b"member1", b"member2"}
    r.sadd("foo", b"member3")
    res = r.srandmember("foo", 2)
    assert len(res) == 2
    for e in res:
        assert e in {b"member1", b"member2", b"member3"}


def test_srandmember_wrong_type(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    with pytest.raises(redis.ResponseError):
        r.srandmember("foo")


def test_srem(r: redis.Redis):
    r.sadd("foo", "member1", "member2", "member3", "member4")
    assert set(r.smembers("foo")) == {b"member1", b"member2", b"member3", b"member4"}
    assert r.srem("foo", "member1") == 1
    assert set(r.smembers("foo")) == {b"member2", b"member3", b"member4"}
    assert r.srem("foo", "member1") == 0
    # Since redis>=2.7.6 returns number of deleted items.
    assert r.srem("foo", "member2", "member3") == 2
    assert set(r.smembers("foo")) == {b"member4"}
    assert r.srem("foo", "member3", "member4") == 1
    assert set(r.smembers("foo")) == set()
    assert r.srem("foo", "member3", "member4") == 0


def test_srem_wrong_type(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    with pytest.raises(redis.ResponseError):
        r.srem("foo", "member")


def test_sunion(r: redis.Redis):
    r.sadd("foo", "member1")
    r.sadd("foo", "member2")
    r.sadd("bar", "member2")
    r.sadd("bar", "member3")
    assert set(r.sunion("foo", "bar")) == {b"member1", b"member2", b"member3"}


def test_sunion_wrong_type(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    r.sadd("bar", "member")
    with pytest.raises(redis.ResponseError):
        r.sunion("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.sunion("bar", "foo")


def test_sunionstore(r: redis.Redis):
    r.sadd("foo", "member1")
    r.sadd("foo", "member2")
    r.sadd("bar", "member2")
    r.sadd("bar", "member3")
    assert r.sunionstore("baz", "foo", "bar") == 3
    assert set(r.smembers("baz")) == {b"member1", b"member2", b"member3"}

    # Catch instances where we store bytes and strings inconsistently
    # and thus baz = {b'member1', b'member2', b'member3', 'member3'}
    r.sadd("baz", "member3")
    assert r.scard("baz") == 3


def test_empty_set(r: redis.Redis):
    r.sadd("foo", "bar")
    r.srem("foo", "bar")
    assert not r.exists("foo")


def test_sscan(r: redis.Redis):
    # Set up the data
    name = "sscan-test"
    for ix in range(20):
        k = "sscan-test:%s" % ix
        r.sadd(name, k)
    expected = r.smembers(name)
    assert len(expected) == 20  # Ensure we know what we're testing

    # Test that we page through the results and get everything out
    results = []
    cursor = "0"
    while cursor != 0:
        cursor, data = r.sscan(name, cursor, count=6)
        results.extend(data)
    assert set(expected) == set(results)

    # Test the iterator version
    results = [r for r in r.sscan_iter(name, count=6)]
    assert set(expected) == set(results)

    # Now test that the MATCH functionality works
    results = []
    cursor = "0"
    while cursor != 0:
        cursor, data = r.sscan(name, cursor, match="*7", count=100)
        results.extend(data)
    assert b"sscan-test:7" in results
    assert b"sscan-test:17" in results
    assert len(results) == 2

    # Test the match on iterator
    results = [r for r in r.sscan_iter(name, match="*7")]
    assert b"sscan-test:7" in results
    assert b"sscan-test:17" in results
    assert len(results) == 2


@pytest.mark.min_server("7")
def test_sintercard(r: redis.Redis):
    r.sadd("foo", "member1")
    r.sadd("foo", "member2")
    r.sadd("bar", "member2")
    r.sadd("bar", "member3")
    assert r.sintercard(2, ["foo", "bar"]) == 1
    assert r.sintercard(1, ["foo"]) == 2


@pytest.mark.min_server("7")
def test_sintercard_key_doesnt_exist(r: redis.Redis):
    r.sadd("foo", "member1")
    r.sadd("foo", "member2")
    r.sadd("bar", "member2")
    r.sadd("bar", "member3")
    assert r.sintercard(2, ["foo", "bar"]) == 1
    assert r.sintercard(1, ["foo"]) == 2
    assert r.sintercard(1, ["foo"], limit=1) == 1
    assert r.sintercard(3, ["foo", "bar", "ddd"]) == 0


@pytest.mark.min_server("7")
def test_sintercard_bytes_keys(r: redis.Redis):
    foo = os.urandom(10)
    bar = os.urandom(10)
    r.sadd(foo, "member1")
    r.sadd(foo, "member2")
    r.sadd(bar, "member2")
    r.sadd(bar, "member3")
    assert r.sintercard(2, [foo, bar]) == 1
    assert r.sintercard(1, [foo]) == 2
    assert r.sintercard(1, [foo], limit=1) == 1


@pytest.mark.min_server("7")
def test_sintercard_wrong_type(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    r.sadd("bar", "member")
    with pytest.raises(redis.ResponseError):
        r.sintercard(2, ["foo", "bar"])
    with pytest.raises(redis.ResponseError):
        r.sintercard(2, ["bar", "foo"])


@pytest.mark.min_server("7")
def test_sintercard_syntax_error(r: redis.Redis):
    r.zadd("foo", {"member": 1})
    r.sadd("bar", "member")
    with pytest.raises(redis.ResponseError):
        r.sintercard(3, ["foo", "bar"])
    with pytest.raises(redis.ResponseError):
        r.sintercard(1, ["bar", "foo"])
    with pytest.raises(redis.ResponseError):
        r.sintercard(1, ["bar", "foo"], limit="x")


def test_pfadd(r: redis.Redis):
    key = "hll-pfadd"
    assert r.pfadd(key, "a", "b", "c", "d", "e", "f", "g") == 1
    assert r.pfcount(key) == 7


def test_pfcount(r: redis.Redis):
    key1 = "hll-pfcount01"
    key2 = "hll-pfcount02"
    key3 = "hll-pfcount03"
    assert r.pfadd(key1, "foo", "bar", "zap") == 1
    assert r.pfadd(key1, "zap", "zap", "zap") == 0
    assert r.pfadd(key1, "foo", "bar") == 0
    assert r.pfcount(key1) == 3
    assert r.pfadd(key2, "1", "2", "3") == 1
    assert r.pfcount(key2) == 3
    assert r.pfcount(key1, key2) == 6
    assert r.pfadd(key3, "foo", "bar", "zip") == 1
    assert r.pfcount(key3) == 3
    assert r.pfcount(key1, key3) == 4
    assert r.pfcount(key1, key2, key3) == 7


def test_pfmerge(r: redis.Redis):
    key1 = "hll-pfmerge01"
    key2 = "hll-pfmerge02"
    key3 = "hll-pfmerge03"
    assert r.pfadd(key1, "foo", "bar", "zap", "a") == 1
    assert r.pfadd(key2, "a", "b", "c", "foo") == 1
    assert r.pfmerge(key3, key1, key2)
    assert r.pfcount(key3) == 6


@pytest.mark.slow
def test_set_ex_should_expire_value(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    r.set("foo", "bar", ex=1)
    sleep(2)
    assert r.get("foo") is None


@pytest.mark.slow
def test_set_px_should_expire_value(r: redis.Redis):
    r.set("foo", "bar", px=500)
    sleep(1.5)
    assert r.get("foo") is None


@pytest.mark.slow
def test_psetex_expire_value(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.psetex("foo", 0, "bar")
    r.psetex("foo", 500, "bar")
    sleep(1.5)
    assert r.get("foo") is None


@pytest.mark.slow
def test_psetex_expire_value_using_timedelta(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.psetex("foo", timedelta(seconds=0), "bar")
    r.psetex("foo", timedelta(seconds=0.5), "bar")
    sleep(1.5)
    assert r.get("foo") is None
