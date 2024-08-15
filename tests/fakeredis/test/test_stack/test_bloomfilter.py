import pytest
import redis

from fakeredis import _msgs as msgs

bloom_tests = pytest.importorskip("probables")


def test_bf_add(r: redis.Redis):
    assert r.bf().add("key", "value") == 1
    assert r.bf().add("key", "value") == 0

    r.set("key1", "value")
    with pytest.raises(redis.exceptions.ResponseError):
        r.bf().add("key1", "v")


def test_bf_madd(r: redis.Redis):
    assert r.bf().madd("key", "v1", "v2", "v2") == [1, 1, 0]
    assert r.bf().madd("key", "v1", "v2", "v4") == [0, 0, 1]

    r.set("key1", "value")
    with pytest.raises(redis.exceptions.ResponseError):
        r.bf().add("key1", "v")


@pytest.mark.unsupported_server_types("dragonfly")
def test_bf_card(r: redis.Redis):
    assert r.bf().madd("key", "v1", "v2", "v3") == [1, 1, 1]
    assert r.bf().card("key") == 3
    assert r.bf().card("key-new") == 0

    r.set("key1", "value")
    with pytest.raises(redis.exceptions.ResponseError):
        r.bf().card("key1")


def test_bf_exists(r: redis.Redis):
    assert r.bf().madd("key", "v1", "v2", "v3") == [1, 1, 1]
    assert r.bf().exists("key", "v1") == 1
    assert r.bf().exists("key", "v5") == 0
    assert r.bf().exists("key-new", "v5") == 0

    r.set("key1", "value")
    with pytest.raises(redis.exceptions.ResponseError):
        r.bf().add("key1", "v")


def test_bf_mexists(r: redis.Redis):
    assert r.bf().madd("key", "v1", "v2", "v3") == [1, 1, 1]
    assert r.bf().mexists("key", "v1") == [
        1,
    ]
    assert r.bf().mexists("key", "v1", "v5") == [1, 0]
    assert r.bf().mexists("key-new", "v5") == [
        0,
    ]

    r.set("key1", "value")
    with pytest.raises(redis.exceptions.ResponseError):
        r.bf().add("key1", "v")


def test_bf_reserve(r: redis.Redis):
    assert r.bf().reserve("bloom", 0.01, 1000)
    assert r.bf().reserve("bloom_ns", 0.01, 1000, noScale=True)
    with pytest.raises(redis.exceptions.ResponseError, match=msgs.NONSCALING_FILTERS_CANNOT_EXPAND_MSG):
        assert r.bf().reserve("bloom_e", 0.01, 1000, expansion=1, noScale=True)
    with pytest.raises(redis.exceptions.ResponseError, match=msgs.ITEM_EXISTS_MSG):
        assert r.bf().reserve("bloom", 0.01, 1000)


@pytest.mark.unsupported_server_types("dragonfly")
def test_bf_insert(r: redis.Redis):
    assert r.bf().create("bloom", 0.01, 1000)
    assert r.bf().insert("bloom", ["foo"]) == [1]
    assert r.bf().insert("bloom", ["foo", "bar"]) == [0, 1]
    assert r.bf().insert("captest", ["foo"], capacity=10) == [1]
    assert r.bf().insert("errtest", ["foo"], error=0.01) == [1]
    assert r.bf().exists("bloom", "foo") == 1
    assert r.bf().exists("bloom", "noexist") == 0
    assert r.bf().mexists("bloom", "foo", "noexist") == [1, 0]
    with pytest.raises(redis.exceptions.ResponseError, match=msgs.NOT_FOUND_MSG):
        r.bf().insert("nocreate", [1, 2, 3], noCreate=True)
    # with pytest.raises(redis.exceptions.ResponseError, match=msgs.NONSCALING_FILTERS_CANNOT_EXPAND_MSG):
    #     r.bf().insert("nocreate", [1, 2, 3], expansion=2, noScale=True)
