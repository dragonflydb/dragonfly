import pytest
import redis
import redis.client
from packaging.version import Version

from test.testtools import raw_command

REDIS_VERSION = Version(redis.__version__)


def test_zadd(r: redis.Redis):
    r.zadd("foo", {"four": 4})
    r.zadd("foo", {"three": 3})
    assert r.zadd("foo", {"two": 2, "one": 1, "zero": 0}) == 3
    assert r.zrange("foo", 0, -1) == [b"zero", b"one", b"two", b"three", b"four"]
    assert r.zadd("foo", {"zero": 7, "one": 1, "five": 5}) == 1
    assert r.zrange("foo", 0, -1) == [b"one", b"two", b"three", b"four", b"five", b"zero"]


def test_zadd_empty(r: redis.Redis):
    # Have to add at least one key/value pair
    with pytest.raises(redis.RedisError):
        r.zadd("foo", {})


@pytest.mark.max_server("6.2.7")
def test_zadd_minus_zero_redis6(r: redis.Redis):
    # Changing -0 to +0 is ignored
    r.zadd("foo", {"a": -0.0})
    r.zadd("foo", {"a": 0.0})
    assert raw_command(r, "zscore", "foo", "a") == b"-0"


@pytest.mark.min_server("7")
def test_zadd_minus_zero_redis7(r: redis.Redis):
    r.zadd("foo", {"a": -0.0})
    r.zadd("foo", {"a": 0.0})
    assert raw_command(r, "zscore", "foo", "a") == b"0"


def test_zadd_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zadd("foo", {"two": 2})


def test_zadd_multiple(r: redis.Redis):
    r.zadd("foo", {"one": 1, "two": 2})
    assert r.zrange("foo", 0, 0) == [b"one"]
    assert r.zrange("foo", 1, 1) == [b"two"]


@pytest.mark.parametrize(
    "param,return_value,state",
    [
        ({"four": 2.0, "three": 1.0}, 0, [(b"three", 3.0), (b"four", 4.0)]),
        ({"four": 2.0, "three": 1.0, "zero": 0.0}, 1, [(b"zero", 0.0), (b"three", 3.0), (b"four", 4.0)]),
        ({"two": 2.0, "one": 1.0}, 2, [(b"one", 1.0), (b"two", 2.0), (b"three", 3.0), (b"four", 4.0)]),
    ],
)
@pytest.mark.parametrize("ch", [False, True])
def test_zadd_with_nx(r, param, return_value, state, ch):
    r.zadd("foo", {"four": 4.0, "three": 3.0})
    assert r.zadd("foo", param, nx=True, ch=ch) == return_value
    assert r.zrange("foo", 0, -1, withscores=True) == state


@pytest.mark.parametrize(
    "param,return_value,state",
    [
        ({"four": 2.0, "three": 1.0}, 0, [(b"three", 3.0), (b"four", 4.0)]),
        (
            {"four": 5.0, "three": 1.0, "zero": 0.0},
            2,
            [
                (b"zero", 0.0),
                (b"three", 3.0),
                (b"four", 5.0),
            ],
        ),
        ({"two": 2.0, "one": 1.0}, 2, [(b"one", 1.0), (b"two", 2.0), (b"three", 3.0), (b"four", 4.0)]),
    ],
)
def test_zadd_with_gt_and_ch(r, param, return_value, state):
    r.zadd("foo", {"four": 4.0, "three": 3.0})
    assert r.zadd("foo", param, gt=True, ch=True) == return_value
    assert r.zrange("foo", 0, -1, withscores=True) == state


@pytest.mark.parametrize(
    "param,return_value,state",
    [
        ({"four": 2.0, "three": 1.0}, 0, [(b"three", 3.0), (b"four", 4.0)]),
        ({"four": 5.0, "three": 1.0, "zero": 0.0}, 1, [(b"zero", 0.0), (b"three", 3.0), (b"four", 5.0)]),
        ({"two": 2.0, "one": 1.0}, 2, [(b"one", 1.0), (b"two", 2.0), (b"three", 3.0), (b"four", 4.0)]),
    ],
)
def test_zadd_with_gt(r, param, return_value, state):
    r.zadd("foo", {"four": 4.0, "three": 3.0})
    assert r.zadd("foo", param, gt=True) == return_value
    assert r.zrange("foo", 0, -1, withscores=True) == state


@pytest.mark.parametrize(
    "param,return_value,state",
    [
        ({"four": 4.0, "three": 1.0}, 1, [(b"three", 1.0), (b"four", 4.0)]),
        ({"four": 4.0, "three": 1.0, "zero": 0.0}, 2, [(b"zero", 0.0), (b"three", 1.0), (b"four", 4.0)]),
        ({"two": 2.0, "one": 1.0}, 2, [(b"one", 1.0), (b"two", 2.0), (b"three", 3.0), (b"four", 4.0)]),
    ],
)
def test_zadd_with_ch(r, param, return_value, state):
    r.zadd("foo", {"four": 4.0, "three": 3.0})
    assert r.zadd("foo", param, ch=True) == return_value
    assert r.zrange("foo", 0, -1, withscores=True) == state


@pytest.mark.parametrize(
    "param,changed,state",
    [
        ({"four": 2.0, "three": 1.0}, 2, [(b"three", 1.0), (b"four", 2.0)]),
        ({"four": 4.0, "three": 3.0, "zero": 0.0}, 0, [(b"three", 3.0), (b"four", 4.0)]),
        ({"two": 2.0, "one": 1.0}, 0, [(b"three", 3.0), (b"four", 4.0)]),
    ],
)
@pytest.mark.parametrize("ch", [False, True])
def test_zadd_with_xx(r, param, changed, state, ch):
    r.zadd("foo", {"four": 4.0, "three": 3.0})
    assert r.zadd("foo", param, xx=True, ch=ch) == (changed if ch else 0)
    assert r.zrange("foo", 0, -1, withscores=True) == state


@pytest.mark.parametrize("ch", [False, True])
def test_zadd_with_nx_and_xx(r, ch):
    r.zadd("foo", {"four": 4.0, "three": 3.0})
    with pytest.raises(redis.DataError):
        r.zadd("foo", {"four": -4.0, "three": -3.0}, nx=True, xx=True, ch=ch)


@pytest.mark.parametrize("ch", [False, True])
def test_zadd_incr(r, ch):
    r.zadd("foo", {"four": 4.0, "three": 3.0})
    assert r.zadd("foo", {"four": 1.0}, incr=True, ch=ch) == 5.0
    assert r.zadd("foo", {"three": 1.0}, incr=True, nx=True, ch=ch) is None
    assert r.zscore("foo", "three") == 3.0
    assert r.zadd("foo", {"bar": 1.0}, incr=True, xx=True, ch=ch) is None
    assert r.zadd("foo", {"three": 1.0}, incr=True, xx=True, ch=ch) == 4.0


def test_zadd_with_xx_and_gt_and_ch(r: redis.Redis):
    r.zadd("test", {"one": 1})
    assert r.zscore("test", "one") == 1.0
    assert r.zadd("test", {"one": 4}, xx=True, gt=True, ch=True) == 1
    assert r.zscore("test", "one") == 4.0
    assert r.zadd("test", {"one": 0}, xx=True, gt=True, ch=True) == 0
    assert r.zscore("test", "one") == 4.0


def test_zadd_and_zrangebyscore(r: redis.Redis):
    raw_command(r, "zadd", "", 0.0, "")
    assert raw_command(r, "zrangebyscore", "", 0.0, 0.0, "limit", 0, 0) == []
    with pytest.raises(redis.RedisError):
        raw_command(r, "zrangebyscore", "", 0.0, 0.0, "limit", 0)
    with pytest.raises(redis.RedisError):
        raw_command(r, "zadd", "t", 0.0, "xx", "")
