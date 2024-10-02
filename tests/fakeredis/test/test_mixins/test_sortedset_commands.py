from __future__ import annotations

import math
from collections import OrderedDict
from typing import Tuple, List, Optional

import pytest
import redis
import redis.client

from test import testtools


def round_str(x):
    assert isinstance(x, bytes)
    return round(float(x))


def test_zpopmin(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zpopmin("foo", count=2) == [(b"one", 1.0), (b"two", 2.0)]
    assert r.zpopmin("foo", count=2) == [(b"three", 3.0)]


def test_zpopmin_too_many(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zpopmin("foo", count=5) == [(b"one", 1.0), (b"two", 2.0), (b"three", 3.0)]


def test_zpopmax(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zpopmax("foo", count=2) == [(b"three", 3.0), (b"two", 2.0)]
    assert r.zpopmax("foo", count=2) == [(b"one", 1.0)]


def test_zrange_same_score(r: redis.Redis):
    r.zadd("foo", {"two_a": 2})
    r.zadd("foo", {"two_b": 2})
    r.zadd("foo", {"two_c": 2})
    r.zadd("foo", {"two_d": 2})
    r.zadd("foo", {"two_e": 2})
    assert r.zrange("foo", 2, 3) == [b"two_c", b"two_d"]


def test_zrange_with_bylex_and_byscore(r: redis.Redis):
    r.zadd("foo", {"one_a": 0})
    r.zadd("foo", {"two_a": 0})
    r.zadd("foo", {"two_b": 0})
    r.zadd("foo", {"three_a": 0})
    with pytest.raises(redis.ResponseError):
        testtools.raw_command(r, "zrange", "foo", "(t", "+", "bylex", "byscore")


def test_zrange_with_rev_and_bylex(r: redis.Redis):
    r.zadd("foo", {"one_a": 0})
    r.zadd("foo", {"two_a": 0})
    r.zadd("foo", {"two_b": 0})
    r.zadd("foo", {"three_a": 0})
    assert r.zrange("foo", b"+", b"(t", desc=True, bylex=True) == [
        b"two_b",
        b"two_a",
        b"three_a",
    ]
    assert r.zrange("foo", b"[two_b", b"(t", desc=True, bylex=True) == [
        b"two_b",
        b"two_a",
        b"three_a",
    ]
    assert r.zrange("foo", b"(two_b", b"(t", desc=True, bylex=True) == [
        b"two_a",
        b"three_a",
    ]
    assert r.zrange("foo", b"[two_b", b"[three_a", desc=True, bylex=True) == [
        b"two_b",
        b"two_a",
        b"three_a",
    ]
    assert r.zrange("foo", b"[two_b", b"(three_a", desc=True, bylex=True) == [
        b"two_b",
        b"two_a",
    ]
    assert r.zrange("foo", b"(two_b", b"-", desc=True, bylex=True) == [
        b"two_a",
        b"three_a",
        b"one_a",
    ]
    assert r.zrange("foo", b"(two_b", b"[two_b", bylex=True) == []
    # reversed max + and min - boundaries
    # these will be always empty, but allowed by redis
    assert r.zrange("foo", b"-", b"+", desc=True, bylex=True) == []
    assert r.zrange("foo", b"[three_a", b"+", desc=True, bylex=True) == []
    assert r.zrange("foo", b"-", b"[o", desc=True, bylex=True) == []


def test_zrange_with_bylex(r: redis.Redis):
    r.zadd("foo", {"one_a": 0})
    r.zadd("foo", {"two_a": 0})
    r.zadd("foo", {"two_b": 0})
    r.zadd("foo", {"three_a": 0})
    assert r.zrange("foo", b"(t", b"+", bylex=True) == [b"three_a", b"two_a", b"two_b"]
    assert r.zrange("foo", b"(t", b"[two_b", bylex=True) == [
        b"three_a",
        b"two_a",
        b"two_b",
    ]
    assert r.zrange("foo", b"(t", b"(two_b", bylex=True) == [b"three_a", b"two_a"]
    assert r.zrange("foo", b"[three_a", b"[two_b", bylex=True) == [
        b"three_a",
        b"two_a",
        b"two_b",
    ]
    assert r.zrange("foo", b"(three_a", b"[two_b", bylex=True) == [b"two_a", b"two_b"]
    assert r.zrange("foo", b"-", b"(two_b", bylex=True) == [
        b"one_a",
        b"three_a",
        b"two_a",
    ]
    assert r.zrange("foo", b"[two_b", b"(two_b", bylex=True) == []
    # reversed max + and min - boundaries
    # these will be always empty, but allowed by redis
    assert r.zrange("foo", b"+", b"-", bylex=True) == []
    assert r.zrange("foo", b"+", b"[three_a", bylex=True) == []
    assert r.zrange("foo", b"[o", b"-", bylex=True) == []


def test_zrange_with_byscore(r: redis.Redis):
    r.zadd("foo", {"zero": 0})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"two_a_also": 2})
    r.zadd("foo", {"two_b_also": 2})
    r.zadd("foo", {"four": 4})
    assert r.zrange("foo", 1, 3, byscore=True) == [b"two", b"two_a_also", b"two_b_also"]
    assert r.zrange("foo", 2, 3, byscore=True) == [b"two", b"two_a_also", b"two_b_also"]
    assert r.zrange("foo", 0, 4, byscore=True) == [
        b"zero",
        b"two",
        b"two_a_also",
        b"two_b_also",
        b"four",
    ]
    assert r.zrange("foo", "-inf", 1, byscore=True) == [b"zero"]
    assert r.zrange("foo", 2, "+inf", byscore=True) == [
        b"two",
        b"two_a_also",
        b"two_b_also",
        b"four",
    ]
    assert r.zrange("foo", "-inf", "+inf", byscore=True) == [
        b"zero",
        b"two",
        b"two_a_also",
        b"two_b_also",
        b"four",
    ]


def test_zcard(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    assert r.zcard("foo") == 2


def test_zcard_non_existent_key(r: redis.Redis):
    assert r.zcard("foo") == 0


def test_zcard_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zcard("foo")


def test_zcount(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"three": 2})
    r.zadd("foo", {"five": 5})
    assert r.zcount("foo", 2, 4) == 1
    assert r.zcount("foo", 1, 4) == 2
    assert r.zcount("foo", 0, 5) == 3
    assert r.zcount("foo", 4, "+inf") == 1
    assert r.zcount("foo", "-inf", 4) == 2
    assert r.zcount("foo", "-inf", "+inf") == 3


def test_zcount_exclusive(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"three": 2})
    r.zadd("foo", {"five": 5})
    assert r.zcount("foo", "-inf", "(2") == 1
    assert r.zcount("foo", "-inf", 2) == 2
    assert r.zcount("foo", "(5", "+inf") == 0
    assert r.zcount("foo", "(1", 5) == 2
    assert r.zcount("foo", "(2", "(5") == 0
    assert r.zcount("foo", "(1", "(5") == 1
    assert r.zcount("foo", 2, "(5") == 1


def test_zcount_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zcount("foo", "-inf", "+inf")


def test_zincrby(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    assert r.zincrby("foo", 10, "one") == 11
    assert r.zrange("foo", 0, -1, withscores=True) == [(b"one", 11)]


def test_zincrby_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zincrby("foo", 10, "one")


def test_zrange_descending(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zrange("foo", 0, -1, desc=True) == [b"three", b"two", b"one"]


def test_zrange_descending_with_scores(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zrange("foo", 0, -1, desc=True, withscores=True) == [
        (b"three", 3),
        (b"two", 2),
        (b"one", 1),
    ]


def test_zrange_with_positive_indices(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zrange("foo", 0, 1) == [b"one", b"two"]


def test_zrange_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zrange("foo", 0, -1)


def test_zrange_score_cast(r: redis.Redis):
    r.zadd("foo", {"one": 1.2})
    r.zadd("foo", {"two": 2.2})

    expected_without_cast_round = [(b"one", 1.2), (b"two", 2.2)]
    expected_with_cast_round = [(b"one", 1.0), (b"two", 2.0)]
    assert r.zrange("foo", 0, 2, withscores=True) == expected_without_cast_round
    assert (
        r.zrange("foo", 0, 2, withscores=True, score_cast_func=round_str)
        == expected_with_cast_round
    )


def test_zrank(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zrank("foo", "one") == 0
    assert r.zrank("foo", "two") == 1
    assert r.zrank("foo", "three") == 2


def test_zrank_non_existent_member(r: redis.Redis):
    assert r.zrank("foo", "one") is None


def test_zrank_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zrank("foo", "one")


def test_zrem(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    r.zadd("foo", {"four": 4})
    assert r.zrem("foo", "one") == 1
    assert r.zrange("foo", 0, -1) == [b"two", b"three", b"four"]
    # Since redis>=2.7.6 returns number of deleted items.
    assert r.zrem("foo", "two", "three") == 2
    assert r.zrange("foo", 0, -1) == [b"four"]
    assert r.zrem("foo", "three", "four") == 1
    assert r.zrange("foo", 0, -1) == []
    assert r.zrem("foo", "three", "four") == 0


def test_zrem_non_existent_member(r: redis.Redis):
    assert not r.zrem("foo", "one")


def test_zrem_numeric_member(r: redis.Redis):
    r.zadd("foo", {"128": 13.0, "129": 12.0})
    assert r.zrem("foo", 128) == 1
    assert r.zrange("foo", 0, -1) == [b"129"]


def test_zrem_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zrem("foo", "bar")


def test_zscore(r: redis.Redis):
    r.zadd("foo", {"one": 54})
    assert r.zscore("foo", "one") == 54


def test_zscore_non_existent_member(r: redis.Redis):
    assert r.zscore("foo", "one") is None


def test_zscore_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zscore("foo", "one")


def test_zmscore(r: redis.Redis):
    """When all the requested sorted-set members are in the cache, a valid
    float value should be returned for each requested member.

    The order of the returned scores should always match the order in
    which the set members were supplied.
    """
    cache_key: str = "scored-set-members"
    members: Tuple[str, ...] = ("one", "two", "three", "four", "five", "six")
    scores: Tuple[float, ...] = (1.1, 2.2, 3.3, 4.4, 5.5, 6.6)

    r.zadd(cache_key, dict(zip(members, scores)))
    cached_scores: List[Optional[float]] = r.zmscore(
        cache_key,
        list(members),
    )

    assert all(cached_scores[idx] == score for idx, score in enumerate(scores))


def test_zmscore_missing_members(r: redis.Redis):
    """When none of the requested sorted-set members are in the cache, a value
    of `None` should be returned once for each requested member."""
    cache_key: str = "scored-set-members"
    members: Tuple[str, ...] = ("one", "two", "three", "four", "five", "six")

    r.zadd(cache_key, {"eight": 8.8})
    cached_scores: List[Optional[float]] = r.zmscore(
        cache_key,
        list(members),
    )

    assert all(score is None for score in cached_scores)


def test_zmscore_mixed_membership(r: redis.Redis):
    """When only some requested sorted-set members are in the cache, a
    valid float value should be returned for each present member and `None` for
    each missing member.

    The order of the returned scores should always match the order in
    which the set members were supplied.
    """
    cache_key: str = "scored-set-members"
    members: Tuple[str, ...] = ("one", "two", "three", "four", "five", "six")
    scores: Tuple[float, ...] = (1.1, 2.2, 3.3, 4.4, 5.5, 6.6)

    r.zadd(
        cache_key,
        dict(
            (member, scores[idx])
            for (idx, member) in enumerate(members)
            if idx % 2 != 0
        ),
    )

    cached_scores: List[Optional[float]] = r.zmscore(cache_key, list(members))

    assert all(
        cached_scores[idx] is None for (idx, score) in enumerate(scores) if idx % 2 == 0
    )
    assert all(
        cached_scores[idx] == score
        for (idx, score) in enumerate(scores)
        if idx % 2 != 0
    )


def test_zrevrank(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zrevrank("foo", "one") == 2
    assert r.zrevrank("foo", "two") == 1
    assert r.zrevrank("foo", "three") == 0


def test_zrevrank_non_existent_member(r: redis.Redis):
    assert r.zrevrank("foo", "one") is None


def test_zrevrank_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zrevrank("foo", "one")


def test_zrevrange(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zrevrange("foo", 0, 1) == [b"three", b"two"]
    assert r.zrevrange("foo", 0, -1) == [b"three", b"two", b"one"]


def test_zrevrange_sorted_keys(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"two_b": 2})
    r.zadd("foo", {"three": 3})
    assert r.zrevrange("foo", 0, 2) == [b"three", b"two_b", b"two"]
    assert r.zrevrange("foo", 0, -1) == [b"three", b"two_b", b"two", b"one"]


def test_zrevrange_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zrevrange("foo", 0, 2)


def test_zrevrange_score_cast(r: redis.Redis):
    r.zadd("foo", {"one": 1.2})
    r.zadd("foo", {"two": 2.2})

    expected_without_cast_round = [(b"two", 2.2), (b"one", 1.2)]
    expected_with_cast_round = [(b"two", 2.0), (b"one", 1.0)]
    assert r.zrevrange("foo", 0, 2, withscores=True) == expected_without_cast_round
    assert (
        r.zrevrange("foo", 0, 2, withscores=True, score_cast_func=round_str)
        == expected_with_cast_round
    )


def test_zrange_with_large_int(r: redis.Redis):
    with pytest.raises(
        redis.ResponseError, match="value is not an integer or out of range"
    ):
        r.zrange("", 0, 9223372036854775808)
    with pytest.raises(
        redis.ResponseError, match="value is not an integer or out of range"
    ):
        r.zrange("", 0, -9223372036854775809)


def test_zrangebyscore(r: redis.Redis):
    r.zadd("foo", {"zero": 0})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"two_a_also": 2})
    r.zadd("foo", {"two_b_also": 2})
    r.zadd("foo", {"four": 4})
    assert r.zrangebyscore("foo", 1, 3) == [b"two", b"two_a_also", b"two_b_also"]
    assert r.zrangebyscore("foo", 2, 3) == [b"two", b"two_a_also", b"two_b_also"]
    assert r.zrangebyscore("foo", 0, 4) == [
        b"zero",
        b"two",
        b"two_a_also",
        b"two_b_also",
        b"four",
    ]
    assert r.zrangebyscore("foo", "-inf", 1) == [b"zero"]
    assert r.zrangebyscore("foo", 2, "+inf") == [
        b"two",
        b"two_a_also",
        b"two_b_also",
        b"four",
    ]
    assert r.zrangebyscore("foo", "-inf", "+inf") == [
        b"zero",
        b"two",
        b"two_a_also",
        b"two_b_also",
        b"four",
    ]


def test_zrangebysore_exclusive(r: redis.Redis):
    r.zadd("foo", {"zero": 0})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"four": 4})
    r.zadd("foo", {"five": 5})
    assert r.zrangebyscore("foo", "(0", 6) == [b"two", b"four", b"five"]
    assert r.zrangebyscore("foo", "(2", "(5") == [b"four"]
    assert r.zrangebyscore("foo", 0, "(4") == [b"zero", b"two"]


def test_zrangebyscore_raises_error(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    with pytest.raises(redis.ResponseError):
        r.zrangebyscore("foo", "one", 2)
    with pytest.raises(redis.ResponseError):
        r.zrangebyscore("foo", 2, "three")
    with pytest.raises(redis.ResponseError):
        r.zrangebyscore("foo", 2, "3)")
    with pytest.raises(redis.RedisError):
        r.zrangebyscore("foo", 2, "3)", 0, None)


def test_zrangebyscore_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zrangebyscore("foo", "(1", "(2")


def test_zrangebyscore_slice(r: redis.Redis):
    r.zadd("foo", {"two_a": 2})
    r.zadd("foo", {"two_b": 2})
    r.zadd("foo", {"two_c": 2})
    r.zadd("foo", {"two_d": 2})
    assert r.zrangebyscore("foo", 0, 4, 0, 2) == [b"two_a", b"two_b"]
    assert r.zrangebyscore("foo", 0, 4, 1, 3) == [b"two_b", b"two_c", b"two_d"]


def test_zrangebyscore_withscores(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zrangebyscore("foo", 1, 3, 0, 2, True) == [(b"one", 1), (b"two", 2)]


def test_zrangebyscore_cast_scores(r: redis.Redis):
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"two_a_also": 2.2})

    expected_without_cast_round = [(b"two", 2.0), (b"two_a_also", 2.2)]
    expected_with_cast_round = [(b"two", 2.0), (b"two_a_also", 2.0)]
    assert sorted(r.zrangebyscore("foo", 2, 3, withscores=True)) == sorted(
        expected_without_cast_round
    )
    assert sorted(
        r.zrangebyscore("foo", 2, 3, withscores=True, score_cast_func=round_str)
    ) == sorted(expected_with_cast_round)


def test_zrevrangebyscore(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zrevrangebyscore("foo", 3, 1) == [b"three", b"two", b"one"]
    assert r.zrevrangebyscore("foo", 3, 2) == [b"three", b"two"]
    assert r.zrevrangebyscore("foo", 3, 1, 0, 1) == [b"three"]
    assert r.zrevrangebyscore("foo", 3, 1, 1, 2) == [b"two", b"one"]


def test_zrevrangebyscore_exclusive(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zrevrangebyscore("foo", "(3", 1) == [b"two", b"one"]
    assert r.zrevrangebyscore("foo", 3, "(2") == [b"three"]
    assert r.zrevrangebyscore("foo", "(3", "(1") == [b"two"]
    assert r.zrevrangebyscore("foo", "(2", 1, 0, 1) == [b"one"]
    assert r.zrevrangebyscore("foo", "(2", "(1", 0, 1) == []
    assert r.zrevrangebyscore("foo", "(3", "(0", 1, 2) == [b"one"]


def test_zrevrangebyscore_raises_error(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    with pytest.raises(redis.ResponseError):
        r.zrevrangebyscore("foo", "three", 1)
    with pytest.raises(redis.ResponseError):
        r.zrevrangebyscore("foo", 3, "one")
    with pytest.raises(redis.ResponseError):
        r.zrevrangebyscore("foo", 3, "1)")
    with pytest.raises(redis.ResponseError):
        r.zrevrangebyscore("foo", "((3", "1)")


def test_zrevrangebyscore_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zrevrangebyscore("foo", "(3", "(1")


def test_zrevrangebyscore_cast_scores(r: redis.Redis):
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"two_a_also": 2.2})

    assert r.zrevrangebyscore("foo", 3, 2, withscores=True) == [
        (b"two_a_also", 2.2),
        (b"two", 2.0),
    ]

    assert r.zrevrangebyscore(
        "foo", 3, 2, withscores=True, score_cast_func=round_str
    ) == [
        (b"two_a_also", 2.0),
        (b"two", 2.0),
    ]


def test_zrangebylex(r: redis.Redis):
    r.zadd("foo", {"one_a": 0})
    r.zadd("foo", {"two_a": 0})
    r.zadd("foo", {"two_b": 0})
    r.zadd("foo", {"three_a": 0})
    assert r.zrangebylex("foo", b"(t", b"+") == [b"three_a", b"two_a", b"two_b"]
    assert r.zrangebylex("foo", b"(t", b"[two_b") == [b"three_a", b"two_a", b"two_b"]
    assert r.zrangebylex("foo", b"(t", b"(two_b") == [b"three_a", b"two_a"]
    assert r.zrangebylex("foo", b"[three_a", b"[two_b") == [
        b"three_a",
        b"two_a",
        b"two_b",
    ]
    assert r.zrangebylex("foo", b"(three_a", b"[two_b") == [b"two_a", b"two_b"]
    assert r.zrangebylex("foo", b"-", b"(two_b") == [b"one_a", b"three_a", b"two_a"]
    assert r.zrangebylex("foo", b"[two_b", b"(two_b") == []
    # reversed max + and min - boundaries
    # these will be always empty, but allowed by redis
    assert r.zrangebylex("foo", b"+", b"-") == []
    assert r.zrangebylex("foo", b"+", b"[three_a") == []
    assert r.zrangebylex("foo", b"[o", b"-") == []


def test_zrangebylex_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zrangebylex("foo", b"-", b"+")


def test_zlexcount(r: redis.Redis):
    r.zadd("foo", {"one_a": 0})
    r.zadd("foo", {"two_a": 0})
    r.zadd("foo", {"two_b": 0})
    r.zadd("foo", {"three_a": 0})
    assert r.zlexcount("foo", b"(t", b"+") == 3
    assert r.zlexcount("foo", b"(t", b"[two_b") == 3
    assert r.zlexcount("foo", b"(t", b"(two_b") == 2
    assert r.zlexcount("foo", b"[three_a", b"[two_b") == 3
    assert r.zlexcount("foo", b"(three_a", b"[two_b") == 2
    assert r.zlexcount("foo", b"-", b"(two_b") == 3
    assert r.zlexcount("foo", b"[two_b", b"(two_b") == 0
    # reversed max + and min - boundaries
    # these will be always empty, but allowed by redis
    assert r.zlexcount("foo", b"+", b"-") == 0
    assert r.zlexcount("foo", b"+", b"[three_a") == 0
    assert r.zlexcount("foo", b"[o", b"-") == 0


def test_zlexcount_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zlexcount("foo", b"-", b"+")


def test_zrangebylex_with_limit(r: redis.Redis):
    r.zadd("foo", {"one_a": 0})
    r.zadd("foo", {"two_a": 0})
    r.zadd("foo", {"two_b": 0})
    r.zadd("foo", {"three_a": 0})
    assert r.zrangebylex("foo", b"-", b"+", 1, 2) == [b"three_a", b"two_a"]

    # negative offset no results
    assert r.zrangebylex("foo", b"-", b"+", -1, 3) == []

    # negative limit ignored
    assert r.zrangebylex("foo", b"-", b"+", 0, -2) == [
        b"one_a",
        b"three_a",
        b"two_a",
        b"two_b",
    ]
    assert r.zrangebylex("foo", b"-", b"+", 1, -2) == [b"three_a", b"two_a", b"two_b"]
    assert r.zrangebylex("foo", b"+", b"-", 1, 1) == []


def test_zrangebylex_raises_error(r: redis.Redis):
    r.zadd("foo", {"one_a": 0})
    r.zadd("foo", {"two_a": 0})
    r.zadd("foo", {"two_b": 0})
    r.zadd("foo", {"three_a": 0})

    with pytest.raises(redis.ResponseError):
        r.zrangebylex("foo", b"", b"[two_b")

    with pytest.raises(redis.ResponseError):
        r.zrangebylex("foo", b"-", b"two_b")

    with pytest.raises(redis.ResponseError):
        r.zrangebylex("foo", b"(t", b"two_b")

    with pytest.raises(redis.ResponseError):
        r.zrangebylex("foo", b"t", b"+")

    with pytest.raises(redis.ResponseError):
        r.zrangebylex("foo", b"[two_a", b"")

    with pytest.raises(redis.RedisError):
        r.zrangebylex("foo", b"(two_a", b"[two_b", 1)


def test_zrevrangebylex(r: redis.Redis):
    r.zadd("foo", {"one_a": 0})
    r.zadd("foo", {"two_a": 0})
    r.zadd("foo", {"two_b": 0})
    r.zadd("foo", {"three_a": 0})
    assert r.zrevrangebylex("foo", b"+", b"(t") == [b"two_b", b"two_a", b"three_a"]
    assert r.zrevrangebylex("foo", b"[two_b", b"(t") == [b"two_b", b"two_a", b"three_a"]
    assert r.zrevrangebylex("foo", b"(two_b", b"(t") == [b"two_a", b"three_a"]
    assert r.zrevrangebylex("foo", b"[two_b", b"[three_a") == [
        b"two_b",
        b"two_a",
        b"three_a",
    ]
    assert r.zrevrangebylex("foo", b"[two_b", b"(three_a") == [b"two_b", b"two_a"]
    assert r.zrevrangebylex("foo", b"(two_b", b"-") == [b"two_a", b"three_a", b"one_a"]
    assert r.zrangebylex("foo", b"(two_b", b"[two_b") == []
    # reversed max + and min - boundaries
    # these will be always empty, but allowed by redis
    assert r.zrevrangebylex("foo", b"-", b"+") == []
    assert r.zrevrangebylex("foo", b"[three_a", b"+") == []
    assert r.zrevrangebylex("foo", b"-", b"[o") == []


def test_zrevrangebylex_with_limit(r: redis.Redis):
    r.zadd("foo", {"one_a": 0})
    r.zadd("foo", {"two_a": 0})
    r.zadd("foo", {"two_b": 0})
    r.zadd("foo", {"three_a": 0})
    assert r.zrevrangebylex("foo", b"+", b"-", 1, 2) == [b"two_a", b"three_a"]


def test_zrevrangebylex_raises_error(r: redis.Redis):
    r.zadd("foo", {"one_a": 0})
    r.zadd("foo", {"two_a": 0})
    r.zadd("foo", {"two_b": 0})
    r.zadd("foo", {"three_a": 0})

    with pytest.raises(redis.ResponseError):
        r.zrevrangebylex("foo", b"[two_b", b"")

    with pytest.raises(redis.ResponseError):
        r.zrevrangebylex("foo", b"two_b", b"-")

    with pytest.raises(redis.ResponseError):
        r.zrevrangebylex("foo", b"two_b", b"(t")

    with pytest.raises(redis.ResponseError):
        r.zrevrangebylex("foo", b"+", b"t")

    with pytest.raises(redis.ResponseError):
        r.zrevrangebylex("foo", b"", b"[two_a")

    with pytest.raises(redis.RedisError):
        r.zrevrangebylex("foo", b"[two_a", b"(two_b", 1)


def test_zrevrangebylex_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zrevrangebylex("foo", b"+", b"-")


def test_zremrangebyrank(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zremrangebyrank("foo", 0, 1) == 2
    assert r.zrange("foo", 0, -1) == [b"three"]


def test_zremrangebyrank_negative_indices(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zremrangebyrank("foo", -2, -1) == 2
    assert r.zrange("foo", 0, -1) == [b"one"]


def test_zremrangebyrank_out_of_bounds(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    assert r.zremrangebyrank("foo", 1, 3) == 0


def test_zremrangebyrank_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zremrangebyrank("foo", 1, 3)


def test_zremrangebyscore(r: redis.Redis):
    r.zadd("foo", {"zero": 0})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"four": 4})
    # Outside of range.
    assert r.zremrangebyscore("foo", 5, 10) == 0
    assert r.zrange("foo", 0, -1) == [b"zero", b"two", b"four"]
    # Middle of range.
    assert r.zremrangebyscore("foo", 1, 3) == 1
    assert r.zrange("foo", 0, -1) == [b"zero", b"four"]
    assert r.zremrangebyscore("foo", 1, 3) == 0
    # Entire range.
    assert r.zremrangebyscore("foo", 0, 4) == 2
    assert r.zrange("foo", 0, -1) == []


def test_zremrangebyscore_exclusive(r: redis.Redis):
    r.zadd("foo", {"zero": 0})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"four": 4})
    assert r.zremrangebyscore("foo", "(0", 1) == 0
    assert r.zrange("foo", 0, -1) == [b"zero", b"two", b"four"]
    assert r.zremrangebyscore("foo", "-inf", "(0") == 0
    assert r.zrange("foo", 0, -1) == [b"zero", b"two", b"four"]
    assert r.zremrangebyscore("foo", "(2", 5) == 1
    assert r.zrange("foo", 0, -1) == [b"zero", b"two"]
    assert r.zremrangebyscore("foo", 0, "(2") == 1
    assert r.zrange("foo", 0, -1) == [b"two"]
    assert r.zremrangebyscore("foo", "(1", "(3") == 1
    assert r.zrange("foo", 0, -1) == []


def test_zremrangebyscore_raises_error(r: redis.Redis):
    r.zadd("foo", {"zero": 0})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"four": 4})
    with pytest.raises(redis.ResponseError):
        r.zremrangebyscore("foo", "three", 1)
    with pytest.raises(redis.ResponseError):
        r.zremrangebyscore("foo", 3, "one")
    with pytest.raises(redis.ResponseError):
        r.zremrangebyscore("foo", 3, "1)")
    with pytest.raises(redis.ResponseError):
        r.zremrangebyscore("foo", "((3", "1)")


def test_zremrangebyscore_badkey(r: redis.Redis):
    assert r.zremrangebyscore("foo", 0, 2) == 0


def test_zremrangebyscore_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zremrangebyscore("foo", 0, 2)


def test_zremrangebylex(r: redis.Redis):
    r.zadd("foo", {"two_a": 0})
    r.zadd("foo", {"two_b": 0})
    r.zadd("foo", {"one_a": 0})
    r.zadd("foo", {"three_a": 0})
    assert r.zremrangebylex("foo", b"(three_a", b"[two_b") == 2
    assert r.zremrangebylex("foo", b"(three_a", b"[two_b") == 0
    assert r.zremrangebylex("foo", b"-", b"(o") == 0
    assert r.zremrangebylex("foo", b"-", b"[one_a") == 1
    assert r.zremrangebylex("foo", b"[tw", b"+") == 0
    assert r.zremrangebylex("foo", b"[t", b"+") == 1
    assert r.zremrangebylex("foo", b"[t", b"+") == 0


def test_zremrangebylex_error(r: redis.Redis):
    r.zadd("foo", {"two_a": 0})
    r.zadd("foo", {"two_b": 0})
    r.zadd("foo", {"one_a": 0})
    r.zadd("foo", {"three_a": 0})
    with pytest.raises(redis.ResponseError):
        r.zremrangebylex("foo", b"(t", b"two_b")

    with pytest.raises(redis.ResponseError):
        r.zremrangebylex("foo", b"t", b"+")

    with pytest.raises(redis.ResponseError):
        r.zremrangebylex("foo", b"[two_a", b"")


def test_zremrangebylex_badkey(r: redis.Redis):
    assert r.zremrangebylex("foo", b"(three_a", b"[two_b") == 0


def test_zremrangebylex_wrong_type(r: redis.Redis):
    r.sadd("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zremrangebylex("foo", b"bar", b"baz")


def test_zunionstore(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("bar", {"one": 1})
    r.zadd("bar", {"two": 2})
    r.zadd("bar", {"three": 3})
    r.zunionstore("baz", ["foo", "bar"])
    assert r.zrange("baz", 0, -1, withscores=True) == [
        (b"one", 2),
        (b"three", 3),
        (b"two", 4),
    ]


def test_zunionstore_sum(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("bar", {"one": 1})
    r.zadd("bar", {"two": 2})
    r.zadd("bar", {"three": 3})
    r.zunionstore("baz", ["foo", "bar"], aggregate="SUM")
    assert r.zrange("baz", 0, -1, withscores=True) == [
        (b"one", 2),
        (b"three", 3),
        (b"two", 4),
    ]


def test_zunionstore_max(r: redis.Redis):
    r.zadd("foo", {"one": 0})
    r.zadd("foo", {"two": 0})
    r.zadd("bar", {"one": 1})
    r.zadd("bar", {"two": 2})
    r.zadd("bar", {"three": 3})
    r.zunionstore("baz", ["foo", "bar"], aggregate="MAX")
    assert r.zrange("baz", 0, -1, withscores=True) == [
        (b"one", 1),
        (b"two", 2),
        (b"three", 3),
    ]


def test_zunionstore_min(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("bar", {"one": 0})
    r.zadd("bar", {"two": 0})
    r.zadd("bar", {"three": 3})
    r.zunionstore("baz", ["foo", "bar"], aggregate="MIN")
    assert r.zrange("baz", 0, -1, withscores=True) == [
        (b"one", 0),
        (b"two", 0),
        (b"three", 3),
    ]


def test_zunionstore_weights(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("bar", {"one": 1})
    r.zadd("bar", {"two": 2})
    r.zadd("bar", {"four": 4})
    r.zunionstore("baz", {"foo": 1, "bar": 2}, aggregate="SUM")
    assert r.zrange("baz", 0, -1, withscores=True) == [
        (b"one", 3),
        (b"two", 6),
        (b"four", 8),
    ]


def test_zunionstore_nan_to_zero(r: redis.Redis):
    r.zadd("foo", {"x": math.inf})
    r.zadd("foo2", {"x": math.inf})
    r.zunionstore("bar", OrderedDict([("foo", 1.0), ("foo2", 0.0)]))
    # This is different to test_zinterstore_nan_to_zero because of a quirk
    # in redis. See https://github.com/antirez/redis/issues/3954.
    assert r.zscore("bar", "x") == math.inf


def test_zunionstore_nan_to_zero2(r: redis.Redis):
    r.zadd("foo", {"zero": 0})
    r.zadd("foo2", {"one": 1})
    r.zadd("foo3", {"one": 1})
    r.zunionstore("bar", {"foo": math.inf}, aggregate="SUM")
    assert r.zrange("bar", 0, -1, withscores=True) == [(b"zero", 0)]
    r.zunionstore("bar", OrderedDict([("foo2", math.inf), ("foo3", -math.inf)]))
    assert r.zrange("bar", 0, -1, withscores=True) == [(b"one", 0)]


def test_zunionstore_nan_to_zero_ordering(r: redis.Redis):
    r.zadd("foo", {"e1": math.inf})
    r.zadd("bar", {"e1": -math.inf, "e2": 0.0})
    r.zunionstore("baz", ["foo", "bar", "foo"])
    assert r.zscore("baz", "e1") == 0.0


@pytest.mark.unsupported_server_types("dragonfly")  # TODO Should pass?
def test_zunionstore_mixed_set_types(r: redis.Redis):
    # No score, redis will use 1.0.
    r.sadd("foo", "one")
    r.sadd("foo", "two")
    r.zadd("bar", {"one": 1})
    r.zadd("bar", {"two": 2})
    r.zadd("bar", {"three": 3})
    r.zunionstore("baz", ["foo", "bar"], aggregate="SUM")
    assert r.zrange("baz", 0, -1, withscores=True) == [
        (b"one", 2),
        (b"three", 3),
        (b"two", 3),
    ]


def test_zunionstore_badkey(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zunionstore("baz", ["foo", "bar"], aggregate="SUM")
    assert r.zrange("baz", 0, -1, withscores=True) == [(b"one", 1), (b"two", 2)]
    r.zunionstore("baz", {"foo": 1, "bar": 2}, aggregate="SUM")
    assert r.zrange("baz", 0, -1, withscores=True) == [(b"one", 1), (b"two", 2)]


@pytest.mark.unsupported_server_types("dragonfly")  # TODO Should pass?
def test_zunionstore_wrong_type(r: redis.Redis):
    r.set("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zunionstore("baz", ["foo", "bar"])


def test_zinterstore(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("bar", {"one": 1})
    r.zadd("bar", {"two": 2})
    r.zadd("bar", {"three": 3})
    r.zinterstore("baz", ["foo", "bar"])
    assert r.zrange("baz", 0, -1, withscores=True) == [(b"one", 2), (b"two", 4)]


@pytest.mark.unsupported_server_types("dragonfly")
def test_zinterstore_mixed_set_types(r: redis.Redis):
    r.sadd("foo", "one")
    r.sadd("foo", "two")
    r.zadd("bar", {"one": 1})
    r.zadd("bar", {"two": 2})
    r.zadd("bar", {"three": 3})
    r.zinterstore("baz", ["foo", "bar"], aggregate="SUM")
    assert r.zrange("baz", 0, -1, withscores=True) == [(b"one", 2), (b"two", 3)]


def test_zinterstore_max(r: redis.Redis):
    r.zadd("foo", {"one": 0})
    r.zadd("foo", {"two": 0})
    r.zadd("bar", {"one": 1})
    r.zadd("bar", {"two": 2})
    r.zadd("bar", {"three": 3})
    r.zinterstore("baz", ["foo", "bar"], aggregate="MAX")
    assert r.zrange("baz", 0, -1, withscores=True) == [(b"one", 1), (b"two", 2)]


def test_zinterstore_onekey(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zinterstore("baz", ["foo"], aggregate="MAX")
    assert r.zrange("baz", 0, -1, withscores=True) == [(b"one", 1)]


def test_zinterstore_nokey(r: redis.Redis):
    with pytest.raises(redis.ResponseError):
        r.zinterstore("baz", [], aggregate="MAX")


@pytest.mark.unsupported_server_types("dragonfly")  # TODO causes a crash!
def test_zinterstore_nan_to_zero(r: redis.Redis):
    r.zadd("foo", {"x": math.inf})
    r.zadd("foo2", {"x": math.inf})
    r.zinterstore("bar", OrderedDict([("foo", 1.0), ("foo2", 0.0)]))
    assert r.zscore("bar", "x") == 0.0


def test_zunionstore_nokey(r: redis.Redis):
    with pytest.raises(redis.ResponseError):
        r.zunionstore("baz", [], aggregate="MAX")


@pytest.mark.unsupported_server_types("dragonfly")  # TODO Hang server
def test_zinterstore_wrong_type(r: redis.Redis):
    r.set("foo", "bar")
    with pytest.raises(redis.ResponseError):
        r.zinterstore("baz", ["foo", "bar"])


def test_empty_zset(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zrem("foo", "one")
    assert not r.exists("foo")


def test_zpopmax_too_many(r: redis.Redis):
    r.zadd("foo", {"one": 1})
    r.zadd("foo", {"two": 2})
    r.zadd("foo", {"three": 3})
    assert r.zpopmax("foo", count=5) == [
        (b"three", 3.0),
        (b"two", 2.0),
        (b"one", 1.0),
    ]


def test_bzpopmin(r: redis.Redis):
    r.zadd("foo", {"one": 1, "two": 2, "three": 3})
    r.zadd("bar", {"a": 1.5, "b": 2, "c": 3})
    assert r.bzpopmin(["foo", "bar"], 0) == (b"foo", b"one", 1.0)
    assert r.bzpopmin(["foo", "bar"], 0) == (b"foo", b"two", 2.0)
    assert r.bzpopmin(["foo", "bar"], 0) == (b"foo", b"three", 3.0)
    assert r.bzpopmin(["foo", "bar"], 0) == (b"bar", b"a", 1.5)


def test_bzpopmax(r: redis.Redis):
    r.zadd("foo", {"one": 1, "two": 2, "three": 3})
    r.zadd("bar", {"a": 1.5, "b": 2.5, "c": 3.5})
    assert r.bzpopmax(["foo", "bar"], 0) == (b"foo", b"three", 3.0)
    assert r.bzpopmax(["foo", "bar"], 0) == (b"foo", b"two", 2.0)
    assert r.bzpopmax(["foo", "bar"], 0) == (b"foo", b"one", 1.0)
    assert r.bzpopmax(["foo", "bar"], 0) == (b"bar", b"c", 3.5)


def test_zscan(r: redis.Redis):
    # Set up the data
    name = "zscan-test"
    for ix in range(20):
        r.zadd(name, {"key:%s" % ix: ix})
    expected = dict(r.zrange(name, 0, -1, withscores=True))

    # Test the basic version
    results = {}
    for key, val in r.zscan_iter(name, count=6):
        results[key] = val
    assert results == expected

    # Now test that the MATCH functionality works
    results = {}
    cursor = "0"
    while cursor != 0:
        cursor, data = r.zscan(name, cursor, match="*7", count=6)
        results.update(data)
    assert results == {b"key:7": 7.0, b"key:17": 17.0}


def test_zrandemember(r: redis.Redis):
    r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
    assert r.zrandmember("a") is not None
    assert len(r.zrandmember("a", 2)) == 2
    # with scores
    assert len(r.zrandmember("a", 2, True)) == 4
    # without duplications
    assert len(r.zrandmember("a", 10)) == 5
    # with duplications
    assert len(r.zrandmember("a", -10)) == 10


@pytest.mark.unsupported_server_types("dragonfly")
def test_zdiffstore(r: redis.Redis):
    r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
    r.zadd("b", {"a1": 1, "a2": 2})
    assert r.zdiffstore("out", ["a", "b"])
    assert r.zrange("out", 0, -1) == [b"a3"]
    assert r.zrange("out", 0, -1, withscores=True) == [(b"a3", 3.0)]


def test_zdiff(r: redis.Redis):
    r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
    r.zadd("b", {"a1": 1, "a2": 2})
    assert r.zdiff(["a", "b"]) == [b"a3"]
    assert r.zdiff(["a", "b"], withscores=True) == [b"a3", b"3"]


def test_zunion(r: redis.Redis):
    r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
    r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
    r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
    # sum
    assert r.zunion(["a", "b", "c"]) == [b"a2", b"a4", b"a3", b"a1"]
    assert r.zunion(["a", "b", "c"], withscores=True) == [
        (b"a2", 3),
        (b"a4", 4),
        (b"a3", 8),
        (b"a1", 9),
    ]
    # max
    assert r.zunion(["a", "b", "c"], aggregate="MAX", withscores=True) == [
        (b"a2", 2),
        (b"a4", 4),
        (b"a3", 5),
        (b"a1", 6),
    ]
    # min
    assert r.zunion(["a", "b", "c"], aggregate="MIN", withscores=True) == [
        (b"a1", 1),
        (b"a2", 1),
        (b"a3", 1),
        (b"a4", 4),
    ]
    # with weight
    assert r.zunion({"a": 1, "b": 2, "c": 3}, withscores=True) == [
        (b"a2", 5),
        (b"a4", 12),
        (b"a3", 20),
        (b"a1", 23),
    ]


def test_zinter(r: redis.Redis):
    r.zadd("a", {"a1": 1, "a2": 2, "a3": 1})
    r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
    r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
    assert r.zinter(["a", "b", "c"]) == [b"a3", b"a1"]
    # invalid aggregation
    with pytest.raises(redis.DataError):
        r.zinter(["a", "b", "c"], aggregate="foo", withscores=True)
    # aggregate with SUM
    assert r.zinter(["a", "b", "c"], withscores=True) == [(b"a3", 8), (b"a1", 9)]
    # aggregate with MAX
    assert r.zinter(["a", "b", "c"], aggregate="MAX", withscores=True) == [
        (b"a3", 5),
        (b"a1", 6),
    ]
    # aggregate with MIN
    assert r.zinter(["a", "b", "c"], aggregate="MIN", withscores=True) == [
        (b"a1", 1),
        (b"a3", 1),
    ]
    # with weights
    assert r.zinter({"a": 1, "b": 2, "c": 3}, withscores=True) == [
        (b"a3", 20),
        (b"a1", 23),
    ]


@pytest.mark.min_server("7")
def test_zintercard(r: redis.Redis):
    r.zadd("a", {"a1": 1, "a2": 2, "a3": 1})
    r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
    r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
    assert r.zintercard(3, ["a", "b", "c"]) == 2
    assert r.zintercard(3, ["a", "b", "c"], limit=1) == 1


@pytest.mark.unsupported_server_types("dragonfly")
def test_zrangestore(r: redis.Redis):
    r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
    assert r.zrangestore("b", "a", 0, 1)
    assert r.zrange("b", 0, -1) == [b"a1", b"a2"]
    assert r.zrangestore("b", "a", 1, 2)
    assert r.zrange("b", 0, -1) == [b"a2", b"a3"]
    assert r.zrange("b", 0, -1, withscores=True) == [(b"a2", 2), (b"a3", 3)]
    # reversed order
    assert r.zrangestore("b", "a", 1, 2, desc=True)
    assert r.zrange("b", 0, -1) == [b"a1", b"a2"]
    # by score
    assert r.zrangestore("b", "a", 2, 1, byscore=True, offset=0, num=1, desc=True)
    assert r.zrange("b", 0, -1) == [b"a2"]
    # by lex
    assert r.zrange("a", "[a2", "(a3", bylex=True, offset=0, num=1) == [b"a2"]
    assert r.zrangestore("b", "a", "[a2", "(a3", bylex=True, offset=0, num=1)
    assert r.zrange("b", 0, -1) == [b"a2"]


@pytest.mark.min_server("7")
@pytest.mark.unsupported_server_types("dragonfly")
def test_zmpop(r: redis.Redis):
    r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
    res = [b"a", [[b"a1", b"1"], [b"a2", b"2"]]]
    assert r.zmpop("2", ["b", "a"], min=True, count=2) == res
    with pytest.raises(redis.DataError):
        r.zmpop("2", ["b", "a"], count=2)
    r.zadd("b", {"b1": 10, "ab": 9, "b3": 8})
    assert r.zmpop("2", ["b", "a"], max=True) == [b"b", [[b"b1", b"10"]]]


@pytest.mark.min_server("7")
@pytest.mark.unsupported_server_types("dragonfly")
def test_bzmpop(r: redis.Redis):
    r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
    res = [b"a", [[b"a1", b"1"], [b"a2", b"2"]]]
    assert r.bzmpop(1, "2", ["b", "a"], min=True, count=2) == res
    with pytest.raises(redis.DataError):
        r.bzmpop(1, "2", ["b", "a"], count=2)
    r.zadd("b", {"b1": 10, "ab": 9, "b3": 8})
    res = [b"b", [[b"b1", b"10"]]]
    assert r.bzmpop(0, "2", ["b", "a"], max=True) == res
    assert r.bzmpop(1, "2", ["foo", "bar"], max=True) is None
