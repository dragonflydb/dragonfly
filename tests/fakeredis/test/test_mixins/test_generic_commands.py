from datetime import datetime, timedelta
from time import sleep, time

import pytest
import redis
from redis.exceptions import ResponseError

from fakeredis import _msgs as msgs
from test.testtools import raw_command


@pytest.mark.slow
def test_expireat_should_expire_key_by_datetime(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    r.expireat("foo", datetime.now() + timedelta(seconds=1))
    sleep(1.5)
    assert r.get("foo") is None
    assert r.expireat("bar", datetime.now()) is False


@pytest.mark.slow
def test_expireat_should_expire_key_by_timestamp(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    r.expireat("foo", int(time() + 1))
    sleep(1.5)
    assert r.get("foo") is None
    assert r.expire("bar", 1) is False


def test_expireat_should_return_true_for_existing_key(r: redis.Redis):
    r.set("foo", "bar")
    assert r.expireat("foo", int(time() + 1)) is True


def test_expireat_should_return_false_for_missing_key(r: redis.Redis):
    assert r.expireat("missing", int(time() + 1)) is False


def test_del_operator(r: redis.Redis):
    r["foo"] = "bar"
    del r["foo"]
    assert r.get("foo") is None


def test_expire_should_not_handle_floating_point_values(r: redis.Redis):
    r.set("foo", "bar")
    with pytest.raises(redis.ResponseError, match="value is not an integer or out of range"):
        r.expire("something_new", 1.2)
        r.pexpire("something_new", 1000.2)
        r.expire("some_unused_key", 1.2)
        r.pexpire("some_unused_key", 1000.2)


def test_ttl_should_return_minus_one_for_non_expiring_key(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    assert r.ttl("foo") == -1


def test_sort_range_offset_range(r: redis.Redis):
    r.rpush("foo", "2")
    r.rpush("foo", "1")
    r.rpush("foo", "4")
    r.rpush("foo", "3")

    assert r.sort("foo", start=0, num=2) == [b"1", b"2"]


def test_sort_range_offset_range_and_desc(r: redis.Redis):
    r.rpush("foo", "2")
    r.rpush("foo", "1")
    r.rpush("foo", "4")
    r.rpush("foo", "3")

    assert r.sort("foo", start=0, num=1, desc=True) == [b"4"]


def test_sort_range_offset_norange(r: redis.Redis):
    with pytest.raises(redis.RedisError):
        r.sort("foo", start=1)


def test_sort_range_with_large_range(r: redis.Redis):
    r.rpush("foo", "2")
    r.rpush("foo", "1")
    r.rpush("foo", "4")
    r.rpush("foo", "3")
    # num=20 even though len(foo) is 4.
    assert r.sort("foo", start=1, num=20) == [b"2", b"3", b"4"]


def test_sort_descending(r: redis.Redis):
    r.rpush("foo", "1")
    r.rpush("foo", "2")
    r.rpush("foo", "3")
    assert r.sort("foo", desc=True) == [b"3", b"2", b"1"]


def test_sort_alpha(r: redis.Redis):
    r.rpush("foo", "2a")
    r.rpush("foo", "1b")
    r.rpush("foo", "2b")
    r.rpush("foo", "1a")

    assert r.sort("foo", alpha=True) == [b"1a", b"1b", b"2a", b"2b"]


def test_sort_foo(r: redis.Redis):
    r.rpush("foo", "2a")
    r.rpush("foo", "1b")
    r.rpush("foo", "2b")
    r.rpush("foo", "1a")
    with pytest.raises(redis.ResponseError):
        r.sort("foo", alpha=False)


def test_sort_empty(r: redis.Redis):
    assert r.sort("foo") == []


def test_sort_wrong_type(r: redis.Redis):
    r.set("string", "3")
    with pytest.raises(redis.ResponseError):
        r.sort("string")


def test_sort_with_store_option(r: redis.Redis):
    r.rpush("foo", "2")
    r.rpush("foo", "1")
    r.rpush("foo", "4")
    r.rpush("foo", "3")

    assert r.sort("foo", store="bar") == 4
    assert r.lrange("bar", 0, -1) == [b"1", b"2", b"3", b"4"]


def test_sort_with_by_and_get_option(r: redis.Redis):
    r.rpush("foo", "2")
    r.rpush("foo", "1")
    r.rpush("foo", "4")
    r.rpush("foo", "3")

    r["weight_1"] = "4"
    r["weight_2"] = "3"
    r["weight_3"] = "2"
    r["weight_4"] = "1"

    r["data_1"] = "one"
    r["data_2"] = "two"
    r["data_3"] = "three"
    r["data_4"] = "four"

    assert r.sort("foo", by="weight_*", get="data_*") == [b"four", b"three", b"two", b"one"]
    assert r.sort("foo", by="weight_*", get="#") == [b"4", b"3", b"2", b"1"]
    assert r.sort("foo", by="weight_*", get=("data_*", "#")) == [
        b"four",
        b"4",
        b"three",
        b"3",
        b"two",
        b"2",
        b"one",
        b"1",
    ]
    assert r.sort("foo", by="weight_*", get="data_1") == [None, None, None, None]
    # Test sort with different parameters order
    assert raw_command(r, "sort", "foo", "get", "data_*", "by", "weight_*", "get", "#") == [
        b"four",
        b"4",
        b"three",
        b"3",
        b"two",
        b"2",
        b"one",
        b"1",
    ]


def test_sort_with_hash(r: redis.Redis):
    r.rpush("foo", "middle")
    r.rpush("foo", "eldest")
    r.rpush("foo", "youngest")
    r.hset("record_youngest", "age", 1)
    r.hset("record_youngest", "name", "baby")

    r.hset("record_middle", "age", 10)
    r.hset("record_middle", "name", "teen")

    r.hset("record_eldest", "age", 20)
    r.hset("record_eldest", "name", "adult")

    assert r.sort("foo", by="record_*->age") == [b"youngest", b"middle", b"eldest"]
    assert r.sort("foo", by="record_*->age", get="record_*->name") == [b"baby", b"teen", b"adult"]


def test_sort_with_set(r: redis.Redis):
    r.sadd("foo", "3")
    r.sadd("foo", "1")
    r.sadd("foo", "2")
    assert r.sort("foo") == [b"1", b"2", b"3"]


def test_ttl_should_return_minus_two_for_non_existent_key(r: redis.Redis):
    assert r.get("foo") is None
    assert r.ttl("foo") == -2


def test_type(r: redis.Redis):
    r.set("string_key", "value")
    r.lpush("list_key", "value")
    r.sadd("set_key", "value")
    r.zadd("zset_key", {"value": 1})
    r.hset("hset_key", "key", "value")

    assert r.type("string_key") == b"string"  # noqa: E721
    assert r.type("list_key") == b"list"  # noqa: E721
    assert r.type("set_key") == b"set"  # noqa: E721
    assert r.type("zset_key") == b"zset"  # noqa: E721
    assert r.type("hset_key") == b"hash"  # noqa: E721
    assert r.type("none_key") == b"none"  # noqa: E721


def test_unlink(r: redis.Redis):
    r.set("foo", "bar")
    r.unlink("foo")
    assert r.get("foo") is None


def test_dump_missing(r: redis.Redis):
    assert r.dump("foo") is None


def test_dump_restore(r: redis.Redis):
    r.set("foo", "bar")
    dump = r.dump("foo")
    r.restore("baz", 0, dump)
    assert r.get("baz") == b"bar"
    assert r.ttl("baz") == -1


def test_dump_restore_ttl(r: redis.Redis):
    r.set("foo", "bar")
    dump = r.dump("foo")
    r.restore("baz", 2000, dump)
    assert r.get("baz") == b"bar"
    assert 1000 <= r.pttl("baz") <= 2000


def test_dump_restore_replace(r: redis.Redis):
    r.set("foo", "bar")
    dump = r.dump("foo")
    r.set("foo", "baz")
    r.restore("foo", 0, dump, replace=True)
    assert r.get("foo") == b"bar"


def test_restore_exists(r: redis.Redis):
    r.set("foo", "bar")
    dump = r.dump("foo")
    with pytest.raises(redis.exceptions.ResponseError):
        r.restore("foo", 0, dump)


def test_restore_invalid_dump(r: redis.Redis):
    r.set("foo", "bar")
    dump = r.dump("foo")
    with pytest.raises(redis.exceptions.ResponseError):
        r.restore("baz", 0, dump[:-1])


def test_restore_invalid_ttl(r: redis.Redis):
    r.set("foo", "bar")
    dump = r.dump("foo")
    with pytest.raises(redis.exceptions.ResponseError):
        r.restore("baz", -1, dump)


def test_set_then_get(r: redis.Redis):
    assert r.set("foo", "bar") is True
    assert r.get("foo") == b"bar"


def test_exists(r: redis.Redis):
    assert "foo" not in r
    r.set("foo", "bar")
    assert "foo" in r
    with pytest.raises(redis.ResponseError, match=msgs.WRONG_ARGS_MSG6.format("exists")[4:]):
        raw_command(r, "exists")


@pytest.mark.slow
def test_expire_should_expire_key(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    r.expire("foo", 1)
    sleep(1.5)
    assert r.get("foo") is None
    assert r.expire("bar", 1) is False


def test_expire_should_throw_error(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    with pytest.raises(ResponseError):
        r.expire("foo", 1, nx=True, xx=True)
    with pytest.raises(ResponseError):
        r.expire("foo", 1, nx=True, gt=True)
    with pytest.raises(ResponseError):
        r.expire("foo", 1, nx=True, lt=True)
    with pytest.raises(ResponseError):
        r.expire("foo", 1, gt=True, lt=True)


@pytest.mark.max_server("7")
def test_expire_extra_params_return_error(r: redis.Redis):
    with pytest.raises(redis.exceptions.ResponseError):
        r.expire("foo", 1, nx=True)


def test_expire_should_return_true_for_existing_key(r: redis.Redis):
    r.set("foo", "bar")
    assert r.expire("foo", 1) is True


def test_expire_should_return_false_for_missing_key(r: redis.Redis):
    assert r.expire("missing", 1) is False


@pytest.mark.slow
def test_expire_should_expire_key_using_timedelta(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    r.expire("foo", timedelta(seconds=1))
    sleep(1.5)
    assert r.get("foo") is None
    assert r.expire("bar", 1) is False


@pytest.mark.slow
def test_expire_should_expire_immediately_with_millisecond_timedelta(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    r.expire("foo", timedelta(milliseconds=750))
    assert r.get("foo") is None
    assert r.expire("bar", 1) is False


def test_watch_expire(r: redis.Redis):
    """EXPIRE should mark a key as changed for WATCH."""
    r.set("foo", "bar")
    with r.pipeline() as p:
        p.watch("foo")
        r.expire("foo", 10000)
        p.multi()
        p.get("foo")
        with pytest.raises(redis.exceptions.WatchError):
            p.execute()


@pytest.mark.slow
def test_pexpire_should_expire_key(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    r.pexpire("foo", 150)
    sleep(0.2)
    assert r.get("foo") is None
    assert r.pexpire("bar", 1) == 0


def test_pexpire_should_return_truthy_for_existing_key(r: redis.Redis):
    r.set("foo", "bar")
    assert r.pexpire("foo", 1)


def test_pexpire_should_return_falsey_for_missing_key(r: redis.Redis):
    assert not r.pexpire("missing", 1)


@pytest.mark.slow
def test_pexpire_should_expire_key_using_timedelta(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    r.pexpire("foo", timedelta(milliseconds=750))
    sleep(0.5)
    assert r.get("foo") == b"bar"
    sleep(0.5)
    assert r.get("foo") is None
    assert r.pexpire("bar", 1) == 0


@pytest.mark.slow
def test_pexpireat_should_expire_key_by_datetime(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    r.pexpireat("foo", datetime.now() + timedelta(milliseconds=150))
    sleep(0.2)
    assert r.get("foo") is None
    assert r.pexpireat("bar", datetime.now()) == 0


@pytest.mark.slow
def test_pexpireat_should_expire_key_by_timestamp(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    r.pexpireat("foo", int(time() * 1000 + 150))
    sleep(0.2)
    assert r.get("foo") is None
    assert r.expire("bar", 1) is False


def test_pexpireat_should_return_true_for_existing_key(r: redis.Redis):
    r.set("foo", "bar")
    assert r.pexpireat("foo", int(time() * 1000 + 150))


def test_pexpireat_should_return_false_for_missing_key(r: redis.Redis):
    assert not r.pexpireat("missing", int(time() * 1000 + 150))


def test_pttl_should_return_minus_one_for_non_expiring_key(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    assert r.pttl("foo") == -1


def test_pttl_should_return_minus_two_for_non_existent_key(r: redis.Redis):
    assert r.get("foo") is None
    assert r.pttl("foo") == -2


def test_randomkey_returns_none_on_empty_db(r: redis.Redis):
    assert r.randomkey() is None


def test_randomkey_returns_existing_key(r: redis.Redis):
    r.set("foo", 1)
    r.set("bar", 2)
    r.set("baz", 3)
    assert r.randomkey().decode() in ("foo", "bar", "baz")


def test_persist(r: redis.Redis):
    r.set("foo", "bar", ex=20)
    assert r.persist("foo") == 1
    assert r.ttl("foo") == -1
    assert r.persist("foo") == 0


def test_watch_persist(r: redis.Redis):
    """PERSIST should mark a variable as changed."""
    r.set("foo", "bar", ex=10000)
    with r.pipeline() as p:
        p.watch("foo")
        r.persist("foo")
        p.multi()
        p.get("foo")
        with pytest.raises(redis.exceptions.WatchError):
            p.execute()


def test_set_existing_key_persists(r: redis.Redis):
    r.set("foo", "bar", ex=20)
    r.set("foo", "foo")
    assert r.ttl("foo") == -1


def test_set_non_str_keys(r: redis.Redis):
    assert r.set(2, "bar") is True
    assert r.get(2) == b"bar"
    assert r.get("2") == b"bar"


def test_getset_not_exist(r: redis.Redis):
    val = r.getset("foo", "bar")
    assert val is None
    assert r.get("foo") == b"bar"


def test_get_float_type(r: redis.Redis):  # Test for issue #58
    r.set("key", 123)
    assert r.get("key") == b"123"
    r.incr("key")
    assert r.get("key") == b"124"


def test_set_float_value(r: redis.Redis):
    x = 1.23456789123456789
    r.set("foo", x)
    assert float(r.get("foo")) == x


@pytest.mark.min_server("7")
def test_expire_should_not_expire__when_no_expire_is_set(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    assert r.expire("foo", 1, xx=True) == 0


@pytest.mark.min_server("7")
def test_expire_should_not_expire__when_expire_is_set(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    assert r.expire("foo", 1, nx=True) == 1
    assert r.expire("foo", 2, nx=True) == 0


@pytest.mark.min_server("7")
def test_expire_should_expire__when_expire_is_greater(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    assert r.expire("foo", 100) == 1
    assert r.get("foo") == b"bar"
    assert r.expire("foo", 200, gt=True) == 1


@pytest.mark.min_server("7")
def test_expire_should_expire__when_expire_is_lessthan(r: redis.Redis):
    r.set("foo", "bar")
    assert r.get("foo") == b"bar"
    assert r.expire("foo", 20) == 1
    assert r.expire("foo", 10, lt=True) == 1


def test_rename(r: redis.Redis):
    r.set("foo", "unique value")
    assert r.rename("foo", "bar")
    assert r.get("foo") is None
    assert r.get("bar") == b"unique value"


def test_rename_nonexistent_key(r: redis.Redis):
    with pytest.raises(redis.ResponseError):
        r.rename("foo", "bar")


def test_renamenx_doesnt_exist(r: redis.Redis):
    r.set("foo", "unique value")
    assert r.renamenx("foo", "bar")
    assert r.get("foo") is None
    assert r.get("bar") == b"unique value"


def test_rename_does_exist(r: redis.Redis):
    r.set("foo", "unique value")
    r.set("bar", "unique value2")
    assert not r.renamenx("foo", "bar")
    assert r.get("foo") == b"unique value"
    assert r.get("bar") == b"unique value2"


def test_rename_expiry(r: redis.Redis):
    r.set("foo", "value1", ex=10)
    r.set("bar", "value2")
    r.rename("foo", "bar")
    assert r.ttl("bar") > 0


def test_keys(r: redis.Redis):
    r.set("", "empty")
    r.set("abc\n", "")
    r.set("abc\\", "")
    r.set("abcde", "")
    r.set(b"\xfe\xcd", "")
    assert sorted(r.keys()) == [b"", b"abc\n", b"abc\\", b"abcde", b"\xfe\xcd"]
    assert r.keys("??") == [b"\xfe\xcd"]
    # empty pattern not the same as no pattern
    assert r.keys("") == [b""]
    # ? must match \n
    assert sorted(r.keys("abc?")) == [b"abc\n", b"abc\\"]
    # must be anchored at both ends
    assert r.keys("abc") == []
    assert r.keys("bcd") == []
    # wildcard test
    assert r.keys("a*de") == [b"abcde"]
    # positive groups
    assert sorted(r.keys("abc[d\n]*")) == [b"abc\n", b"abcde"]
    assert r.keys("abc[c-e]?") == [b"abcde"]
    assert r.keys("abc[e-c]?") == [b"abcde"]
    assert r.keys("abc[e-e]?") == []
    assert r.keys("abcd[ef") == [b"abcde"]
    assert r.keys("abcd[]") == []
    # negative groups
    assert r.keys("abc[^d\\\\]*") == [b"abc\n"]
    assert r.keys("abc[^]e") == [b"abcde"]
    # escaping
    assert r.keys(r"abc\?e") == []
    assert r.keys(r"abc\de") == [b"abcde"]
    assert r.keys(r"abc[\d]e") == [b"abcde"]
    # some escaping cases that redis handles strangely
    assert r.keys("abc\\") == [b"abc\\"]
    assert r.keys(r"abc[\c-e]e") == []
    assert r.keys(r"abc[c-\e]e") == []


def test_contains(r: redis.Redis):
    assert not r.exists("foo")
    r.set("foo", "bar")
    assert r.exists("foo")


def test_delete(r: redis.Redis):
    r["foo"] = "bar"
    assert r.delete("foo") == 1
    assert r.get("foo") is None


@pytest.mark.slow
def test_delete_expire(r: redis.Redis):
    r.set("foo", "bar", ex=1)
    r.delete("foo")
    r.set("foo", "bar")
    sleep(2)
    assert r.get("foo") == b"bar"


def test_delete_multiple(r: redis.Redis):
    r["one"] = "one"
    r["two"] = "two"
    r["three"] = "three"
    # Since redis>=2.7.6 returns number of deleted items.
    assert r.delete("one", "two") == 2
    assert r.get("one") is None
    assert r.get("two") is None
    assert r.get("three") == b"three"
    assert r.delete("one", "two") == 0
    # If any keys are deleted, True is returned.
    assert r.delete("two", "three", "three") == 1
    assert r.get("three") is None


def test_delete_nonexistent_key(r: redis.Redis):
    assert r.delete("foo") == 0


def test_basic_sort(r: redis.Redis):
    r.rpush("foo", "2")
    r.rpush("foo", "1")
    r.rpush("foo", "3")

    assert r.sort("foo") == [b"1", b"2", b"3"]
    assert raw_command(r, "sort", "foo", "asc") == [b"1", b"2", b"3"]


def test_key_patterns(r: redis.Redis):
    r.mset({"one": 1, "two": 2, "three": 3, "four": 4})
    assert sorted(r.keys("*o*")) == [b"four", b"one", b"two"]
    assert r.keys("t??") == [b"two"]
    assert sorted(r.keys("*")) == [b"four", b"one", b"three", b"two"]
    assert sorted(r.keys()) == [b"four", b"one", b"three", b"two"]


@pytest.mark.min_server("7")
def test_watch_when_setbit_does_not_change_value(r: redis.Redis):
    r.set("foo", b"0")

    with r.pipeline() as p:
        p.watch("foo")
        assert r.setbit("foo", 0, 0) == 0
        assert p.multi() is None
        assert p.execute() == []


def test_from_hypothesis_redis7(r: redis.Redis):
    r.set("foo", b"0")
    assert r.setbit("foo", 0, 0) == 0
    assert r.append("foo", b"") == 1

    r.set(b"", b"")
    assert r.setbit(b"", 0, 0) == 0
    assert r.get(b"") == b"\x00"
