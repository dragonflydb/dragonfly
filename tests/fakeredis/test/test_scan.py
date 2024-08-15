from time import sleep

import pytest
import redis

from test.testtools import key_val_dict


def test_sscan_delete_key_while_scanning_should_not_returns_it_in_scan(r: redis.Redis):
    size = 600
    name = "sscan-test"
    all_keys_set = {f"{i}".encode() for i in range(size)}
    r.sadd(name, *[k for k in all_keys_set])
    assert r.scard(name) == size

    cursor, keys = r.sscan(name, 0)
    assert len(keys) < len(all_keys_set)

    key_to_remove = next(x for x in all_keys_set if x not in keys)
    assert r.srem(name, key_to_remove) == 1
    assert not r.sismember(name, key_to_remove)
    while cursor != 0:
        cursor, data = r.sscan(name, cursor=cursor)
        keys.extend(data)
    assert len(set(keys)) == len(keys)
    assert len(keys) == size - 1
    assert key_to_remove not in keys


def test_hscan_delete_key_while_scanning_should_not_returns_it_in_scan(r: redis.Redis):
    size = 600
    name = "hscan-test"
    all_keys_dict = key_val_dict(size=size)
    r.hset(name, mapping=all_keys_dict)
    assert len(r.hgetall(name)) == size

    cursor, keys = r.hscan(name, 0)
    assert len(keys) < len(all_keys_dict)

    key_to_remove = next(x for x in all_keys_dict if x not in keys)
    assert r.hdel(name, key_to_remove) == 1
    assert r.hget(name, key_to_remove) is None
    while cursor != 0:
        cursor, data = r.hscan(name, cursor=cursor)
        keys.update(data)
    assert len(set(keys)) == len(keys)
    assert len(keys) == size - 1
    assert key_to_remove not in keys


def test_scan_delete_unseen_key_while_scanning_should_not_returns_it_in_scan(r: redis.Redis):
    size = 30
    all_keys_dict = key_val_dict(size=size)
    assert all(r.set(k, v) for k, v in all_keys_dict.items())
    assert len(r.keys()) == size

    cursor, keys = r.scan()

    key_to_remove = next(x for x in all_keys_dict if x not in keys)
    assert r.delete(key_to_remove) == 1
    assert r.get(key_to_remove) is None
    while cursor != 0:
        cursor, data = r.scan(cursor=cursor)
        keys.extend(data)
    assert len(set(keys)) == len(keys)
    assert len(keys) == size - 1
    assert key_to_remove not in keys


# @pytest.mark.xfail # todo
# def test_scan_delete_seen_key_while_scanning_should_return_all_keys(r: redis.Redis):
#     size = 30
#     all_keys_dict = key_val_dict(size=size)
#     assert all(r.set(k, v) for k, v in all_keys_dict.items())
#     assert len(r.keys()) == size
#
#     cursor, keys = r.scan()
#
#     key_to_remove = keys[0]
#     assert r.delete(keys[0]) == 1
#     assert r.get(key_to_remove) is None
#     while cursor != 0:
#         cursor, data = r.scan(cursor=cursor)
#         keys.extend(data)
#
#     assert len(set(keys)) == len(keys)
#     keys = set(keys)
#     assert len(keys) == size, f"{set(all_keys_dict).difference(keys)} is not empty but should be"
#     assert key_to_remove in keys


def test_scan_add_key_while_scanning_should_return_all_keys(r: redis.Redis):
    size = 30
    all_keys_dict = key_val_dict(size=size)
    assert all(r.set(k, v) for k, v in all_keys_dict.items())
    assert len(r.keys()) == size

    cursor, keys = r.scan()

    r.set("new_key", "new val")
    while cursor != 0:
        cursor, data = r.scan(cursor=cursor)
        keys.extend(data)

    keys = set(keys)
    assert len(keys) >= size, f"{set(all_keys_dict).difference(keys)} is not empty but should be"


def test_scan(r: redis.Redis):
    # Set up the data
    for ix in range(20):
        k = "scan-test:%s" % ix
        v = "result:%s" % ix
        r.set(k, v)
    expected = r.keys()
    assert len(expected) == 20  # Ensure we know what we're testing

    # Test that we page through the results and get everything out
    results = []
    cursor = "0"
    while cursor != 0:
        cursor, data = r.scan(cursor, count=6)
        results.extend(data)
    assert set(expected) == set(results)

    # Now test that the MATCH functionality works
    results = []
    cursor = "0"
    while cursor != 0:
        cursor, data = r.scan(cursor, match="*7", count=100)
        results.extend(data)
    assert b"scan-test:7" in results
    assert b"scan-test:17" in results
    assert len(set(results)) == 2

    # Test the match on iterator
    results = [r for r in r.scan_iter(match="*7")]
    assert b"scan-test:7" in results
    assert b"scan-test:17" in results
    assert len(set(results)) == 2


def test_scan_single(r: redis.Redis):
    r.set("foo1", "bar1")
    assert r.scan(match="foo*") == (0, [b"foo1"])


def test_scan_iter_single_page(r: redis.Redis):
    r.set("foo1", "bar1")
    r.set("foo2", "bar2")
    assert set(r.scan_iter(match="foo*")) == {b"foo1", b"foo2"}
    assert set(r.scan_iter()) == {b"foo1", b"foo2"}
    assert set(r.scan_iter(match="")) == set()
    assert set(r.scan_iter(match="foo1", _type="string")) == {
        b"foo1",
    }


def test_scan_iter_multiple_pages(r: redis.Redis):
    all_keys = key_val_dict(size=100)
    assert all(r.set(k, v) for k, v in all_keys.items())
    assert set(r.scan_iter()) == set(all_keys)


def test_scan_iter_multiple_pages_with_match(r: redis.Redis):
    all_keys = key_val_dict(size=100)
    assert all(r.set(k, v) for k, v in all_keys.items())
    # Now add a few keys that don't match the key:<number> pattern.
    r.set("otherkey", "foo")
    r.set("andanother", "bar")
    actual = set(r.scan_iter(match="key:*"))
    assert actual == set(all_keys)


def test_scan_multiple_pages_with_count_arg(r: redis.Redis):
    all_keys = key_val_dict(size=100)
    assert all(r.set(k, v) for k, v in all_keys.items())
    assert set(r.scan_iter(count=1000)) == set(all_keys)


def test_scan_all_in_single_call(r: redis.Redis):
    all_keys = key_val_dict(size=100)
    assert all(r.set(k, v) for k, v in all_keys.items())
    # Specify way more than the 100 keys we've added.
    actual = r.scan(count=1000)
    assert set(actual[1]) == set(all_keys)
    assert actual[0] == 0


@pytest.mark.slow
def test_scan_expired_key(r: redis.Redis):
    r.set("expiringkey", "value")
    r.pexpire("expiringkey", 1)
    sleep(1)
    assert r.scan()[1] == []


def test_scan_stream(r: redis.Redis):
    r.xadd("mystream", {"test": "value"})
    assert r.type("mystream") == b"stream"  # noqa: E721
    for s in r.scan_iter(_type="STRING"):
        print(s)
