import threading
import time
from typing import List

import pytest
import redis

from fakeredis import _msgs as msgs
from fakeredis._stream import XStream, StreamRangeTest
from test import testtools


def get_ids(results):
    return [result[0] for result in results]


def add_items(r: redis.Redis, stream: str, n: int):
    id_list = list()
    for i in range(n):
        id_list.append(r.xadd(stream, {"k": i}))
    return id_list


@pytest.mark.fake
def test_xstream():
    stream = XStream()
    stream.add([0, 0, 1, 1, 2, 2, 3, 3], "0-1")
    stream.add([1, 1, 2, 2, 3, 3, 4, 4], "1-2")
    stream.add([2, 2, 3, 3, 4, 4], "1-3")
    stream.add([3, 3, 4, 4], "2-1")
    stream.add([3, 3, 4, 4], "2-2")
    stream.add([3, 3, 4, 4], "3-1")
    assert stream.add([3, 3, 4, 4], "4-*") == b"4-0"
    assert stream.last_item_key() == b"4-0"
    assert stream.add([3, 3, 4, 4], "4-*-*") is None
    assert len(stream) == 7
    i = iter(stream)
    assert next(i) == [b"0-1", [0, 0, 1, 1, 2, 2, 3, 3]]
    assert next(i) == [b"1-2", [1, 1, 2, 2, 3, 3, 4, 4]]
    assert next(i) == [b"1-3", [2, 2, 3, 3, 4, 4]]
    assert next(i) == [b"2-1", [3, 3, 4, 4]]
    assert next(i) == [b"2-2", [3, 3, 4, 4]]

    assert stream.find_index_key_as_str("1-2") == (1, True)
    assert stream.find_index_key_as_str("0-1") == (0, True)
    assert stream.find_index_key_as_str("2-1") == (3, True)
    assert stream.find_index_key_as_str("1-4") == (3, False)

    lst = stream.irange(StreamRangeTest.decode(b"0-2"), StreamRangeTest.decode(b"3-0"))
    assert len(lst) == 4

    stream = XStream()
    assert stream.delete(["1"]) == 0
    entry_key: bytes = stream.add([0, 0, 1, 1, 2, 2, 3, 3])
    assert len(stream) == 1
    assert (
        stream.delete(
            [
                entry_key,
            ]
        )
        == 1
    )
    assert len(stream) == 0


def test_xadd_redis__green(r: redis.Redis):
    stream = "stream"
    before = int(1000 * time.time())
    m1 = r.xadd(stream, {"some": "other"})
    ts1, seq1 = m1.decode().split("-")
    after = int(1000 * time.time()) + 1
    assert before <= int(ts1) <= after
    seq1 = int(seq1)
    m2 = r.xadd(stream, {"add": "more"}, id=f"{ts1}-{seq1 + 1}")
    ts2, seq2 = m2.decode().split("-")
    assert ts1 == ts2
    assert int(seq2) == int(seq1) + 1

    stream = "stream2"
    m1 = r.xadd(stream, {"some": "other"})
    ts1, seq1 = m1.decode().split("-")
    ts1 = int(ts1) - 1
    with pytest.raises(redis.ResponseError):
        r.xadd(stream, {"add": "more"}, id=f"{ts1}-*")
    with pytest.raises(redis.ResponseError):
        r.xadd(stream, {"add": "more"}, id=f"{ts1}-1")


@pytest.mark.min_server("7")
def test_xadd_redis7(r: redis.Redis):  # Using ts-*
    stream = "stream"
    m1 = r.xadd(stream, {"some": "other"})
    ts1, seq1 = m1.decode().split("-")
    m2 = r.xadd(stream, {"add": "more"}, id=f"{ts1}-*")
    ts2, seq2 = m2.decode().split("-")
    ts1, seq1 = int(ts1), int(seq1)
    ts2, seq2 = int(ts2), int(seq2)
    assert ts2 == ts1
    assert seq2 == seq1 + 1


def test_xadd_maxlen(r: redis.Redis):
    stream = "stream"
    id_list = add_items(r, stream, 10)
    maxlen = 5
    id_list.append(r.xadd(stream, {"k": "new"}, maxlen=maxlen, approximate=False))
    assert r.xlen(stream) == maxlen
    results = r.xrange(stream, id_list[0])
    assert get_ids(results) == id_list[len(id_list) - maxlen :]
    with pytest.raises(redis.ResponseError):
        testtools.raw_command(r, "xadd", stream, "maxlen", "3", "minid", "sometestvalue", "field", "value")
    assert r.set("non-a-stream", 1) == 1
    with pytest.raises(redis.ResponseError):
        r.xlen("non-a-stream")


def test_xadd_minid(r: redis.Redis):
    stream = "stream"
    id_list = add_items(r, stream, 10)
    minid = id_list[6]
    id_list.append(r.xadd(stream, {"k": "new"}, minid=minid, approximate=False))
    assert r.xlen(stream) == len(id_list) - 6
    results = r.xrange(stream, id_list[0])
    assert get_ids(results) == id_list[6:]


def test_xtrim(r: redis.Redis):
    stream = "stream"

    # trimming an empty key doesn't do anything
    assert r.xtrim(stream, 1000) == 0
    add_items(r, stream, 4)

    # trimming an amount larger than the number of messages doesn't do anything
    assert r.xtrim(stream, 5, approximate=False) == 0

    # 1 message is trimmed
    assert r.xtrim(stream, 3, approximate=False) == 1


@pytest.mark.min_server("6.2.4")
def test_xtrim_minlen_and_length_args(r: redis.Redis):
    stream = "stream"
    add_items(r, stream, 4)

    # Future self: No limits without approximate, according to the api
    # with pytest.raises(redis.ResponseError):
    #     assert r.xtrim(stream, 3, approximate=False, limit=2)

    with pytest.raises(redis.DataError):
        assert r.xtrim(stream, maxlen=3, minid="sometestvalue")

    with pytest.raises(redis.ResponseError):
        testtools.raw_command(r, "xtrim", stream, "maxlen", "3", "minid", "sometestvalue")
    # minid with a limit
    stream = "s2"
    m1 = add_items(r, stream, 4)[0]
    assert r.xtrim(stream, minid=m1, limit=3) == 0

    # pure minid
    m4 = add_items(r, stream, 4)[-1]
    assert r.xtrim(stream, approximate=False, minid=m4) == 7

    # minid approximate
    r.xadd(stream, {"foo": "bar"})
    r.xadd(stream, {"foo": "bar"})
    m3 = r.xadd(stream, {"foo": "bar"})
    r.xadd(stream, {"foo": "bar"})
    assert r.xtrim(stream, approximate=False, minid=m3) == 3


def test_xadd_nomkstream(r: redis.Redis):
    r.xadd("stream2", {"some": "other"}, nomkstream=True)
    assert r.xlen("stream2") == 0
    # nomkstream option
    stream = "stream"
    r.xadd(stream, {"foo": "bar"})
    r.xadd(stream, {"some": "other"}, nomkstream=False)
    assert r.xlen(stream) == 2
    r.xadd(stream, {"some": "other"}, nomkstream=True)
    assert r.xlen(stream) == 3


def _add_to_stream(r: redis.Redis, stream_name: str, n: int):
    res = []
    for _ in range(n):
        res.append(r.xadd(stream_name, {"foo": "bar"}))
    return res


def test_xrevrange(r: redis.Redis):
    stream = "stream"
    m1, m2, m3, m4 = _add_to_stream(r, stream, 4)

    results = r.xrevrange(stream, max=m4)
    assert get_ids(results) == [m4, m3, m2, m1]

    results = r.xrevrange(stream, max=m3, min=m2)
    assert get_ids(results) == [m3, m2]

    results = r.xrevrange(stream, min=m3)
    assert get_ids(results) == [m4, m3]

    results = r.xrevrange(stream, min=m2, count=1)
    assert get_ids(results) == [m4]


def test_xrange(r: redis.Redis):
    m = r.xadd("stream1", {"foo": "bar"})
    assert r.xrange("stream1") == [
        (m, {b"foo": b"bar"}),
    ]

    stream = "stream2"
    m = testtools.raw_command(r, "xadd", stream, "*", b"field", b"value", b"foo", b"bar")

    assert r.xrevrange(stream) == [
        (m, {b"field": b"value", b"foo": b"bar"}),
    ]

    stream = "stream"
    m1, m2, m3, m4 = _add_to_stream(r, stream, 4)

    results = r.xrange(stream, min=m1)
    assert get_ids(results) == [m1, m2, m3, m4]

    results = r.xrange(stream, min=m2, max=m3)
    assert get_ids(results) == [m2, m3]

    results = r.xrange(stream, max=m3)
    assert get_ids(results) == [m1, m2, m3]

    results = r.xrange(stream, max=m2, count=1)
    assert get_ids(results) == [m1]


def get_stream_message(client, stream, message_id):
    """Fetch a stream message and format it as a (message_id, fields) pair"""
    response = client.xrange(stream, min=message_id, max=message_id)
    assert len(response) == 1
    return response[0]


def test_xread_multiple_streams_blocking(r: redis.Redis):
    stream1 = "stream1"
    stream2 = "stream2"
    m1 = r.xadd(stream1, {"foo": "bar"})
    m2 = r.xadd(stream2, {"bing": "baz"})

    res = r.xread(streams={stream1: 0, stream2: 0}, block=10)
    assert len(res) == 2


def test_xread_blocking_no_count(r: redis.Redis):
    k = "key"
    r.xadd(k, {"value": 1234})
    streams = {k: "0"}
    m1 = r.xread(streams=streams, block=10)
    assert m1[0][1][0][1] == {b"value": b"1234"}


def test_xread(r: redis.Redis):
    stream = "stream"
    m1 = r.xadd(stream, {"foo": "bar"})
    m2 = r.xadd(stream, {"bing": "baz"})

    expected = [
        [
            stream.encode(),
            [get_stream_message(r, stream, m1), get_stream_message(r, stream, m2)],
        ]
    ]
    # xread starting at 0 returns both messages
    assert r.xread(streams={stream: 0}) == expected

    expected = [[stream.encode(), [get_stream_message(r, stream, m1)]]]
    # xread starting at 0 and count=1 returns only the first message
    assert r.xread(streams={stream: 0}, count=1) == expected

    expected = [[stream.encode(), [get_stream_message(r, stream, m2)]]]
    # xread starting at m1 returns only the second message
    assert r.xread(streams={stream: m1}) == expected

    # xread starting at the last message returns an empty list
    assert r.xread(streams={stream: m2}) == []


def test_xread_count(r: redis.Redis):
    r.xadd("test", {"x": 1})
    result = r.xread(streams={"test": "0"}, count=100, block=10)
    assert result[0][0] == b"test"
    assert result[0][1][0][1] == {b"x": b"1"}


def test_xread_bad_commands(r: redis.Redis):
    with pytest.raises(redis.ResponseError) as exc_info:
        testtools.raw_command(r, "xread", "foo", "11-1")
    print(exc_info)
    with pytest.raises(redis.ResponseError) as ex2:
        testtools.raw_command(
            r,
            "xread",
            "streams",
            "foo",
        )
    print(ex2)


def test_xdel(r: redis.Redis):
    stream = "stream"

    # deleting from an empty stream doesn't do anything
    assert r.xdel(stream, 1) == 0

    m1 = r.xadd(stream, {"foo": "bar"})
    m2 = r.xadd(stream, {"foo": "bar"})
    m3 = r.xadd(stream, {"foo": "bar"})

    # xdel returns the number of deleted elements
    assert r.xdel(stream, m1) == 1
    assert r.xdel(stream, m2, m3) == 2

    with pytest.raises(redis.ResponseError) as ex:
        testtools.raw_command(r, "XDEL", stream)
    assert ex.value.args[0] == msgs.WRONG_ARGS_MSG6.format("xdel")[4:]
    assert r.xdel("non-existing-key", "1-1") == 0


def test_xgroup_destroy(r: redis.Redis):
    stream = "stream"
    group = "group"
    r.xadd(stream, {"foo": "bar"})

    assert r.xgroup_destroy(stream, group) == 0

    r.xgroup_create(stream, group, 0)
    assert r.xgroup_destroy(stream, group) == 1


@pytest.mark.max_server("6.3")
def test_xgroup_create_redis6(r: redis.Redis):
    stream, group = "stream", "group"
    message_id = r.xadd(stream, {"foo": "bar"})
    r.xgroup_create(stream, group, message_id)
    r.xadd(stream, {"foo": "bar"})
    res = r.xinfo_groups(stream)
    assert len(res) == 1
    assert res[0]["name"] == group.encode()
    assert res[0]["consumers"] == 0
    assert res[0]["pending"] == 0
    assert res[0]["last-delivered-id"] == message_id


@pytest.mark.min_server("7")
def test_xgroup_create_redis7(r: redis.Redis):
    stream, group = "stream", "group"
    message_id = r.xadd(stream, {"foo": "bar"})
    r.xgroup_create(stream, group, message_id)
    r.xadd(stream, {"foo": "bar"})
    expected = [
        {
            "name": group.encode(),
            "consumers": 0,
            "pending": 0,
            "last-delivered-id": message_id,
            "entries-read": None,
            "lag": 1,
        }
    ]
    assert r.xinfo_groups(stream) == expected


@pytest.mark.min_server("7")
def test_xgroup_setid_redis7(r: redis.Redis):
    stream, group = "stream", "group"
    message_id = r.xadd(stream, {"foo": "bar"})

    r.xgroup_create(stream, group, 0)
    # advance the last_delivered_id to the message_id
    r.xgroup_setid(stream, group, message_id, entries_read=2)
    expected = [
        {
            "name": group.encode(),
            "consumers": 0,
            "pending": 0,
            "last-delivered-id": message_id,
            "entries-read": 2,
            "lag": -1,
        }
    ]
    assert r.xinfo_groups(stream) == expected


def test_xgroup_delconsumer(r: redis.Redis):
    stream, group, consumer = "stream", "group", "consumer"
    r.xadd(stream, {"foo": "bar"})
    r.xadd(stream, {"foo": "bar"})
    r.xgroup_create(stream, group, 0)

    # a consumer that hasn't yet read any messages doesn't do anything
    assert r.xgroup_delconsumer(stream, group, consumer) == 0

    # read all messages from the group
    r.xreadgroup(group, consumer, streams={stream: ">"})

    # deleting the consumer should return 2 pending messages
    assert r.xgroup_delconsumer(stream, group, consumer) == 2


def test_xgroup_createconsumer(r: redis.Redis):
    stream, group, consumer = "stream", "group", "consumer"
    r.xadd(stream, {"foo": "bar"})
    r.xadd(stream, {"foo": "bar"})
    r.xgroup_create(stream, group, 0)
    assert r.xgroup_createconsumer(stream, group, consumer) == 1
    # Adding consumer with existing consumer name does nothing
    assert r.xgroup_createconsumer(stream, group, consumer) == 0

    # read all messages from the group
    r.xreadgroup(group, consumer, streams={stream: ">"})

    # deleting the consumer should return 2 pending messages
    assert r.xgroup_delconsumer(stream, group, consumer) == 2


def test_xinfo_consumers(r: redis.Redis):
    stream, group, consumer1, consumer2 = "stream", "group", "consumer1", "consumer2"
    r.xadd(stream, {"foo": "bar"})
    r.xadd(stream, {"foo": "bar"})
    r.xadd(stream, {"foo": "bar"})

    r.xgroup_create(stream, group, 0)
    r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
    r.xreadgroup(group, consumer2, streams={stream: ">"})
    info = r.xinfo_consumers(stream, group)
    assert len(info) == 2
    expected = [
        {"name": consumer1.encode(), "pending": 1},
        {"name": consumer2.encode(), "pending": 2},
    ]

    # we can't determine the idle/inactive time, so just make sure it's an int
    assert isinstance(info[0].pop("idle"), int)
    assert isinstance(info[1].pop("idle"), int)
    assert isinstance(info[0].pop("inactive", 0), int)
    assert isinstance(info[1].pop("inactive", 0), int)
    assert info == expected


def test_xreadgroup(r: redis.Redis):
    stream, group, consumer = "stream", "group", "consumer1"
    with pytest.raises(redis.exceptions.ResponseError):
        r.xreadgroup(group, consumer, streams={stream: ">"})
    c1 = {b"foo": b"bar"}
    c2 = {b"bing": b"baz"}
    m1 = r.xadd(stream, c1)
    m2 = r.xadd(stream, c2)
    with pytest.raises(
        redis.exceptions.ResponseError, match=msgs.XREADGROUP_KEY_OR_GROUP_NOT_FOUND_MSG.format(stream, group)
    ):
        r.xreadgroup(group, consumer, streams={stream: ">"})
    r.xgroup_create(stream, group, 0)

    expected = [
        [
            stream.encode(),
            [get_stream_message(r, stream, m1), get_stream_message(r, stream, m2)],
        ]
    ]
    # xread starting at 0 returns both messages
    assert r.xreadgroup(group, consumer, streams={stream: ">"}) == expected

    r.xgroup_destroy(stream, group)
    r.xgroup_create(stream, group, 0)

    expected = [[stream.encode(), [get_stream_message(r, stream, m1)]]]
    # xread with count=1 returns only the first message
    assert r.xreadgroup(group, consumer, streams={stream: ">"}, count=1) == expected

    r.xgroup_destroy(stream, group)

    # create the group using $ as the last id meaning subsequent reads
    # will only find messages added after this
    r.xgroup_create(stream, group, "$")

    expected = []
    # xread starting after the last message returns an empty message list
    assert r.xreadgroup(group, consumer, streams={stream: ">"}) == expected

    # xreadgroup with noack does not have any items in the PEL
    r.xgroup_destroy(stream, group)
    r.xgroup_create(stream, group, "0")
    assert len(r.xreadgroup(group, consumer, streams={stream: ">"}, noack=True)[0][1]) == 2
    # now there should be nothing pending
    res = r.xreadgroup(group, consumer, streams={stream: "0"})
    assert len(res[0][1]) == 0

    r.xgroup_destroy(stream, group)
    r.xgroup_create(stream, group, "0")

    assert r.xreadgroup(group, consumer, streams={stream: ">"}) == [[stream.encode(), [(m1, c1), (m2, c2)]]]
    # delete all the messages in the stream
    assert r.xtrim(stream, 0) == 2
    # TODO groups keep ids of deleted messages
    # expected = [[stream.encode(), [(m1, {}), (m2, {})]]]
    # assert r.xreadgroup(group, consumer, streams={stream: "0"}) == expected
    r.xreadgroup(group, consumer, streams={stream: ">"}, count=10, block=500)


def test_xinfo_stream(r: redis.Redis):
    stream = "stream"
    m1 = r.xadd(stream, {"foo": "bar"})
    m2 = r.xadd(stream, {"foo": "bar"})
    info = r.xinfo_stream(stream)

    assert info["length"] == 2
    assert info["first-entry"] == get_stream_message(r, stream, m1)
    assert info["last-entry"] == get_stream_message(r, stream, m2)


def assert_consumer_info(r: redis.Redis, stream: str, group: str, equal_keys: List) -> List:
    res = r.xinfo_consumers(stream, group)
    assert len(res) == len(equal_keys)
    for i in range(len(equal_keys)):
        for k in res[i]:
            if k in equal_keys[i]:
                assert res[i][k] == equal_keys[i][k], f"res[{i}][{k}] mismatch, {res}!={equal_keys}"
            else:
                print(f"res[{i}][{k}]={res[i][k]}")
    return res


def test_xack(r: redis.Redis):
    stream, group, consumer = "stream", "group", "consumer"
    # xack on a stream that doesn't exist
    assert r.xack(stream, group, "0-0") == 0

    m1 = r.xadd(stream, {"one": "one"})
    m2 = r.xadd(stream, {"two": "two"})
    m3 = r.xadd(stream, {"three": "three"})

    # xack on a group that doesn't exist
    assert r.xack(stream, group, m1) == 0

    r.xgroup_create(stream, group, 0)
    r.xreadgroup(group, consumer, streams={stream: ">"})
    assert_consumer_info(r, stream, group, [{"name": b"consumer", "pending": 3}])
    assert r.xack(stream, group, m1) == 1
    time.sleep(0.01)
    res = assert_consumer_info(r, stream, group, [{"name": b"consumer", "pending": 2}])
    assert "idle" in res[0] and res[0]["idle"] > 0
    assert r.xack(stream, group, m2, m3) == 2
    assert_consumer_info(r, stream, group, [{"name": b"consumer", "pending": 0}])


@pytest.mark.min_server("7")
def test_xinfo_stream_redis7(r: redis.Redis):
    stream = "stream"
    m1 = r.xadd(stream, {"foo": "bar"})
    m2 = r.xadd(stream, {"foo": "bar"})
    info = r.xinfo_stream(stream)

    assert info["length"] == 2
    assert info["first-entry"] == get_stream_message(r, stream, m1)
    assert info["last-entry"] == get_stream_message(r, stream, m2)
    assert info["max-deleted-entry-id"] == b"0-0"
    assert info["entries-added"] == 2
    assert info["recorded-first-entry-id"] == m1

    r.xtrim(stream, 0)
    # Info about empty stream
    info = r.xinfo_stream(stream)

    assert info["length"] == 0
    assert info["first-entry"] is None
    assert info["last-entry"] is None
    assert info["max-deleted-entry-id"] == b"0-0"
    assert info["entries-added"] == 2
    assert info["recorded-first-entry-id"] == b"0-0"

    with pytest.raises(redis.exceptions.ResponseError):
        r.xinfo_stream("non-existing-key")


def test_xinfo_stream_full(r: redis.Redis):
    stream, group = "stream", "group"
    m1 = r.xadd(stream, {"foo": "bar"})
    r.xgroup_create(stream, group, 0)
    info = r.xinfo_stream(stream, full=True)

    assert info["length"] == 1
    assert m1 in info["entries"]
    assert len(info["groups"]) == 1


def test_xpending(r: redis.Redis):
    stream, group, consumer1, consumer2 = "stream", "group", "consumer1", "consumer2"
    m1 = r.xadd(stream, {"foo": "bar"})
    m2 = r.xadd(stream, {"foo": "bar"})
    r.xgroup_create(stream, group, 0)

    # xpending on a group that has no consumers yet
    expected = {"pending": 0, "min": None, "max": None, "consumers": []}
    assert r.xpending(stream, group) == expected

    # read 1 message from the group with each consumer
    r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
    r.xreadgroup(group, consumer2, streams={stream: ">"}, count=1)

    expected = {
        "pending": 2,
        "min": m1,
        "max": m2,
        "consumers": [
            {"name": consumer1.encode(), "pending": 1},
            {"name": consumer2.encode(), "pending": 1},
        ],
    }
    assert r.xpending(stream, group) == expected


def test_xpending_range(r: redis.Redis):
    stream, group, consumer1, consumer2 = "stream", "group", "consumer1", "consumer2"
    m1 = r.xadd(stream, {"foo": "bar"})
    m2 = r.xadd(stream, {"foo": "bar"})
    r.xgroup_create(stream, group, 0)

    # xpending range on a group that has no consumers yet
    assert r.xpending_range(stream, group, min="-", max="+", count=5) == []

    # read 1 message from the group with each consumer
    r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
    r.xreadgroup(group, consumer2, streams={stream: ">"}, count=1)

    response = r.xpending_range(stream, group, min="-", max="+", count=5)
    assert len(response) == 2
    assert response[0]["message_id"] == m1
    assert response[0]["consumer"] == consumer1.encode()
    assert response[1]["message_id"] == m2
    assert response[1]["consumer"] == consumer2.encode()

    # test with consumer name
    response = r.xpending_range(stream, group, min="-", max="+", count=5, consumername=consumer1)
    assert response[0]["message_id"] == m1
    assert response[0]["consumer"] == consumer1.encode()


def test_xpending_range_idle(r: redis.Redis):
    stream, group, consumer1, consumer2 = "stream", "group", "consumer1", "consumer2"
    r.xadd(stream, {"foo": "bar"})
    r.xadd(stream, {"foo": "bar"})
    r.xgroup_create(stream, group, 0)

    # read 1 message from the group with each consumer
    r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
    r.xreadgroup(group, consumer2, streams={stream: ">"}, count=1)

    response = r.xpending_range(stream, group, min="-", max="+", count=5)
    assert len(response) == 2
    response = r.xpending_range(stream, group, min="-", max="+", count=5, idle=1000)
    assert len(response) == 0


def test_xpending_range_negative(r: redis.Redis):
    stream, group = "stream", "group"
    with pytest.raises(redis.DataError):
        r.xpending_range(stream, group, min="-", max="+", count=None)
    with pytest.raises(ValueError):
        r.xpending_range(stream, group, min="-", max="+", count="one")
    with pytest.raises(redis.DataError):
        r.xpending_range(stream, group, min="-", max="+", count=-1)
    with pytest.raises(ValueError):
        r.xpending_range(stream, group, min="-", max="+", count=5, idle="one")
    with pytest.raises(redis.exceptions.ResponseError):
        r.xpending_range(stream, group, min="-", max="+", count=5, idle=1.5)
    with pytest.raises(redis.DataError):
        r.xpending_range(stream, group, min="-", max="+", count=5, idle=-1)
    with pytest.raises(redis.DataError):
        r.xpending_range(stream, group, min=None, max=None, count=None, idle=0)
    with pytest.raises(redis.DataError):
        r.xpending_range(stream, group, min=None, max=None, count=None, consumername=0)


@pytest.mark.max_server("6.3")
@testtools.run_test_if_redispy_ver("gte", "4.4")
def test_xautoclaim_redis6(r: redis.Redis):
    stream, group, consumer1, consumer2 = "stream", "group", "consumer1", "consumer2"

    message_id1 = r.xadd(stream, {"john": "wick"})
    message_id2 = r.xadd(stream, {"johny": "deff"})
    message = get_stream_message(r, stream, message_id1)
    r.xgroup_create(stream, group, 0)

    # trying to claim a message that isn't already pending doesn't
    # do anything
    assert r.xautoclaim(stream, group, consumer2, min_idle_time=0) == [b"0-0", []]

    # read the group as consumer1 to initially claim the messages
    r.xreadgroup(group, consumer1, streams={stream: ">"})

    # claim one message as consumer2
    response = r.xautoclaim(stream, group, consumer2, min_idle_time=0, count=1)
    assert response[1] == [message]

    # reclaim the messages as consumer1, but use the justid argument
    # which only returns message ids
    assert r.xautoclaim(stream, group, consumer1, min_idle_time=0, start_id=0, justid=True) == [
        message_id1,
        message_id2,
    ]
    assert r.xautoclaim(stream, group, consumer1, min_idle_time=0, start_id=message_id2, justid=True) == [message_id2]


@pytest.mark.min_server("7")
@testtools.run_test_if_redispy_ver("gte", "4.4")
def test_xautoclaim_redis7(r: redis.Redis):
    stream, group, consumer1, consumer2 = "stream", "group", "consumer1", "consumer2"

    message_id1 = r.xadd(stream, {"john": "wick"})
    message_id2 = r.xadd(stream, {"johny": "deff"})
    message = get_stream_message(r, stream, message_id1)
    r.xgroup_create(stream, group, 0)

    # trying to claim a message that isn't already pending doesn't
    # do anything
    assert r.xautoclaim(stream, group, consumer2, min_idle_time=0) == [b"0-0", [], []]

    # read the group as consumer1 to initially claim the messages
    r.xreadgroup(group, consumer1, streams={stream: ">"})

    # claim one message as consumer2
    response = r.xautoclaim(stream, group, consumer2, min_idle_time=0, count=1)
    assert response[1] == [message]

    # reclaim the messages as consumer1, but use the justid argument
    # which only returns message ids
    assert r.xautoclaim(stream, group, consumer1, min_idle_time=0, start_id=0, justid=True) == [
        message_id1,
        message_id2,
    ]
    assert r.xautoclaim(stream, group, consumer1, min_idle_time=0, start_id=message_id2, justid=True) == [message_id2]


@pytest.mark.min_server("7")
def test_xclaim_trimmed_redis7(r: redis.Redis):
    # xclaim should not raise an exception if the item is not there
    stream, group = "stream", "group"

    r.xgroup_create(stream, group, id="$", mkstream=True)

    # add a couple of new items
    sid1 = r.xadd(stream, {"item": 0})
    sid2 = r.xadd(stream, {"item": 0})

    # read them from consumer1
    r.xreadgroup(group, "consumer1", {stream: ">"})

    # add a 3rd and trim the stream down to 2 items
    r.xadd(stream, {"item": 3}, maxlen=2, approximate=False)

    # xclaim them from consumer2
    # the item that is still in the stream should be returned
    item = r.xclaim(stream, group, "consumer2", 0, [sid1, sid2])
    assert len(item) == 1
    assert item[0][0] == sid2


def test_xclaim(r: redis.Redis):
    stream, group, consumer1, consumer2 = "stream", "group", "consumer1", "consumer2"

    message_id = r.xadd(stream, {"john": "wick"})
    message = get_stream_message(r, stream, message_id)
    r.xgroup_create(stream, group, 0)

    # trying to claim a message that isn't already pending doesn't
    # do anything
    assert r.xclaim(stream, group, consumer2, min_idle_time=0, message_ids=(message_id,)) == []

    # read the group as consumer1 to initially claim the messages
    r.xreadgroup(group, consumer1, streams={stream: ">"})

    # claim the message as consumer2
    assert r.xclaim(stream, group, consumer2, min_idle_time=0, message_ids=(message_id,)) == [
        message,
    ]

    # reclaim the message as consumer1, but use the justid argument
    # which only returns message ids
    assert r.xclaim(
        stream,
        group,
        consumer1,
        min_idle_time=0,
        message_ids=(message_id,),
        justid=True,
    ) == [
        message_id,
    ]


def test_xread_blocking(create_redis):
    # thread with xread block 0 should hang
    # putting data in the stream should unblock it
    event = threading.Event()
    event.clear()

    def thread_func():
        while not event.is_set():
            time.sleep(0.1)
        r = create_redis(db=1)
        r.xadd("stream", {"x": "1"})
        time.sleep(0.1)

    t = threading.Thread(target=thread_func)
    t.start()
    r1 = create_redis(db=1)
    event.set()
    result = r1.xread({"stream": "$"}, block=0, count=1)
    event.clear()
    t.join()
    assert result[0][0] == b"stream"
    assert result[0][1][0][1] == {b"x": b"1"}


def test_stream_ttl(r: redis.Redis):
    stream = "stream"

    m1 = r.xadd(stream, {"foo": "bar"})
    expected = [
        [
            stream.encode(),
            [get_stream_message(r, stream, m1)],
        ]
    ]
    assert r.xread(streams={stream: 0}) == expected
    assert r.xtrim(stream, 0) == 1
    assert r.ttl(stream) == -1
