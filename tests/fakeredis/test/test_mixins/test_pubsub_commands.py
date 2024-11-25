import threading
import time
import uuid
from queue import Queue
from time import sleep
from typing import Optional, Dict, Any

import pytest
import redis
from redis.client import PubSub

from .. import testtools


def wait_for_message(
    pubsub: PubSub, timeout=0.5, ignore_subscribe_messages=False
) -> Optional[Dict[str, Any]]:
    now = time.time()
    timeout = now + timeout
    while now < timeout:
        message = pubsub.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages
        )
        if message is not None:
            return message
        time.sleep(0.01)
        now = time.time()
    return None


def make_message(_type, channel, data, pattern=None):
    return {
        "type": _type,
        "pattern": pattern and pattern.encode("utf-8") or None,
        "channel": channel and channel.encode("utf-8") or None,
        "data": data.encode("utf-8") if isinstance(data, str) else data,
    }


def test_ping_pubsub(r: redis.Redis):
    p = r.pubsub()
    p.subscribe("channel")
    p.parse_response()  # Consume the subscribe command reply
    p.ping()
    assert p.parse_response() == [b"pong", b""]
    p.ping("test")
    assert p.parse_response() == [b"pong", b"test"]


@pytest.mark.slow
def test_pubsub_subscribe(r: redis.Redis):
    pubsub = r.pubsub()
    pubsub.subscribe("channel")
    sleep(1)
    expected_message = {
        "type": "subscribe",
        "pattern": None,
        "channel": b"channel",
        "data": 1,
    }
    message = pubsub.get_message()
    keys = list(pubsub.channels.keys())

    key = keys[0]
    key = key if type(key) is bytes else bytes(key, encoding="utf-8")

    assert len(keys) == 1
    assert key == b"channel"
    assert message == expected_message


@pytest.mark.slow
def test_pubsub_numpat(r: redis.Redis):
    p = r.pubsub()
    p.psubscribe("*oo", "*ar", "b*z")
    for i in range(3):
        assert wait_for_message(p)["type"] == "psubscribe"
    assert r.pubsub_numpat() == 3


@pytest.mark.slow
def test_pubsub_psubscribe(r: redis.Redis):
    pubsub = r.pubsub()
    pubsub.psubscribe("channel.*")
    sleep(1)
    expected_message = {
        "type": "psubscribe",
        "pattern": None,
        "channel": b"channel.*",
        "data": 1,
    }

    message = pubsub.get_message()
    keys = list(pubsub.patterns.keys())
    assert len(keys) == 1
    assert message == expected_message


@pytest.mark.slow
def test_pubsub_unsubscribe(r: redis.Redis):
    pubsub = r.pubsub()
    pubsub.subscribe("channel-1", "channel-2", "channel-3")
    sleep(1)
    expected_message = {
        "type": "unsubscribe",
        "pattern": None,
        "channel": b"channel-1",
        "data": 2,
    }
    pubsub.get_message()
    pubsub.get_message()
    pubsub.get_message()

    # unsubscribe from one
    pubsub.unsubscribe("channel-1")
    sleep(1)
    message = pubsub.get_message()
    keys = list(pubsub.channels.keys())
    assert message == expected_message
    assert len(keys) == 2

    # unsubscribe from multiple
    pubsub.unsubscribe()
    sleep(1)
    pubsub.get_message()
    pubsub.get_message()
    keys = list(pubsub.channels.keys())
    assert message == expected_message
    assert len(keys) == 0


@pytest.mark.slow
def test_pubsub_punsubscribe(r: redis.Redis):
    pubsub = r.pubsub()
    pubsub.psubscribe("channel-1.*", "channel-2.*", "channel-3.*")
    sleep(1)
    expected_message = {
        "type": "punsubscribe",
        "pattern": None,
        "channel": b"channel-1.*",
        "data": 2,
    }
    pubsub.get_message()
    pubsub.get_message()
    pubsub.get_message()

    # unsubscribe from one
    pubsub.punsubscribe("channel-1.*")
    sleep(1)
    message = pubsub.get_message()
    keys = list(pubsub.patterns.keys())
    assert message == expected_message
    assert len(keys) == 2

    # unsubscribe from multiple
    pubsub.punsubscribe()
    sleep(1)
    pubsub.get_message()
    pubsub.get_message()
    keys = list(pubsub.patterns.keys())
    assert len(keys) == 0


@pytest.mark.slow
def test_pubsub_listen(r: redis.Redis):
    def _listen(pubsub, q):
        count = 0
        for message in pubsub.listen():
            q.put(message)
            count += 1
            if count == 4:
                pubsub.close()

    channel = "ch1"
    patterns = ["ch1*", "ch[1]", "ch?"]
    pubsub = r.pubsub()
    pubsub.subscribe(channel)
    pubsub.psubscribe(*patterns)
    sleep(1)
    msgs = [pubsub.get_message() for _ in range(4)]
    assert msgs[0]["type"] == "subscribe"
    for i in range(1, 4):
        assert msgs[i]["type"] == "psubscribe"

    q = Queue()
    t = threading.Thread(target=_listen, args=(pubsub, q))
    t.start()
    msg = "hello world"
    r.publish(channel, msg)
    t.join()

    msgs = [q.get() for _ in range(4)]

    bpatterns = [pattern.encode() for pattern in patterns]
    bpatterns.append(channel.encode())
    msg = msg.encode()
    for item in msgs:
        assert item["data"] == msg
        assert item["channel"] in bpatterns


@pytest.mark.slow
def test_pubsub_listen_handler(r: redis.Redis):
    def _handler(message):
        calls.append(message)

    channel = "ch1"
    patterns = {"ch?": _handler}
    calls = []

    pubsub = r.pubsub()
    pubsub.subscribe(ch1=_handler)
    pubsub.psubscribe(**patterns)
    sleep(1)
    msg1 = pubsub.get_message()
    msg2 = pubsub.get_message()
    assert msg1["type"] == "subscribe"
    assert msg2["type"] == "psubscribe"
    msg = "hello world"
    r.publish(channel, msg)
    sleep(1)
    for i in range(2):
        msg = pubsub.get_message()
        assert msg is None  # get_message returns None when handler is used
    pubsub.close()
    calls.sort(key=lambda call: call["type"])
    assert calls == [
        {"pattern": None, "channel": b"ch1", "data": b"hello world", "type": "message"},
        {
            "pattern": b"ch?",
            "channel": b"ch1",
            "data": b"hello world",
            "type": "pmessage",
        },
    ]


@pytest.mark.slow
def test_pubsub_ignore_sub_messages_listen(r: redis.Redis):
    def _listen(pubsub, q):
        count = 0
        for message in pubsub.listen():
            q.put(message)
            count += 1
            if count == 4:
                pubsub.close()

    channel = "ch1"
    patterns = ["ch1*", "ch[1]", "ch?"]
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(channel)
    pubsub.psubscribe(*patterns)
    sleep(1)

    q = Queue()
    t = threading.Thread(target=_listen, args=(pubsub, q))
    t.start()
    msg = "hello world"
    r.publish(channel, msg)
    t.join()

    msg1 = q.get()
    msg2 = q.get()
    msg3 = q.get()
    msg4 = q.get()

    bpatterns = [pattern.encode() for pattern in patterns]
    bpatterns.append(channel.encode())
    msg = msg.encode()
    assert msg1["data"] == msg
    assert msg1["channel"] in bpatterns
    assert msg2["data"] == msg
    assert msg2["channel"] in bpatterns
    assert msg3["data"] == msg
    assert msg3["channel"] in bpatterns
    assert msg4["data"] == msg
    assert msg4["channel"] in bpatterns


@pytest.mark.slow
def test_pubsub_binary(r: redis.Redis):
    def _listen(pubsub, q):
        for message in pubsub.listen():
            q.put(message)
            pubsub.close()

    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe("channel\r\n\xff")
    sleep(1)

    q = Queue()
    t = threading.Thread(target=_listen, args=(pubsub, q))
    t.start()
    msg = b"\x00hello world\r\n\xff"
    r.publish("channel\r\n\xff", msg)
    t.join()

    received = q.get()
    assert received["data"] == msg


@pytest.mark.slow
def test_pubsub_run_in_thread(r: redis.Redis):
    q = Queue()

    pubsub = r.pubsub()
    pubsub.subscribe(channel=q.put)
    pubsub_thread = pubsub.run_in_thread()

    msg = b"Hello World"
    r.publish("channel", msg)

    retrieved = q.get()
    assert retrieved["data"] == msg

    pubsub_thread.stop()
    # Newer versions of redis wait for an unsubscribe message, which sometimes comes early
    # https://github.com/andymccurdy/redis-py/issues/1150
    if pubsub.channels:
        pubsub.channels = {}
    pubsub_thread.join()
    assert not pubsub_thread.is_alive()

    pubsub.subscribe(channel=None)
    with pytest.raises(redis.exceptions.PubSubError):
        pubsub_thread = pubsub.run_in_thread()

    pubsub.unsubscribe("channel")

    pubsub.psubscribe(channel=None)
    with pytest.raises(redis.exceptions.PubSubError):
        pubsub_thread = pubsub.run_in_thread()


@pytest.mark.slow
@pytest.mark.parametrize(
    "timeout_value",
    [1, pytest.param(None, marks=testtools.run_test_if_redispy_ver("gte", "3.2"))],
)
def test_pubsub_timeout(r, timeout_value):
    def publish():
        sleep(0.1)
        r.publish("channel", "hello")

    p = r.pubsub()
    p.subscribe("channel")
    p.parse_response()  # Drains the subscribe command message
    publish_thread = threading.Thread(target=publish)
    publish_thread.start()
    message = p.get_message(timeout=timeout_value)
    assert message == {
        "type": "message",
        "pattern": None,
        "channel": b"channel",
        "data": b"hello",
    }
    publish_thread.join()

    if timeout_value is not None:
        # For infinite timeout case don't wait for the message that will never appear.
        message = p.get_message(timeout=timeout_value)
        assert message is None


def test_pubsub_channels(r: redis.Redis):
    p = r.pubsub()
    p.subscribe("foo", "bar", "baz", "test")
    expected = {b"foo", b"bar", b"baz", b"test"}
    assert set(r.pubsub_channels()) == expected


def test_pubsub_channels_pattern(r: redis.Redis):
    p = r.pubsub()
    p.subscribe("foo", "bar", "baz", "test")
    assert set(r.pubsub_channels("b*")) == {
        b"bar",
        b"baz",
    }


def test_pubsub_no_subcommands(r: redis.Redis):
    with pytest.raises(redis.ResponseError):
        testtools.raw_command(r, "PUBSUB")


@pytest.mark.min_server("7")
@pytest.mark.max_server("7")
def test_pubsub_help_redis7(r: redis.Redis):
    assert testtools.raw_command(r, "PUBSUB HELP") == [
        b"PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        b"CHANNELS [<pattern>]",
        b"    Return the currently active channels matching a <pattern> (default: '*')"
        b".",
        b"NUMPAT",
        b"    Return number of subscriptions to patterns.",
        b"NUMSUB [<channel> ...]",
        b"    Return the number of subscribers for the specified channels, excluding",
        b"    pattern subscriptions(default: no channels).",
        b"SHARDCHANNELS [<pattern>]",
        b"    Return the currently active shard level channels matching a <pattern> (d"
        b"efault: '*').",
        b"SHARDNUMSUB [<shardchannel> ...]",
        b"    Return the number of subscribers for the specified shard level channel(s"
        b")",
        b"HELP",
        b"    Prints this help.",
    ]


@pytest.mark.min_server("7.1")
def test_pubsub_help_redis71(r: redis.Redis):
    assert testtools.raw_command(r, "PUBSUB HELP") == [
        b"PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        b"CHANNELS [<pattern>]",
        b"    Return the currently active channels matching a <pattern> (default: '*')"
        b".",
        b"NUMPAT",
        b"    Return number of subscriptions to patterns.",
        b"NUMSUB [<channel> ...]",
        b"    Return the number of subscribers for the specified channels, excluding",
        b"    pattern subscriptions(default: no channels).",
        b"SHARDCHANNELS [<pattern>]",
        b"    Return the currently active shard level channels matching a <pattern> (d"
        b"efault: '*').",
        b"SHARDNUMSUB [<shardchannel> ...]",
        b"    Return the number of subscribers for the specified shard level channel(s"
        b")",
        b"HELP",
        b"    Print this help.",
    ]


def test_pubsub_numsub(r: redis.Redis):
    a = uuid.uuid4().hex
    b = uuid.uuid4().hex
    c = uuid.uuid4().hex
    p1 = r.pubsub()
    p2 = r.pubsub()

    p1.subscribe(a, b, c)
    p2.subscribe(a, b)

    assert r.pubsub_numsub(a, b, c) == [
        (a.encode(), 2),
        (b.encode(), 2),
        (c.encode(), 1),
    ]
    assert r.pubsub_numsub() == []
    assert r.pubsub_numsub(a, "non-existing") == [(a.encode(), 2), (b"non-existing", 0)]


@pytest.mark.min_server("7")
@testtools.run_test_if_redispy_ver("gte", "5.0.0rc2")
@pytest.mark.unsupported_server_types("dragonfly")
def test_published_message_to_shard_channel(r: redis.Redis):
    p = r.pubsub()
    p.ssubscribe("foo")
    assert wait_for_message(p) == make_message("ssubscribe", "foo", 1)
    assert r.spublish("foo", "test message") == 1

    message = wait_for_message(p)
    assert isinstance(message, dict)
    assert message == make_message("smessage", "foo", "test message")


@pytest.mark.min_server("7")
@testtools.run_test_if_redispy_ver("gte", "5.0.0")
@pytest.mark.unsupported_server_types("dragonfly")
def test_subscribe_property_with_shard_channels_cluster(r: redis.Redis):
    p = r.pubsub()
    keys = ["foo", "bar", "uni" + chr(4456) + "code"]
    assert p.subscribed is False
    p.ssubscribe(keys[0])
    # we're now subscribed even though we haven't processed the reply from the server just yet
    assert p.subscribed is True
    assert wait_for_message(p) == make_message("ssubscribe", keys[0], 1)
    # we're still subscribed
    assert p.subscribed is True

    # unsubscribe from all shard_channels
    p.sunsubscribe()
    # we're still technically subscribed until we process the response messages from the server
    assert p.subscribed is True
    assert wait_for_message(p) == make_message("sunsubscribe", keys[0], 0)
    # now we're no longer subscribed as no more messages can be delivered to any channels we were listening to
    assert p.subscribed is False

    # subscribing again flips the flag back
    p.ssubscribe(keys[0])
    assert p.subscribed is True
    assert wait_for_message(p) == make_message("ssubscribe", keys[0], 1)

    # unsubscribe again
    p.sunsubscribe()
    assert p.subscribed is True
    # subscribe to another shard_channel before reading the unsubscribe response
    p.ssubscribe(keys[1])
    assert p.subscribed is True
    # read the unsubscribe for key1
    assert wait_for_message(p) == make_message("sunsubscribe", keys[0], 0)
    # we're still subscribed to key2, so subscribed should still be True
    assert p.subscribed is True
    # read the key2 subscribe message
    assert wait_for_message(p) == make_message("ssubscribe", keys[1], 1)
    p.sunsubscribe()
    # haven't read the message yet, so we're still subscribed
    assert p.subscribed is True
    assert wait_for_message(p) == make_message("sunsubscribe", keys[1], 0)
    # now we're finally unsubscribed
    assert p.subscribed is False


@pytest.mark.min_server("7")
@testtools.run_test_if_redispy_ver("gte", "5.0.0")
@pytest.mark.unsupported_server_types("dragonfly")
def test_pubsub_shardnumsub(r: redis.Redis):
    channels = {b"foo", b"bar", b"baz"}
    p1 = r.pubsub()
    p1.ssubscribe(*channels)
    for node in channels:
        assert wait_for_message(p1)["type"] == "ssubscribe"
    p2 = r.pubsub()
    p2.ssubscribe("bar", "baz")
    for i in range(2):
        assert wait_for_message(p2)["type"] == "ssubscribe"
    p3 = r.pubsub()
    p3.ssubscribe("baz")
    assert wait_for_message(p3)["type"] == "ssubscribe"

    channels = [(b"foo", 1), (b"bar", 2), (b"baz", 3)]
    assert r.pubsub_shardnumsub("foo", "bar", "baz", target_nodes="all") == channels


@pytest.mark.min_server("7")
@testtools.run_test_if_redispy_ver("gte", "5.0.0rc2")
@pytest.mark.unsupported_server_types("dragonfly")
def test_pubsub_shardchannels(r: redis.Redis):
    p = r.pubsub()
    p.ssubscribe("foo", "bar", "baz", "quux")
    for i in range(4):
        assert wait_for_message(p)["type"] == "ssubscribe"
    expected = [b"bar", b"baz", b"foo", b"quux"]
    assert all([channel in r.pubsub_shardchannels() for channel in expected])
