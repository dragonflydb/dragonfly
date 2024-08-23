from __future__ import annotations

import fakeredis
import pytest
import redis
import redis.client

from . import testtools


def test_multiple_successful_watch_calls(r: redis.Redis):
    p = r.pipeline()
    p.watch("bam")
    p.multi()
    p.set("foo", "bar")
    # Check that the watched keys buffer has been emptied.
    p.execute()

    # bam is no longer being watched, so it's ok to modify
    # it now.
    p.watch("foo")
    r.set("bam", "boo")
    p.multi()
    p.set("foo", "bats")
    assert p.execute() == [True]


def test_watch_state_is_cleared_after_abort(r: redis.Redis):
    # redis-py's pipeline handling and connection pooling interferes with this
    # test, so raw commands are used instead.
    testtools.raw_command(r, "watch", "foo")
    testtools.raw_command(r, "multi")
    with pytest.raises(redis.ResponseError):
        testtools.raw_command(r, "mget")  # Wrong number of arguments
    with pytest.raises(redis.exceptions.ExecAbortError):
        testtools.raw_command(r, "exec")

    testtools.raw_command(
        r, "set", "foo", "bar"
    )  # Should NOT trigger the watch from earlier
    testtools.raw_command(r, "multi")
    testtools.raw_command(r, "set", "abc", "done")
    testtools.raw_command(r, "exec")

    assert r.get("abc") == b"done"


def test_pipeline_transaction_shortcut(r: redis.Redis):
    # This example taken pretty much from the redis-py documentation.
    r.set("OUR-SEQUENCE-KEY", 13)
    calls = []

    def client_side_incr(pipe):
        calls.append((pipe,))
        current_value = pipe.get("OUR-SEQUENCE-KEY")
        next_value = int(current_value) + 1

        if len(calls) < 3:
            # Simulate a change from another thread.
            r.set("OUR-SEQUENCE-KEY", next_value)

        pipe.multi()
        pipe.set("OUR-SEQUENCE-KEY", next_value)

    res = r.transaction(client_side_incr, "OUR-SEQUENCE-KEY")

    assert res == [True]
    assert int(r.get("OUR-SEQUENCE-KEY")) == 16
    assert len(calls) == 3


def test_pipeline_transaction_value_from_callable(r: redis.Redis):
    def callback(pipe):
        # No need to do anything here since we only want the return value
        return "OUR-RETURN-VALUE"

    res = r.transaction(callback, "OUR-SEQUENCE-KEY", value_from_callable=True)
    assert res == "OUR-RETURN-VALUE"


def test_pipeline_empty(r: redis.Redis):
    p = r.pipeline()
    assert len(p) == 0


def test_pipeline_length(r: redis.Redis):
    p = r.pipeline()
    p.set("baz", "quux").get("baz")
    assert len(p) == 2


def test_pipeline_no_commands(r: redis.Redis):
    # Prior to 3.4, redis-py's execute is a nop if there are no commands
    # queued, so it succeeds even if watched keys have been changed.
    r.set("foo", "1")
    p = r.pipeline()
    p.watch("foo")
    r.set("foo", "2")
    with pytest.raises(redis.WatchError):
        p.execute()


def test_pipeline_failed_transaction(r: redis.Redis):
    p = r.pipeline()
    p.multi()
    p.set("foo", "bar")
    # Deliberately induce a syntax error
    p.execute_command("set")
    # It should be an ExecAbortError, but redis-py tries to DISCARD after the
    # failed EXEC, which raises a ResponseError.
    with pytest.raises(redis.ResponseError):
        p.execute()
    assert not r.exists("foo")


def test_pipeline_srem_no_change(r: redis.Redis):
    # A regression test for a case picked up by hypothesis tests.
    p = r.pipeline()
    p.watch("foo")
    r.srem("foo", "bar")
    p.multi()
    p.set("foo", "baz")
    p.execute()
    assert r.get("foo") == b"baz"


# The behaviour changed in redis 6.0 (see https://github.com/redis/redis/issues/6594).
@pytest.mark.min_server("6.0")
def test_pipeline_move(r: redis.Redis):
    # A regression test for a case picked up by hypothesis tests.
    r.set("foo", "bar")
    p = r.pipeline()
    p.watch("foo")
    r.move("foo", 1)
    # Ensure the transaction isn't empty, which had different behaviour in
    # older versions of redis-py.
    p.multi()
    p.set("bar", "baz")
    with pytest.raises(redis.exceptions.WatchError):
        p.execute()


@pytest.mark.min_server("6.0.6")
def test_exec_bad_arguments(r: redis.Redis):
    # Redis 6.0.6 changed the behaviour of exec so that it always fails with
    # EXECABORT, even when it's just bad syntax.
    with pytest.raises(redis.exceptions.ExecAbortError):
        r.execute_command("exec", "blahblah")


@pytest.mark.min_server("6.0.6")
def test_exec_bad_arguments_abort(r: redis.Redis):
    r.execute_command("multi")
    with pytest.raises(redis.exceptions.ExecAbortError):
        r.execute_command("exec", "blahblah")
    # Should have aborted the transaction, so we can run another one
    p = r.pipeline()
    p.multi()
    p.set("bar", "baz")
    p.execute()
    assert r.get("bar") == b"baz"


def test_pipeline(r: redis.Redis):
    # The pipeline method returns an object for
    # issuing multiple commands in a batch.
    p = r.pipeline()
    p.watch("bam")
    p.multi()
    p.set("foo", "bar").get("foo")
    p.lpush("baz", "quux")
    p.lpush("baz", "quux2").lrange("baz", 0, -1)
    res = p.execute()

    # Check return values returned as list.
    assert res == [True, b"bar", 1, 2, [b"quux2", b"quux"]]

    # Check side effects happened as expected.
    assert r.lrange("baz", 0, -1) == [b"quux2", b"quux"]

    # Check that the command buffer has been emptied.
    assert p.execute() == []


def test_pipeline_ignore_errors(r: redis.Redis):
    """Test the pipeline ignoring errors when asked."""
    with r.pipeline() as p:
        p.set("foo", "bar")
        p.rename("baz", "bats")
        with pytest.raises(redis.exceptions.ResponseError):
            p.execute()
        assert [] == p.execute()
    with r.pipeline() as p:
        p.set("foo", "bar")
        p.rename("baz", "bats")
        res = p.execute(raise_on_error=False)

        assert [] == p.execute()

        assert len(res) == 2
        assert isinstance(res[1], redis.exceptions.ResponseError)


def test_pipeline_non_transactional(r: redis.Redis):
    # For our simple-minded model I don't think
    # there is any observable difference.
    p = r.pipeline(transaction=False)
    res = p.set("baz", "quux").get("baz").execute()

    assert res == [True, b"quux"]


def test_pipeline_raises_when_watched_key_changed(r: redis.Redis):
    r.set("foo", "bar")
    r.rpush("greet", "hello")
    p = r.pipeline()
    try:
        p.watch("greet", "foo")
        nextf = bytes(p.get("foo")) + b"baz"
        # Simulate change happening on another thread.
        r.rpush("greet", "world")
        # Begin pipelining.
        p.multi()
        p.set("foo", nextf)

        with pytest.raises(redis.WatchError):
            p.execute()
    finally:
        p.reset()


def test_pipeline_succeeds_despite_unwatched_key_changed(r: redis.Redis):
    # Same setup as before except for the params to the WATCH command.
    r.set("foo", "bar")
    r.rpush("greet", "hello")
    p = r.pipeline()
    try:
        # Only watch one of the 2 keys.
        p.watch("foo")
        nextf = bytes(p.get("foo")) + b"baz"
        # Simulate change happening on another thread.
        r.rpush("greet", "world")
        p.multi()
        p.set("foo", nextf)
        p.execute()

        # Check the commands were executed.
        assert r.get("foo") == b"barbaz"
    finally:
        p.reset()


def test_pipeline_succeeds_when_watching_nonexistent_key(r: redis.Redis):
    r.set("foo", "bar")
    r.rpush("greet", "hello")
    p = r.pipeline()
    try:
        # Also watch a nonexistent key.
        p.watch("foo", "bam")
        nextf = bytes(p.get("foo")) + b"baz"
        # Simulate change happening on another thread.
        r.rpush("greet", "world")
        p.multi()
        p.set("foo", nextf)
        p.execute()

        # Check the commands were executed.
        assert r.get("foo") == b"barbaz"
    finally:
        p.reset()


def test_watch_state_is_cleared_across_multiple_watches(r: redis.Redis):
    r.set("foo", "one")
    r.set("bar", "baz")
    p = r.pipeline()

    try:
        p.watch("foo")
        # Simulate change happening on another thread.
        r.set("foo", "three")
        p.multi()
        p.set("foo", "three")
        with pytest.raises(redis.WatchError):
            p.execute()

        # Now watch another key.  It should be ok to change
        # foo as we're no longer watching it.
        p.watch("bar")
        r.set("foo", "four")
        p.multi()
        p.set("bar", "five")
        assert p.execute() == [True]
    finally:
        p.reset()


@pytest.mark.fake
def test_socket_cleanup_watch(fake_server):
    r1 = fakeredis.FakeStrictRedis(server=fake_server)
    r2 = fakeredis.FakeStrictRedis(server=fake_server)
    pipeline = r1.pipeline(transaction=False)
    # This needs some poking into redis-py internals to ensure that we reach
    # FakeSocket._cleanup. We need to close the socket while there is still
    # a watch in place, but not allow it to be garbage collected (hence we
    # set 'sock' even though it is unused).
    with pipeline:
        pipeline.watch("test")
        sock = pipeline.connection._sock  # noqa: F841
        pipeline.connection.disconnect()
    r2.set("test", "foo")


def test_get_within_pipeline(r: redis.Redis):
    r.set("test", "foo")
    r.set("test2", "foo2")
    expected_keys = set(r.keys())
    with r.pipeline() as p:
        assert set(r.keys()) == expected_keys
        p.watch("test")
        assert set(r.keys()) == expected_keys


@pytest.mark.fake
def test_get_within_pipeline_w_host():
    r = fakeredis.FakeRedis("localhost")
    r.set("test", "foo")
    r.set("test2", "foo2")
    expected_keys = set(r.keys())
    with r.pipeline() as p:
        assert set(r.keys()) == expected_keys
        p.watch("test")
        assert set(r.keys()) == expected_keys


@pytest.mark.fake
def test_get_within_pipeline_no_args():
    r = fakeredis.FakeRedis()
    r.set("test", "foo")
    r.set("test2", "foo2")
    expected_keys = set(r.keys())
    with r.pipeline() as p:
        assert set(r.keys()) == expected_keys
        p.watch("test")
        assert set(r.keys()) == expected_keys
