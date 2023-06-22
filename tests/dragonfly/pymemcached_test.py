import pytest
import pymemcache
from . import dfly_args


@dfly_args({"memcached_port": 11211})
def test_add_get(memcached_connection):
    assert memcached_connection.add(b"key", b"data", noreply=False)
    assert memcached_connection.get(b"key") == b"data"

@dfly_args({"memcached_port": 11211})
def test_add_set(memcached_connection):
    assert memcached_connection.add(b"key", b"data", noreply=False)
    memcached_connection.set(b"key", b"other")
    assert memcached_connection.get(b"key") == b"other"

@dfly_args({"memcached_port": 11211})
def test_set_add(memcached_connection):
    memcached_connection.set(b"key", b"data")
    # stuck here
    assert not memcached_connection.add(b"key", b"other", noreply=False)
    # expects to see NOT_STORED
    memcached_connection.set(b"key", b"other")
    assert memcached_connection.get(b"key") == b"other"

@dfly_args({"memcached_port": 11211})
def test_mixed_reply(memcached_connection):
    memcached_connection.set(b"key", b"data", noreply=True)
    memcached_connection.add(b"key", b"other", noreply=False)
    memcached_connection.add(b"key", b"final", noreply=True)

    assert memcached_connection.get(b"key") == b"data"
