import logging
import pytest
from pymemcache.client.base import Client as MCClient
from redis import Redis
import socket
import random
import time

from . import dfly_args
from .instance import DflyInstance

DEFAULT_ARGS = {"memcached_port": 11211, "proactor_threads": 4}

# Generic basic tests


@dfly_args(DEFAULT_ARGS)
def test_basic(memcached_client: MCClient):
    assert not memcached_client.default_noreply

    # set -> replace -> add -> get
    assert memcached_client.set("key1", "value1")
    assert memcached_client.replace("key1", "value2")
    assert not memcached_client.add("key1", "value3")
    assert memcached_client.get("key1") == b"value2"

    # add -> get
    assert memcached_client.add("key2", "value1")
    assert memcached_client.get("key2") == b"value1"

    # delete
    assert memcached_client.delete("key1")
    assert not memcached_client.delete("key3")
    assert memcached_client.get("key1") == None

    # prepend append
    assert memcached_client.set("key4", "B")
    assert memcached_client.prepend("key4", "A")
    assert memcached_client.append("key4", "C")
    assert memcached_client.get("key4") == b"ABC"

    # incr
    memcached_client.set("key5", 0)
    assert memcached_client.incr("key5", 1) == 1
    assert memcached_client.incr("key5", 1) == 2
    assert memcached_client.decr("key5", 1) == 1


# Noreply (and pipeline) tests


@dfly_args(DEFAULT_ARGS)
async def test_noreply_pipeline(df_server: DflyInstance, memcached_client: MCClient):
    """
    With the noreply option the python client doesn't wait for replies,
    so all the commands are pipelined. Assert pipelines work correctly and the
    succeeding regular command receives a reply (it should join the pipeline as last).
    """

    client = df_server.client()
    for attempts in range(2):
        keys = [f"k{i}" for i in range(1000)]
        values = [f"d{i}" for i in range(len(keys))]

        for k, v in zip(keys, values):
            memcached_client.set(k, v, noreply=True)

        # quick follow up before the pipeline finishes
        assert memcached_client.get("k10") == b"d10"
        # check all commands were executed
        assert memcached_client.get_many(keys) == {k: v.encode() for k, v in zip(keys, values)}

        info = await client.info()
        if info["total_pipelined_commands"] > 100:
            return
        logging.warning(
            f"Have not identified pipelining at attempt {attempts} Info: \n" + str(info)
        )
        await client.flushall()

    assert False, "Pipelining not detected"


@dfly_args(DEFAULT_ARGS)
def test_noreply_alternating(memcached_client: MCClient):
    """
    Assert alternating noreply works correctly, will cause many dispatch queue emptyings.
    """
    for i in range(200):
        if i % 2 == 0:
            memcached_client.set(f"k{i}", "D1", noreply=True)
            memcached_client.set(f"k{i}", "D2", noreply=True)
            memcached_client.set(f"k{i}", "D3", noreply=True)
        assert memcached_client.add(f"k{i}", "DX", noreply=False) == (i % 2 != 0)


# Raw connection tests


@dfly_args(DEFAULT_ARGS)
def test_length_in_set_command(df_server: DflyInstance, memcached_client: MCClient):
    """
    Test parser correctly reads value based on length and complains about bad chunks
    """
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(("127.0.0.1", int(df_server["memcached_port"])))

    cases = [b"NOTFOUR", b"FOUR", b"F4\r\n", b"\r\n\r\n"]

    # TODO: \r\n hangs

    for case in cases:
        print("case", case)
        client.sendall(b"set foo 0 0 4\r\n" + case + b"\r\n")
        response = client.recv(256)
        if len(case) == 4:
            assert response == b"STORED\r\n"
        else:
            assert response == b"CLIENT_ERROR bad data chunk\r\n"

    client.close()


# Auxiliary tests


@dfly_args(DEFAULT_ARGS)
def test_large_request(memcached_client):
    assert memcached_client.set(b"key1", b"d" * 4096, noreply=False)
    assert memcached_client.set(b"key2", b"d" * 4096 * 2, noreply=False)


@dfly_args(DEFAULT_ARGS)
def test_version(memcached_client: MCClient):
    """
    php-memcached client expects version to be in the format of "n.n.n", so we return 1.5.0 emulating an old memcached server.
    Our real version is being returned in the stats command.
    Also verified manually that php client parses correctly the version string that ends with "DF".
    """
    assert b"1.6.0 DF" == memcached_client.version()
    stats = memcached_client.stats()
    version = stats[b"version"].decode("utf-8")
    assert version.startswith("v") or version == "dev"


@dfly_args(DEFAULT_ARGS)
def test_flags(memcached_client: MCClient):
    for i in range(1, 20):
        flags = random.randrange(50, 1000)
        memcached_client.set("a", "real-value", flags=flags, noreply=True)

        res = memcached_client.raw_command("get a", "END\r\n").split()
        # workaround sometimes memcached_client.raw_command returns empty str
        if len(res) > 0:
            assert res[2].decode() == str(flags)


@dfly_args(DEFAULT_ARGS)
def test_expiration(memcached_client: MCClient):
    assert not memcached_client.default_noreply

    assert memcached_client.set("key1", "value1", 2)
    assert memcached_client.set("key2", "value2", int(time.time()) + 2)
    assert memcached_client.set("key3", "value3", int(time.time()) + 200)
    assert memcached_client.get("key1") == b"value1"
    assert memcached_client.get("key2") == b"value2"
    assert memcached_client.get("key3") == b"value3"
    assert memcached_client.set("key3", "value3", int(time.time()) - 200)
    assert memcached_client.get("key3") == None
    time.sleep(2)
    assert memcached_client.get("key1") == None
    assert memcached_client.get("key2") == None
    assert memcached_client.get("key3") == None
