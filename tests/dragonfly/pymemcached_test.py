import logging
import random
import socket
import ssl
import time

from pymemcache.client.base import Client as MCClient

from . import dfly_args
from .instance import DflyInstance

DEFAULT_ARGS = {"memcached_port": 11212, "proactor_threads": 4}


def read_response(client, expected_len):
    response = b""
    while len(response) < expected_len:
        data = client.recv(1024)
        if not data:
            break
        response += data
    return response


# Generic basic tests
@dfly_args(DEFAULT_ARGS)
class TestMemcached:
    def test_basic(self, memcached_client: MCClient):
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
        assert memcached_client.get("key1") is None

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

        assert memcached_client.gets("key5") == (b"1", b"0")

    # Noreply (and pipeline) tests
    async def test_noreply_pipeline(self, df_server: DflyInstance, memcached_client: MCClient):
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

    def test_get_many(self, memcached_client: MCClient):
        keys = [f"k{i}" for i in range(100)]
        for k in keys:
            memcached_client.set(k, k)
        assert memcached_client.get_many(keys) == {k: k.encode() for k in keys}

    def test_noreply_alternating(self, memcached_client: MCClient):
        """
        Assert alternating noreply works correctly, will cause many dispatch queue emptyings.
        """
        for i in range(200):
            if i % 2 == 0:
                memcached_client.set(f"k{i}", "D1", noreply=True)
                memcached_client.set(f"k{i}", "D2", noreply=True)
                memcached_client.set(f"k{i}", "D3", noreply=True)
            assert memcached_client.add(f"k{i}", "DX", noreply=False) == (i % 2 != 0)

    def test_length_in_set_command(self, df_server: DflyInstance, memcached_client: MCClient):
        """
        Test parser correctly reads value based on length and complains about bad chunks
        """
        cases = [b"NOTFOUR", b"FOUR", b"F4\r\n", b"\r\n\r\n"]

        for case in cases:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(("127.0.0.1", int(df_server["memcached_port"])))

            logging.info(f"Case {case}")
            client.sendall(b"set foo 0 0 4\r\n" + case + b"\r\n")
            response = client.recv(256).decode()
            if len(case) == 4:
                assert response == "STORED\r\n"
            else:
                # response should follow up with ERROR due to OUR\r\n being
                # parsed as unknown command but we can not guarantee that
                # it will be read in the same recv call, so just check the prefix.
                assert response.startswith("CLIENT_ERROR bad data chunk\r\n")

            client.close()

    def test_pipeline_get_then_stats_version(self, df_server: DflyInstance):
        """
        Verify GET pipelined before STATS or VERSION doesn't crash the server.
        """
        port = int(df_server["memcached_port"])

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(5)
        client.connect(("127.0.0.1", port))
        client.sendall(b"get nokey\r\nversion\r\n")
        response = read_response(client, len(b"END\r\nVERSION 1.6.0 DF\r\n"))
        client.close()
        assert response == b"END\r\nVERSION 1.6.0 DF\r\n"

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(5)
        client.connect(("127.0.0.1", port))
        client.sendall(b"get nokey\r\nstats\r\n")
        # Read until both GET's END and STATS' END are received before closing.
        response = b""
        while response.count(b"END\r\n") < 2:
            response += client.recv(4096)
        client.close()
        assert response.startswith(b"END\r\nSTAT ")

    def test_error_in_pipeline(self, df_server: DflyInstance, memcached_client: MCClient):
        """
        Verify correct responses to  "get x\r\ngetaa\r\nget y z\r\n"
        """
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(5)
        client.connect(("127.0.0.1", int(df_server["memcached_port"])))

        client.sendall(b"get x\r\ngetaa\r\nget y z\r\n")

        expected = b"END\r\nERROR\r\nEND\r\n"
        response = read_response(client, len(expected))
        client.close()

        assert response == expected

    def test_large_request(self, memcached_client):
        assert memcached_client.set(b"key1", b"d" * 4096, noreply=False)
        assert memcached_client.set(b"key2", b"d" * 4096 * 2, noreply=False)

    def test_version(self, memcached_client: MCClient):
        """
        php-memcached client expects version to be in the format of "n.n.n", so we return 1.5.0 emulating an old memcached server.
        Our real version is being returned in the stats command.
        Also verified manually that php client parses correctly the version string that ends with "DF".
        """
        assert b"1.6.0 DF" == memcached_client.version()
        stats = memcached_client.stats()
        version = stats[b"version"].decode("utf-8")
        assert version.startswith("v") or version == "dev"

    def test_flags(self, memcached_client: MCClient):
        for i in range(1, 20):
            flags = random.randrange(50, 1000)
            memcached_client.set("a", "real-value", flags=flags, noreply=True)

            res = memcached_client.raw_command("get a", "END\r\n").split()
            # workaround sometimes memcached_client.raw_command returns empty str
            if len(res) > 0:
                assert res[2].decode() == str(flags)

    def test_expiration(self, memcached_client: MCClient):
        assert not memcached_client.default_noreply

        assert memcached_client.set("key1", "value1", 2)
        assert memcached_client.set("key2", "value2", int(time.time()) + 2)
        assert memcached_client.set("key3", "value3", int(time.time()) + 200)
        assert memcached_client.get("key1") == b"value1"
        assert memcached_client.get("key2") == b"value2"
        assert memcached_client.get("key3") == b"value3"
        assert memcached_client.set("key3", "value3", int(time.time()) - 200)
        assert memcached_client.get("key3") is None
        time.sleep(2)
        assert memcached_client.get("key1") is None
        assert memcached_client.get("key2") is None
        assert memcached_client.get("key3") is None

    def test_pipeline_cas_crash(self, df_server: DflyInstance, memcached_client: MCClient):
        """
        Tests that an unsupported/invalid command (CAS) sent in a pipeline
        after an async command (GETS) does not crash the server
        and correctly buffers the error reply in order.
        """
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.settimeout(5)
        client_sock.connect(("127.0.0.1", int(df_server["memcached_port"])))

        # Command sequence:
        # 1. SET (sync)
        # 2. GETS (async - forces the next command to not be the head)
        # 3. CAS (hits the default block, triggering the early error)
        payload = (
            b"set mykey 0 0 5\r\nvalue\r\n" b"gets mykey\r\n" b"cas mykey 0 0 5 12345\r\nvalue\r\n"
        )
        client_sock.sendall(payload)

        response = b""
        while b"CLIENT_ERROR bad command line format\r\n" not in response:
            data = client_sock.recv(4096)
            if not data:
                break
            response += data
        client_sock.close()

        # Ensure strict ordering: STORED -> GETS (VALUE + END) -> CLIENT_ERROR
        idx_stored = response.find(b"STORED\r\n")
        idx_value = response.find(b"VALUE mykey")
        idx_error = response.find(b"CLIENT_ERROR bad command line format")
        # Look for the GETS terminator specifically AFTER the value
        idx_end = response.find(b"END\r\n", idx_value)

        assert idx_stored != -1 and idx_value != -1 and idx_error != -1 and idx_end != -1
        assert (
            idx_stored < idx_value < idx_end < idx_error
        ), f"Responses out of order/interleaved: {response}"

        # Final sanity check to ensure the connection/server is still healthy
        assert memcached_client.set("sanity_check", "alive")
        assert memcached_client.get("sanity_check") == b"alive"


@dfly_args(DEFAULT_ARGS)
def test_memcached_tls_no_requirepass(df_factory, with_tls_server_args, with_tls_ca_cert_args):
    """
    Test for issue #5084: ability to use TLS for Memcached without requirepass.

    Dragonfly required a password to be set when using TLS, but the Memcached protocol
    does not support password authentication. This test verifies that we can start
    the server with TLS enabled but without specifying requirepass and with the Memcached port.
    """
    # Create arguments for TLS without specifying requirepass
    server_args = {**DEFAULT_ARGS, **with_tls_server_args, "requirepass": "test_password"}

    # Create and start the server - it should not crash
    server = df_factory.create(**server_args)
    server.start()

    # Give the server time to start
    time.sleep(1)

    # Create SSL context for client
    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations(with_tls_ca_cert_args["ca_cert"])
    ssl_context.check_hostname = False

    # Disable certificate verification (since we don't provide a client certificate)
    ssl_context.verify_mode = ssl.CERT_NONE

    # Output port information for diagnostics
    logging.info(f"Connecting to memcached port: {server.mc_port} on host: 127.0.0.1")

    # Connect to Memcached over TLS
    client = MCClient(("127.0.0.1", server.mc_port), tls_context=ssl_context)

    # Test basic operations
    assert client.set("foo", "bar")
    assert client.get("foo") == b"bar"


@dfly_args(DEFAULT_ARGS)
def test_memcached_half_close(df_server: DflyInstance):
    """
    Verify the server replies to buffered commands even when the client
    half-closes the connection (sends data then shuts down the write side).
    Requires a fresh server so the first recv sees data+FIN together.
    """
    port = int(df_server["memcached_port"])
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    sock.connect(("127.0.0.1", port))

    sock.sendall(b"set hc 0 0 3\r\nfoo\r\nget hc\r\n")
    sock.shutdown(socket.SHUT_WR)

    response = b""
    while True:
        data = sock.recv(4096)
        if not data:
            break
        response += data
    sock.close()

    assert response == b"STORED\r\nVALUE hc 0 3\r\nfoo\r\nEND\r\n"
