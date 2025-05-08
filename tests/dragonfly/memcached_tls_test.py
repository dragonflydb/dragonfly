from .instance import DflyInstance
from . import dfly_args
import pymemcache.client.base
from pymemcache.client.base import Client as MCClient
import ssl
import pytest
import time
import socket

# Basic arguments for enabling memcached and TLS
DEFAULT_ARGS = {"memcached_port": 11211, "proactor_threads": 4}


@dfly_args(DEFAULT_ARGS)
def test_memcached_tls_no_requirepass(df_factory, with_tls_server_args, with_tls_ca_cert_args):
    """
    Test for issue #5084: ability to use TLS for Memcached without requirepass.

    Dragonfly required a password to be set when using TLS, but the Memcached protocol
    does not support password authentication. This test verifies that we can start
    the server with TLS enabled but without specifying requirepass and with the Memcached port.
    """
    # Create arguments for TLS without specifying requirepass
    server_args = {**DEFAULT_ARGS, **with_tls_server_args}

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
    print(f"Connecting to memcached port: {server.mc_port} on host: 127.0.0.1")

    # Connect to Memcached over TLS
    client = MCClient(("127.0.0.1", server.mc_port), tls_context=ssl_context)

    # Test basic operations
    assert client.set("foo", "bar")
    assert client.get("foo") == b"bar"
