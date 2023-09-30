import pytest
import redis
from .utility import *
from .instance import DflyStartException


async def test_tls_no_auth(df_factory, with_tls_server_args):
    # Needs some authentication
    server = df_factory.create(**with_tls_server_args)
    with pytest.raises(DflyStartException):
        server.start()


async def test_tls_no_key(df_factory):
    # Needs a private key and certificate.
    server = df_factory.create(tls=None, requirepass="XXX")
    with pytest.raises(DflyStartException):
        server.start()


async def test_tls_password(df_factory, with_tls_server_args, gen_ca_cert):
    with df_factory.create(requirepass="XXX", **with_tls_server_args) as server:
        async with server.client(
            ssl=True, password="XXX", ssl_ca_certs=gen_ca_cert["ca_cert"]
        ) as client:
            await client.ping()


async def test_tls_client_certs(
    df_factory, with_ca_tls_server_args, with_tls_client_args, gen_ca_cert
):
    with df_factory.create(**with_ca_tls_server_args) as server:
        async with server.client(
            **with_tls_client_args, ssl_ca_certs=gen_ca_cert["ca_cert"]
        ) as client:
            await client.ping()


async def test_client_tls_no_auth(df_factory):
    server = df_factory.create(tls_replication=None)
    with pytest.raises(DflyStartException):
        server.start()


async def test_client_tls_password(df_factory):
    with df_factory.create(tls_replication=None, masterauth="XXX"):
        pass


async def test_client_tls_cert(df_factory, with_tls_server_args):
    key_args = with_tls_server_args.copy()
    key_args.pop("tls")
    with df_factory.create(tls_replication=None, **key_args):
        pass
