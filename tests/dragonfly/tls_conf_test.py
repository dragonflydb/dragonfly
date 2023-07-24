import pytest
import redis
from .utility import *
from . import DflyStartException


def test_tls_no_auth(df_factory, with_tls_server_args):
    # Needs some authentication
    server = df_factory.create(port=1111, **with_tls_server_args)
    with pytest.raises(DflyStartException):
        server.start()


@pytest.mark.skip("Enable after #1562 is merged")
def test_tls_no_key(df_factory):
    # Needs a private key and certificate.
    server = df_factory.create(port=1112, tls=None, requirepass="XXX")
    with pytest.raises(DflyStartException):
        server.start()


def test_tls_password(df_factory, with_tls_server_args):
    server = df_factory.create(port=1113, requirepass="XXX", **with_tls_server_args)
    server.start()
    server.stop()


def test_tls_client_certs(df_factory, with_ca_tls_server_args):
    server = df_factory.create(port=1114, **with_ca_tls_server_args)
    server.start()
    server.stop()


def test_client_tls_no_auth(df_factory):
    server = df_factory.create(port=1115, tls_replication=None)
    with pytest.raises(DflyStartException):
        server.start()


def test_client_tls_password(df_factory):
    server = df_factory.create(port=1116, tls_replication=None, masterauth="XXX")
    server.start()
    server.stop()


def test_client_tls_cert(df_factory, with_tls_server_args):
    key_args = with_tls_server_args.copy()
    key_args.pop("tls")
    server = df_factory.create(port=1117, tls_replication=None, **key_args)
    server.start()
    server.stop()
