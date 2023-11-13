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


async def test_tls_password(df_factory, with_tls_server_args, with_tls_ca_cert_args):
    with df_factory.create(requirepass="XXX", **with_tls_server_args) as server:
        async with server.client(
            ssl=True, password="XXX", ssl_ca_certs=with_tls_ca_cert_args["ca_cert"]
        ) as client:
            await client.ping()


async def test_tls_client_certs(
    df_factory, with_ca_tls_server_args, with_tls_client_args, with_tls_ca_cert_args
):
    with df_factory.create(**with_ca_tls_server_args) as server:
        async with server.client(
            **with_tls_client_args, ssl_ca_certs=with_tls_ca_cert_args["ca_cert"]
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


async def test_config_enable_tls_with_ca_dir(
    df_factory, with_ca_dir_tls_server_args, with_tls_client_args
):
    server_args, ca_cert = with_ca_dir_tls_server_args
    server_args["tls"] = "true"

    with df_factory.create(**server_args) as server:
        async with server.client(**with_tls_client_args, ssl_ca_certs=ca_cert) as client:
            await client.execute_command("SET foo 44")
            res = await client.execute_command("GET foo")
            assert res == "44"


async def test_config_update_tls_certs(
    df_factory, with_tls_server_args, with_tls_ca_cert_args, tmp_dir
):
    # Generate new certificates.
    ca_key = os.path.join(tmp_dir, "ca-key-new.pem")
    ca_cert = os.path.join(tmp_dir, "ca-cert-new.pem")
    gen_ca_cert(ca_key, ca_cert)
    tls_server_key = os.path.join(tmp_dir, "df-key-new.pem")
    tls_server_req = os.path.join(tmp_dir, "df-req-new.pem")
    tls_server_cert = os.path.join(tmp_dir, "df-cert-new.pem")
    gen_certificate(
        ca_key,
        ca_cert,
        tls_server_req,
        tls_server_key,
        tls_server_cert,
    )

    with df_factory.create(requirepass="XXX", **with_tls_server_args) as server:
        async with server.client(
            ssl=True, password="XXX", ssl_ca_certs=with_tls_ca_cert_args["ca_cert"]
        ) as client:
            await client.config_set(
                "tls_key_file",
                tls_server_key,
            )
            await client.config_set("tls_cert_file", tls_server_cert)
            # Note must still set `tls true` to reload the TLS context.
            await client.config_set("tls", "true")

            # The existing connection should still work.
            await client.ping()

        # Connecting with the old CA should fail.
        with pytest.raises(redis.exceptions.ConnectionError):
            async with server.client(
                ssl=True, password="XXX", ssl_ca_certs=with_tls_ca_cert_args["ca_cert"]
            ) as client:
                await client.ping()

        # Connecting with the new CA should succeed.
        async with server.client(ssl=True, password="XXX", ssl_ca_certs=ca_cert) as client:
            await client.ping()


async def test_config_enable_tls(
    df_factory, with_ca_tls_server_args, with_tls_client_args, with_tls_ca_cert_args
):
    with df_factory.create() as server:
        async with server.client() as client:
            await client.ping()

            # Note the order here matters as flags are applied in order.
            await client.config_set(
                "tls_key_file",
                with_ca_tls_server_args["tls_key_file"],
            )
            await client.config_set(
                "tls_cert_file",
                with_ca_tls_server_args["tls_cert_file"],
            )
            await client.config_set(
                "tls_ca_cert_file",
                with_ca_tls_server_args["tls_ca_cert_file"],
            )
            await client.config_set(
                "tls",
                "true",
            )

            # The existing client should still be connected.
            await client.ping()

        # Connecting without TLS should fail.
        with pytest.raises(redis.exceptions.ConnectionError):
            async with server.client() as client_unauth:
                await client_unauth.ping()

        # Connecting with TLS should succeed.
        async with server.client(
            **with_tls_client_args, ssl_ca_certs=with_tls_ca_cert_args["ca_cert"]
        ) as client_tls:
            await client_tls.ping()


async def test_config_disable_tls(
    df_factory, with_ca_tls_server_args, with_tls_client_args, with_tls_ca_cert_args
):
    with df_factory.create(**with_ca_tls_server_args) as server:
        async with server.client(
            **with_tls_client_args, ssl_ca_certs=with_tls_ca_cert_args["ca_cert"]
        ) as client_tls:
            await client_tls.config_set("tls", "false")

        # Connecting without TLS should succeed.
        async with server.client() as client_unauth:
            await client_unauth.ping()
