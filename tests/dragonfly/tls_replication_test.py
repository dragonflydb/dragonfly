import pytest
import asyncio
import redis
from redis import asyncio as aioredis
from . import DflyInstanceFactory, dfly_args
import logging
import time
from dragonfly.replication_test import check_all_replicas_finished

BASE_PORT = 1111
ADMIN_PORT = 1211

"""
Test replication with tls and non-tls options
"""

# 1. Number of master threads
# 2. Number of threads for each replica
replication_cases = [(8, 8)]

@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_replica", replication_cases)
@dfly_args({"dbfilename": "test-tls-replication-{{timestamp}}", "admin_nopass" : True})
async def test_no_tls_on_admin_port(gen_tls_cert, tls_server_key_file_name, tls_server_cert_file_name, df_local_factory, df_seeder_factory, t_master, t_replica):
    gen_tls_cert
    with_tls_args = {"tls": "",
                    "tls_key_file": df_local_factory.dfly_path + tls_server_key_file_name,
                    "tls_cert_file": df_local_factory.dfly_path + tls_server_cert_file_name,
                    "no_tls_on_admin_port": "true"}
    # 1. Spin up dragonfly without tls, debug populate
    master = df_local_factory.create(admin_port=ADMIN_PORT, **with_tls_args, port=BASE_PORT, proactor_threads=t_master)
    master.start()
    c_master = aioredis.Redis(port=master.admin_port)
    await c_master.execute_command("DEBUG POPULATE 100")
    db_size = await c_master.execute_command("DBSIZE")
    assert 100 == db_size

    # 2. Spin up a replica and initiate a REPLICAOF
    replica = df_local_factory.create(admin_port=ADMIN_PORT + 1, **with_tls_args, port=BASE_PORT + 1, proactor_threads=t_replica)
    replica.start()
    c_replica = aioredis.Redis(port=replica.admin_port)
    res = await c_replica.execute_command("REPLICAOF localhost " + str(master.admin_port))
    assert b"OK" == res
    await check_all_replicas_finished([c_replica], c_master)

    # 3. Verify that replica dbsize == debug populate key size -- replication works
    db_size = await c_replica.execute_command("DBSIZE")
    assert 100 == db_size
