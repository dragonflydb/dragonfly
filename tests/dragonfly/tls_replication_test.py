import pytest
import asyncio
import redis
from redis import asyncio as aioredis
from . import DflyInstanceFactory, dfly_args
import logging
import time

BASE_PORT = 1111

"""
Test replication with tls and non-tls options
"""

# 1. Number of master threads
# 2. Number of threads for each replica
replication_cases = [(8, 8)]

def tls_args(df_instance):
    with_tls_args = {"tls": "",
                    "tls_key_file": df_instance.dfly_path + "dfly-key.pem",
                    "tls_cert_file": df_instance.dfly_path + "dfly-cert.crt",
                    "no_tls_on_replica": "true"}
    return with_tls_args

@pytest.mark.asyncio
@pytest.mark.parametrize("t_master, t_replica", replication_cases)
@dfly_args({"dbfilename": "test-tls-replication-{{timestamp}}"})
async def test_replication_all(df_local_factory, df_seeder_factory, t_master, t_replica):

    # 1. Spin up dragonfly without tls, debug populate and then shut it down
    master = df_local_factory.create(port=BASE_PORT, proactor_threads=t_master)
    master.start()
    c_master = aioredis.Redis(port=master.port)
    await c_master.execute_command("DEBUG POPULATE 100")
    await c_master.execute_command("SAVE")
    db_size = await c_master.execute_command("DBSIZE")
    assert 100 == db_size

    master.stop()
    with_tls_args = tls_args(master)
    # 2. Spin up dragonfly again, this time, with different options:
    #   a. Enable tls
    #   b. Allow non-tls connection from replica to master
    master = df_local_factory.create(**with_tls_args, port=BASE_PORT, proactor_threads=t_master)
    master.start()
    # we need to sleep, because currently tls connections on master fail
    # TODO Fix this once master properly loads tls certificates and redis-cli does not
    # require --insecure flag, therefore for now we can't use `wait_available`
    time.sleep(10)

    # 3. Try to connect with redis cli on master. This should fail since redis-cli does
    # not use tls key and certificate
    c_master = aioredis.Redis(port=master.port)
    try:
        await c_master.execute_command("DBSIZE")
        raise "Non tls connection connected on master with tls. This should NOT happen"
    except redis.ConnectionError:
        pass

    # 4. Spin up a replica and initiate a REPLICAOF
    replica = df_local_factory.create(port=BASE_PORT + 1, proactor_threads=t_replica)
    replica.start()
    c_replica = aioredis.Redis(port=replica.port)
    res = await c_replica.execute_command("REPLICAOF localhost " + str(master.port))
    assert b"OK" == res
    time.sleep(10)

    # 5. Verify that replica dbsize == debug populate key size -- replication works
    db_size = await c_replica.execute_command("DBSIZE")
    assert 100 == db_size
