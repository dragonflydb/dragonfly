from .instance import DflyInstance
from . import dfly_args
from meta_memcache import (
    Key,
    ServerAddress,
    CacheClient,
    connection_pool_factory_builder,
)

DEFAULT_ARGS = {"memcached_port": 11211, "proactor_threads": 4}


@dfly_args(DEFAULT_ARGS)
def test_basic(df_server: DflyInstance):
    pool = CacheClient.cache_client_from_servers(
        servers=[
            ServerAddress(host="localhost", port=DEFAULT_ARGS.get("memcached_port")),
        ],
        connection_pool_factory_fn=connection_pool_factory_builder(),
    )
    # TODO: to add integration tests
