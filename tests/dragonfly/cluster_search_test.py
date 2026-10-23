import asyncio
import random

import pytest
import redis

from . import dfly_args
from .cluster_test_utils import apply_config, create_cluster, wait_for_ft_index_creation
from .instance import DflyInstanceFactory


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "cluster_search": "yes"})
async def test_SearchRequestDistribution(df_factory: DflyInstanceFactory):
    """
    Create cluster of 3 nodes.
    Send FT.CREATE to first node and check that index was created on all nodes.
    Search for all documents from cluster.
    """

    instances, nodes = await create_cluster(
        df_factory, 3, vmodule="coordinator=2,search_family=3,protocol_client=3"
    )
    nodes[0].slots = [(0, 5259)]
    nodes[1].slots = [(5260, 10519)]
    nodes[2].slots = [(10520, 16383)]

    await apply_config(nodes)

    assert (
        await nodes[0].client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "title", "TEXT"
        )
        == "OK"
    )

    for node in nodes:
        await wait_for_ft_index_creation(node.client, "idx")

    cclient = instances[0].cluster_client()

    docs_num = 100
    for i in range(docs_num):
        assert await cclient.execute_command("HSET", f"s{i}", "title", f"test {i}") == 1

    async def search_test():
        res = await nodes[0].client.execute_command(
            "FT.SEARCH", "idx", "@title:test", "text", "LIMIT", "0", "1000"
        )
        assert res[0] == docs_num
        for i in range(docs_num):
            assert f"s{i}" in res

    await asyncio.gather(*(search_test() for _ in range(docs_num)))


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "cluster_search": "yes"})
async def test_SortedSearchRequest(df_factory: DflyInstanceFactory):
    """
    Create cluster of 3 nodes.
    Execute Search request with sorting on indexed field.
    """

    instances, nodes = await create_cluster(
        df_factory, 3, vmodule="coordinator=2,search_family=3,protocol_client=3"
    )
    nodes[0].slots = [(0, 5259)]
    nodes[1].slots = [(5260, 10519)]
    nodes[2].slots = [(10520, 16383)]

    await apply_config(nodes)

    assert (
        await nodes[0].client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "title", "TEXT", "size", "NUMERIC"
        )
        == "OK"
    )

    for node in nodes:
        await wait_for_ft_index_creation(node.client, "idx")

    cclient = instances[0].cluster_client()

    docs_num = 100
    for i in range(docs_num):
        assert (
            await cclient.execute_command("HSET", f"s{i}", "title", f"test {i}", "size", f"{i}")
            == 2
        )

    async def search_test():
        limit_size = random.randint(1, docs_num // 2)
        offset = random.randint(0, docs_num // 2)
        res = await nodes[0].client.execute_command(
            "FT.SEARCH",
            "idx",
            "@title:test",
            "text",
            "SORTBY",
            "size",
            "ASC",
            "LIMIT",
            f"{offset}",
            f"{limit_size}",
        )
        assert res[0] == docs_num
        for i in range(offset, offset + limit_size):
            assert f"s{i}" in res, f"offset: {offset}, limit_size: {limit_size}, res: {res}"

        for i in range(offset):
            assert f"s{i}" not in res

        for i in range(offset + limit_size, docs_num):
            assert f"s{i}" not in res

    await asyncio.gather(*(search_test() for _ in range(2)))


@dfly_args({"proactor_threads": 4, "cluster_mode": "yes", "cluster_search": "yes"})
async def test_SearchAliasFanout(df_factory: DflyInstanceFactory):
    _, nodes = await create_cluster(
        df_factory, 2, vmodule="coordinator=2,search_family=3,protocol_client=3"
    )
    nodes[0].slots = [(0, 8191)]
    nodes[1].slots = [(8192, 16383)]
    await apply_config(nodes)

    assert (
        await nodes[0].client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "title", "TEXT"
        )
        == "OK"
    )
    for node in nodes:
        await wait_for_ft_index_creation(node.client, "idx")
    assert await nodes[0].client.execute_command("FT.ALIASADD", "alias", "idx") == "OK"
    with pytest.raises(redis.exceptions.ResponseError, match="no such index"):
        await nodes[1].client.execute_command("FT.SEARCH", "alias", "@title:test")

    # These keys hash to slots owned by nodes 0 and 1 respectively.
    keys = ("doc:0", "doc:2")
    assert await nodes[0].client.execute_command("HSET", keys[0], "title", "test") == 1
    assert await nodes[1].client.execute_command("HSET", keys[1], "title", "test") == 1

    async def search(client, index):
        return await asyncio.wait_for(
            client.execute_command("FT.SEARCH", index, "@title:test"), timeout=5
        )

    alias_search, canonical_search = await asyncio.gather(
        search(nodes[0].client, "alias"), search(nodes[1].client, "idx")
    )
    for result in (alias_search, canonical_search):
        assert result[0] == 2
        assert set(result[1::2]) == set(keys)
