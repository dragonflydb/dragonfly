"""
Test compatibility with the redis-py client search module.
Search correctness should be ensured with unit tests.
"""
import pytest
from redis import asyncio as aioredis
from .utility import *

from redis.commands.search.query import Query
from redis.commands.search.field import TextField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

TEST_DATA = [
    {"title": "First article", "content": "Long description"},
    {"title": "Second article", "content": "Small text"},
    {"title": "Third piece", "content": "Brief description"},
    {"title": "Last piece", "content": "Interesting text"},
]

TEST_DATA_SCHEMA = [TextField("title"), TextField("content")]


async def index_test_data(async_client: aioredis.Redis, itype: IndexType, prefix=""):
    for i, e in enumerate(TEST_DATA):
        if itype == IndexType.HASH:
            await async_client.hset(prefix+str(i), mapping=e)
        else:
            await async_client.json().set(prefix+str(i), "$", e)


def contains_test_data(res, td_indices):
    if res.total != len(td_indices):
        return False

    docset = set()
    for doc in res.docs:
        docset.add(f"{doc.title}//{doc.content}")

    for td_entry in (TEST_DATA[tdi] for tdi in td_indices):
        if not f"{td_entry['title']}//{td_entry['content']}" in docset:
            return False

    return True


@pytest.mark.parametrize("index_type", [IndexType.HASH, IndexType.JSON])
async def test_basic(async_client, index_type):
    i1 = async_client.ft("i-"+str(index_type))
    await i1.create_index(TEST_DATA_SCHEMA, definition=IndexDefinition(index_type=index_type))
    await index_test_data(async_client, index_type)

    res = await i1.search("article")
    assert contains_test_data(res, [0, 1])

    res = await i1.search("text")
    assert contains_test_data(res, [1, 3])

    res = await i1.search("brief piece")
    assert contains_test_data(res, [2])

    res = await i1.search("@title:(article|last) @content:text")
    assert contains_test_data(res, [1, 3])
