"""
Test compatibility with the redis-py client search module.
Search correctness should be ensured with unit tests.
"""
import pytest
from redis import asyncio as aioredis
from .utility import *
from . import dfly_args

import numpy as np

from redis.commands.search.query import Query
from redis.commands.search.field import TextField, NumericField, TagField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

TEST_DATA = [
    {
        "title": "First article",
        "content": "Long description",
        "views": 100,
        "topic": "world, science",
    },
    {
        "title": "Second article",
        "content": "Small text",
        "views": 200,
        "topic": "national, policits",
    },
    {
        "title": "Third piece",
        "content": "Brief description",
        "views": 300,
        "topic": "health, lifestyle",
    },
    {
        "title": "Last piece",
        "content": "Interesting text",
        "views": 400,
        "topic": "world, business",
    },
]

BASIC_TEST_SCHEMA = [
    TextField("title"),
    TextField("content"),
    NumericField("views"),
    TagField("topic"),
]


async def index_test_data(async_client: aioredis.Redis, itype: IndexType, prefix=""):
    for i, e in enumerate(TEST_DATA):
        if itype == IndexType.HASH:
            await async_client.hset(prefix + str(i), mapping=e)
        else:
            await async_client.json().set(prefix + str(i), "$", e)


def doc_to_str(doc):
    if not type(doc) is dict:
        doc = doc.__dict__

    doc = dict(doc)  # copy to remove fields
    doc.pop("id", None)
    doc.pop("payload", None)

    return "//".join(sorted(doc))


def contains_test_data(res, td_indices):
    if res.total != len(td_indices):
        return False

    docset = {doc_to_str(doc) for doc in res.docs}

    for td_entry in (TEST_DATA[tdi] for tdi in td_indices):
        if not doc_to_str(td_entry) in docset:
            return False

    return True


@dfly_args({"proactor_threads": 4})
@pytest.mark.parametrize("index_type", [IndexType.HASH, IndexType.JSON])
async def test_basic(async_client: aioredis.Redis, index_type):
    i1 = async_client.ft("i1-" + str(index_type))

    await index_test_data(async_client, index_type)
    await i1.create_index(BASIC_TEST_SCHEMA, definition=IndexDefinition(index_type=index_type))

    res = await i1.search("article")
    assert contains_test_data(res, [0, 1])

    res = await i1.search("text")
    assert contains_test_data(res, [1, 3])

    res = await i1.search("brief piece")
    assert contains_test_data(res, [2])

    res = await i1.search("@title:(article|last) @content:text")
    assert contains_test_data(res, [1, 3])

    res = await i1.search("@views:[200 300]")
    assert contains_test_data(res, [1, 2])

    res = await i1.search("@views:[0 150] | @views:[350 500]")
    assert contains_test_data(res, [0, 3])

    res = await i1.search("@topic:{world}")
    assert contains_test_data(res, [0, 3])

    res = await i1.search("@topic:{business}")
    assert contains_test_data(res, [3])

    res = await i1.search("@topic:{world | national}")
    assert contains_test_data(res, [0, 1, 3])

    res = await i1.search("@topic:{science | health}")
    assert contains_test_data(res, [0, 2])


async def knn_query(idx, query, vector):
    params = {"vec": np.array(vector, dtype=np.float32).tobytes()}
    result = await idx.search(query, params)
    return {doc["id"] for doc in result.docs}


@dfly_args({"proactor_threads": 4})
@pytest.mark.parametrize("index_type", [IndexType.HASH, IndexType.JSON])
async def test_knn(async_client: aioredis.Redis, index_type):
    i2 = async_client.ft("i2-" + str(index_type))

    vector_field = VectorField(
        "pos",
        "FLAT",
        {
            "TYPE": "FLOAT32",
            "DIM": 1,
            "DISTANCE_METRIC": "L2",
        },
    )

    await i2.create_index(
        [TagField("even"), vector_field], definition=IndexDefinition(index_type=index_type)
    )

    pipe = async_client.pipeline()
    for i in range(100):
        even = "yes" if i % 2 == 0 else "no"
        if index_type == IndexType.HASH:
            pos = np.array(i, dtype=np.float32).tobytes()
            pipe.hset(f"k{i}", mapping={"even": even, "pos": pos})
        else:
            pipe.json().set(f"k{i}", "$", {"even": even, "pos": [float(i)]})
    await pipe.execute()

    assert await knn_query(i2, "* => [KNN 3 @pos VEC]", [50.0]) == {"k49", "k50", "k51"}

    assert await knn_query(i2, "@even:{yes} => [KNN 3 @pos VEC]", [20.0]) == {"k18", "k20", "k22"}

    assert await knn_query(i2, "@even:{no} => [KNN 4 @pos VEC]", [30.0]) == {
        "k27",
        "k29",
        "k31",
        "k33",
    }

    assert await knn_query(i2, "@even:{yes} => [KNN 3 @pos VEC]", [10.0] == {"k8", "k10", "k12"})


NUM_DIMS = 10
NUM_POINTS = 100


@dfly_args({"proactor_threads": 4})
@pytest.mark.parametrize("index_type", [IndexType.HASH, IndexType.JSON])
async def test_multidim_knn(async_client: aioredis.Redis, index_type):
    vector_field = VectorField(
        "pos",
        "FLAT",
        {
            "TYPE": "FLOAT32",
            "DIM": NUM_DIMS,
            "DISTANCE_METRIC": "L2",
        },
    )

    i3 = async_client.ft("i3-" + str(index_type))
    await i3.create_index([vector_field], definition=IndexDefinition(index_type=index_type))

    def rand_point():
        return np.random.uniform(0, 10, NUM_DIMS).astype(np.float32)

    # Generate points and send to DF
    points = [rand_point() for _ in range(NUM_POINTS)]
    points = list(enumerate(points))

    pipe = async_client.pipeline(transaction=False)
    for i, point in points:
        if index_type == IndexType.HASH:
            pipe.hset(f"k{i}", mapping={"pos": point.tobytes()})
        else:
            pipe.json().set(f"k{i}", "$", {"pos": point.tolist()})
    await pipe.execute()

    # Run 10 random queries
    for _ in range(10):
        center = rand_point()
        limit = random.randint(1, NUM_POINTS / 10)

        expected_ids = [
            f"k{i}"
            for i, point in sorted(points, key=lambda p: np.linalg.norm(center - p[1]))[:limit]
        ]

        got_ids = await knn_query(i3, f"* => [KNN {limit} @pos VEC]", center)

        assert set(expected_ids) == set(got_ids)
