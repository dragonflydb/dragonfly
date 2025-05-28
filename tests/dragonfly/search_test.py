"""
Test compatibility with the redis-py client search module.
Search correctness should be ensured with unit tests.
"""
import pytest
from redis import asyncio as aioredis
from .utility import *
from . import dfly_args
import copy

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


def fix_schema_naming(itype: IndexType, idx_list: list):
    """Copy all schema fields and for json types, change name to json $.path and add alias"""
    if itype == IndexType.HASH:
        return idx_list
    copies = [copy.copy(idx) for idx in idx_list]
    for idx in copies:
        idx.as_name = idx.name
        idx.name = "$." + idx.name
    return copies


async def index_test_data(async_client: aioredis.Redis, itype: IndexType, prefix=""):
    for i, e in enumerate(TEST_DATA):
        if itype == IndexType.HASH:
            await async_client.hset(prefix + str(i), mapping=e)
        else:
            await async_client.json().set(prefix + str(i), "$", e)


def doc_to_str(index_type, doc):
    if not type(doc) is dict:
        doc = doc.__dict__

    if "json" in doc:
        return json.dumps(json.loads(doc["json"]), sort_keys=True)

    if index_type == IndexType.JSON:
        return json.dumps(doc, sort_keys=True)

    doc = dict(doc)  # copy to remove fields
    doc.pop("id", None)
    doc.pop("payload", None)

    return "//".join(sorted(doc))


def contains_test_data(itype, res, td_indices):
    if res.total != len(td_indices):
        return False

    docset = {doc_to_str(itype, doc) for doc in res.docs}

    for td_entry in (TEST_DATA[tdi] for tdi in td_indices):
        if not doc_to_str(itype, td_entry) in docset:
            return False

    return True


@dfly_args({"proactor_threads": 4})
async def test_management(async_client: aioredis.Redis):
    SCHEMA_1 = [TextField("f1"), NumericField("f2", sortable=True)]
    SCHEMA_2 = [
        NumericField("f3", no_index=True, sortable=True),
        TagField("f4"),
        VectorField(
            "f5",
            algorithm="HNSW",
            attributes={"TYPE": "FLOAT32", "DIM": 1, "DISTANCE_METRIC": "L2", "INITIAL_CAP": 100},
        ),
    ]

    i1 = async_client.ft("i1")
    i2 = async_client.ft("i2")

    await i1.create_index(SCHEMA_1, definition=IndexDefinition(prefix=["p1"]))
    await i2.create_index(SCHEMA_2, definition=IndexDefinition(prefix=["p2"]))

    # Fill indices with 10 and 15 docs respectively
    for i in range(10):
        await async_client.hset(f"p1-{i}", mapping={"f1": "ok", "f2": 11})
    for i in range(15):
        await async_client.hset(
            f"p2-{i}",
            mapping={"f3": 12, "f4": "hmm", "f5": np.array(0).astype(np.float32).tobytes()},
        )

    assert sorted(await async_client.execute_command("FT._LIST")) == ["i1", "i2"]

    i1info = await i1.info()
    assert i1info["index_definition"] == ["key_type", "HASH", "prefix", "p1"]
    assert i1info["num_docs"] == 10
    assert sorted(i1info["attributes"]) == [
        ["identifier", "f1", "attribute", "f1", "type", "TEXT"],
        ["identifier", "f2", "attribute", "f2", "type", "NUMERIC", "SORTABLE"],
    ]

    i2info = await i2.info()
    assert i2info["index_definition"] == ["key_type", "HASH", "prefix", "p2"]
    assert i2info["num_docs"] == 15
    assert sorted(i2info["attributes"]) == [
        ["identifier", "f3", "attribute", "f3", "type", "NUMERIC", "NOINDEX", "SORTABLE"],
        ["identifier", "f4", "attribute", "f4", "type", "TAG"],
        ["identifier", "f5", "attribute", "f5", "type", "VECTOR"],
    ]

    await i1.dropindex()
    await i2.dropindex()

    assert await async_client.execute_command("FT._LIST") == []


@dfly_args({"proactor_threads": 4})
@pytest.mark.parametrize("index_type", [IndexType.HASH, IndexType.JSON])
async def test_basic(async_client: aioredis.Redis, index_type):
    i1 = async_client.ft("i1-" + str(index_type))

    await index_test_data(async_client, index_type)
    await i1.create_index(
        fix_schema_naming(index_type, BASIC_TEST_SCHEMA),
        definition=IndexDefinition(index_type=index_type),
    )

    res = await i1.search("article")
    assert contains_test_data(index_type, res, [0, 1])

    res = await i1.search("text")
    assert contains_test_data(index_type, res, [1, 3])

    res = await i1.search("brief piece")
    assert contains_test_data(index_type, res, [2])

    res = await i1.search("@title:(article|last) @content:text")
    assert contains_test_data(index_type, res, [1, 3])

    res = await i1.search("@views:[200 300]")
    assert contains_test_data(index_type, res, [1, 2])

    res = await i1.search("@views:[0 150] | @views:[350 500]")
    assert contains_test_data(index_type, res, [0, 3])

    res = await i1.search("@topic:{world}")
    assert contains_test_data(index_type, res, [0, 3])

    res = await i1.search("@topic:{business}")
    assert contains_test_data(index_type, res, [3])

    res = await i1.search("@topic:{world | national}")
    assert contains_test_data(index_type, res, [0, 1, 3])

    res = await i1.search("@topic:{science | health}")
    assert contains_test_data(index_type, res, [0, 2])

    await i1.dropindex()


@dfly_args({"proactor_threads": 4})
async def test_big_json(async_client: aioredis.Redis):
    i1 = async_client.ft("i1")
    gen_arr = lambda base: {"blob": [base + str(i) for i in range(100)]}

    await async_client.json().set("k1", "$", gen_arr("alex"))
    await async_client.json().set("k2", "$", gen_arr("bob"))

    await i1.create_index(
        [TextField(name="$.blob", as_name="items")],
        definition=IndexDefinition(index_type=IndexType.JSON),
    )

    res = await i1.search("alex55")
    assert res.docs[0].id == "k1"

    res = await i1.search("bob77")
    assert res.docs[0].id == "k2"

    res = await i1.search("alex11 | bob22")
    assert res.total == 2

    await i1.dropindex()


async def knn_query(idx, query, vector):
    params = {"vec": np.array(vector, dtype=np.float32).tobytes()}
    result = await idx.search(query, params)
    return {doc["id"] for doc in result.docs}


@dfly_args({"proactor_threads": 4})
@pytest.mark.parametrize("index_type", [IndexType.HASH, IndexType.JSON])
@pytest.mark.parametrize("algo_type", ["FLAT", "HNSW"])
async def test_knn(async_client: aioredis.Redis, index_type, algo_type):
    i2 = async_client.ft("i2-" + str(index_type))

    vector_field = VectorField(
        "pos",
        algorithm=algo_type,
        attributes={
            "TYPE": "FLOAT32",
            "DIM": 1,
            "DISTANCE_METRIC": "L2",
            "INITIAL_CAP": 100,
        },
    )

    await i2.create_index(
        fix_schema_naming(index_type, [TagField("even"), vector_field]),
        definition=IndexDefinition(index_type=index_type),
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

    assert await knn_query(i2, "* => [KNN 3 @pos $vec]", [50.0]) == {"k49", "k50", "k51"}

    assert await knn_query(i2, "@even:{yes} => [KNN 3 @pos $vec]", [20.0]) == {"k18", "k20", "k22"}

    assert await knn_query(i2, "@even:{no} => [KNN 4 @pos $vec]", [30.0]) == {
        "k27",
        "k29",
        "k31",
        "k33",
    }

    assert await knn_query(i2, "@even:{yes} => [KNN 3 @pos $vec]", [10.0] == {"k8", "k10", "k12"})
    await i2.dropindex()


NUM_DIMS = 10
NUM_POINTS = 100


@dfly_args({"proactor_threads": 4})
@pytest.mark.parametrize("index_type", [IndexType.HASH, IndexType.JSON])
@pytest.mark.parametrize("algo_type", ["HNSW", "FLAT"])
@pytest.mark.skip("Fails on ARM")
async def test_multidim_knn(async_client: aioredis.Redis, index_type, algo_type):
    vector_field = VectorField(
        "pos",
        algorithm=algo_type,
        attributes={
            "TYPE": "FLOAT32",
            "DIM": NUM_DIMS,
            "DISTANCE_METRIC": "L2",
        },
    )

    i3 = async_client.ft("i3-" + str(index_type))
    await i3.create_index(
        fix_schema_naming(index_type, [vector_field]),
        definition=IndexDefinition(index_type=index_type),
    )

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

        got_ids = await knn_query(i3, f"* => [KNN {limit} @pos $vec]", center)

        assert set(expected_ids) == set(got_ids)

    await i3.dropindex()


@dfly_args({"proactor_threads": 4})
async def test_knn_score_return(async_client: aioredis.Redis):
    i1 = async_client.ft("i1")
    vector_field = VectorField(
        "pos",
        algorithm="FLAT",
        attributes={
            "DIM": 1,
            "DISTANCE_METRIC": "L2",
            "INITIAL_CAP": 100,
        },
    )

    await i1.create_index(
        [vector_field],
        definition=IndexDefinition(index_type=IndexType.HASH),
    )

    pipe = async_client.pipeline()
    for i in range(100):
        pipe.hset(f"k{i}", mapping={"pos": np.array(i, dtype=np.float32).tobytes()})
    await pipe.execute()

    params = {"vec": np.array([1.0], dtype=np.float32).tobytes()}
    result = await i1.search("* => [KNN 3 @pos $vec AS distance]", params)

    assert result.total == 3
    assert [d["distance"] for d in result.docs] == ["0", "1", "1"]

    result = await i1.search(
        Query("* => [KNN 3 @pos $vec AS distance]").return_fields("pos"), params
    )
    assert not any(hasattr(d, "distance") for d in result.docs)

    await i1.dropindex()


@dfly_args({"proactor_threads": 4, "dbfilename": "search-data"})
async def test_index_persistence(df_server):
    client = aioredis.Redis(port=df_server.port)

    # Build two indices and fill them with data

    SCHEMA_1 = [TextField("title"), NumericField("views", sortable=True), TagField("topic")]
    SCHEMA_2 = [
        TextField("name"),
        NumericField("age", sortable=True),
        TagField("job", separator=":", case_sensitive=True),
        VectorField(
            "pos",
            algorithm="HNSW",
            attributes={"TYPE": "FLOAT32", "DIM": 1, "DISTANCE_METRIC": "L2", "INITIAL_CAP": 100},
        ),
    ]

    i1 = client.ft("i1")
    await i1.create_index(
        fix_schema_naming(IndexType.JSON, SCHEMA_1),
        stopwords=["interesting", "stopwords"],
        definition=IndexDefinition(index_type=IndexType.JSON, prefix=["blog-"]),
    )

    i2 = client.ft("i2")
    await i2.create_index(
        fix_schema_naming(IndexType.HASH, SCHEMA_2),
        definition=IndexDefinition(index_type=IndexType.HASH, prefix=["people-"]),
    )

    for i in range(150):
        await client.json().set(
            f"blog-{i}",
            ".",
            {"title": f"Post {i}", "views": i * 10, "topic": "even" if i % 2 == 0 else "odd"},
        )

    for i in range(200):
        await client.hset(
            f"people-{i}",
            mapping={
                "name": f"Name {i}",
                "age": i,
                "job": "newsagent" if i % 2 == 0 else "writer",
                "pos": np.array(i / 200.0).astype(np.float32).tobytes(),
            },
        )

    info_1 = await i1.info()
    info_2 = await i2.info()
    assert info_1["num_docs"] == 150
    assert info_2["num_docs"] == 200

    # stop & start server

    df_server.stop()
    df_server.start()

    client = aioredis.Redis(port=df_server.port)
    await wait_available_async(client)

    # Check indices were loaded

    assert {i.decode() for i in await client.execute_command("FT._LIST")} == {"i1", "i2"}

    i1 = client.ft("i1")
    i2 = client.ft("i2")

    info_1_new = await i1.info()
    info_2_new = await i2.info()

    def build_fields_set(info):
        fields = set()
        for field in info["attributes"]:
            fields.add(tuple(field))
        return fields

    assert build_fields_set(info_1) == build_fields_set(info_1_new)
    assert build_fields_set(info_2) == build_fields_set(info_2_new)

    assert info_1["index_definition"] == info_1_new["index_definition"]
    assert info_2["index_definition"] == info_2_new["index_definition"]

    assert info_1["num_docs"] == info_1_new["num_docs"]
    assert info_2["num_docs"] == info_2_new["num_docs"]

    # Check basic queries run correctly

    assert (await i1.search("@views:[0 90]")).total == 10
    assert (await i1.search("@views:[100 190] @topic:{even}")).total == 5

    assert (await i2.search("@job:{writer}")).total == 100
    assert (await i2.search("@job:{writer} @age:[100 200]")).total == 50
    assert (await i2.search("@job:{wRiTeR}")).total == 0

    # Check fields are sortable
    assert (await i1.search(Query("*").sort_by("views", asc=True).paging(0, 1))).docs[0][
        "id"
    ] == "blog-0"
    assert (await i2.search(Query("*").sort_by("age", asc=False).paging(0, 1))).docs[0][
        "age"
    ] == "199"

    # Check stopwords were loaded
    await client.json().set("blog-sw1", ".", {"title": "some stopwords"})
    assert (await i1.search("some")).total == 1
    assert (await i1.search("stopwords")).total == 0

    await i1.dropindex()
    await i2.dropindex()


@dfly_args({"proactor_threads": 4})
def test_redis_om(df_server):
    try:
        import redis_om
    except ModuleNotFoundError:
        skip_if_not_in_github()
        raise

    client = redis.Redis(port=df_server.port)

    class TestCar(redis_om.HashModel):
        producer: str = redis_om.Field(index=True)
        description: str = redis_om.Field(index=True, full_text_search=True)
        speed: int = redis_om.Field(index=True, sortable=True)

        class Meta:
            database = client

    def extract_producers(testset):
        return sorted([car.producer for car in testset])

    def make_car(producer, description, speed):
        return TestCar(producer=producer, description=description, speed=speed)

    CARS = [
        make_car("BMW", "Very fast and elegant", 200),
        make_car("Audi", "Fast & stylish", 170),
        make_car("Mercedes", "High class but expensive!", 150),
        make_car("Honda", "Good allrounder with flashy looks", 120),
        make_car("Peugeot", "Good allrounder for the whole family", 100),
        make_car("Mini", "Fashinable cooper for the big city", 80),
        make_car("John Deere", "It's not a car, it's a tractor in fact!", 50),
    ]

    for car in CARS:
        car.save()

    redis_om.Migrator().run()

    # Get all cars
    assert extract_producers(TestCar.find().all()) == extract_producers(CARS)

    # Get all cars of a specific producer
    assert extract_producers(
        TestCar.find((TestCar.producer == "Peugeot") | (TestCar.producer == "Mini"))
    ) == ["Mini", "Peugeot"]

    # Get only fast cars
    assert extract_producers(TestCar.find(TestCar.speed >= 150).all()) == extract_producers(
        [c for c in CARS if c.speed >= 150]
    )

    # Get only slow cars
    assert extract_producers(TestCar.find(TestCar.speed < 100).all()) == extract_producers(
        [c for c in CARS if c.speed < 100]
    )

    # Get all cars which are fast based on description
    assert extract_producers(TestCar.find(TestCar.description % "fast")) == ["Audi", "BMW"]

    # Get all cars which are not marked as extensive by descriptions
    assert extract_producers(
        TestCar.find(~(TestCar.description % "expensive")).all()
    ) == extract_producers([c for c in CARS if c.producer != "Mercedes"])

    # Get a fast allrounder
    assert extract_producers(
        TestCar.find((TestCar.speed >= 110) & (TestCar.description % "allrounder"))
    ) == ["Honda"]

    # What's the slowest car
    assert extract_producers([TestCar.find().sort_by("speed").first()]) == ["John Deere"]

    # What's the fastest car
    assert extract_producers([TestCar.find().sort_by("-speed").first()]) == ["BMW"]

    for index in client.execute_command("FT._LIST"):
        client.ft(index.decode()).dropindex()
