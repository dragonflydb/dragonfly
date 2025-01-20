import pytest
import redis

cuckoofilters_tests = pytest.importorskip("probables")

topk_tests = pytest.importorskip("probables")

pytestmark = []
pytestmark.extend(
    [
        pytest.mark.unsupported_server_types("dragonfly"),
    ]
)


def test_cf_add_and_insert(r: redis.Redis):
    assert r.cf().create("cuckoo", 1000)
    assert r.cf().add("cuckoo", "filter")
    assert not r.cf().addnx("cuckoo", "filter")
    assert 1 == r.cf().addnx("cuckoo", "newItem")
    assert [1] == r.cf().insert("captest", ["foo"])
    assert [1] == r.cf().insert("captest", ["foo"], capacity=1000)
    assert [1] == r.cf().insertnx("captest", ["bar"])
    assert [1] == r.cf().insertnx("captest", ["food"], nocreate="1")
    assert [0, 0, 1] == r.cf().insertnx("captest", ["foo", "bar", "baz"])
    assert [0] == r.cf().insertnx("captest", ["bar"], capacity=1000)
    assert [1] == r.cf().insert("empty1", ["foo"], capacity=1000)
    assert [1] == r.cf().insertnx("empty2", ["bar"], capacity=1000)
    info = r.cf().info("captest")
    assert info.get("insertedNum") == 5
    assert info.get("deletedNum") == 0
    assert info.get("filterNum") == 1


def test_create_cf(r: redis.Redis):
    assert r.cf().create("cuckoo", 1000)
    assert r.cf().create("cuckoo_e", 1000, expansion=1)
    assert r.cf().create("cuckoo_bs", 1000, bucket_size=4)
    assert r.cf().create("cuckoo_mi", 1000, max_iterations=10)
    assert r.cms().initbydim("cmsDim", 100, 5)
    assert r.cms().initbyprob("cmsProb", 0.01, 0.01)
    assert r.topk().reserve("topk", 5, 100, 5, 0.9)


def test_cf_exists_and_del(r: redis.Redis):
    assert r.cf().create("cuckoo", 1000)
    assert r.cf().add("cuckoo", "filter")
    assert r.cf().exists("cuckoo", "filter")
    assert not r.cf().exists("cuckoo", "notexist")
    assert [1, 0] == r.cf().mexists("cuckoo", "filter", "notexist")
    assert 1 == r.cf().count("cuckoo", "filter")
    assert 0 == r.cf().count("cuckoo", "notexist")
    assert r.cf().delete("cuckoo", "filter")
    assert 0 == r.cf().count("cuckoo", "filter")
