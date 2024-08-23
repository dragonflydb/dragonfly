import pytest
import redis.commands.bf
from redis.commands.bf import BFInfo

json_tests = pytest.importorskip("probables")


def intlist(obj):
    return [int(v) for v in obj]


def test_create_bf(r: redis.Redis):
    """Test CREATE/RESERVE calls"""
    assert r.bf().create("bloom", 0.01, 1000)
    assert r.bf().create("bloom_e", 0.01, 1000, expansion=1)
    assert r.bf().create("bloom_ns", 0.01, 1000, noScale=True)


@pytest.mark.unsupported_server_types("dragonfly")
def test_create_cf(r: redis.Redis):
    assert r.cf().create("cuckoo", 1000)
    assert r.cf().create("cuckoo_e", 1000, expansion=1)
    assert r.cf().create("cuckoo_bs", 1000, bucket_size=4)
    assert r.cf().create("cuckoo_mi", 1000, max_iterations=10)
    assert r.cms().initbydim("cmsDim", 100, 5)
    assert r.cms().initbyprob("cmsProb", 0.01, 0.01)
    assert r.topk().reserve("topk", 5, 100, 5, 0.9)


def test_bf_reserve(r: redis.Redis):
    """Testing BF.RESERVE"""
    assert r.bf().reserve("bloom", 0.01, 1000)
    assert r.bf().reserve("bloom_e", 0.01, 1000, expansion=1)
    assert r.bf().reserve("bloom_ns", 0.01, 1000, noScale=True)


def test_bf_add(r: redis.Redis):
    assert r.bf().create("bloom", 0.01, 1000)
    assert 1 == r.bf().add("bloom", "foo")
    assert 0 == r.bf().add("bloom", "foo")
    assert [0] == intlist(r.bf().madd("bloom", "foo"))
    assert [0, 1] == r.bf().madd("bloom", "foo", "bar")
    assert [0, 0, 1] == r.bf().madd("bloom", "foo", "bar", "baz")
    assert 1 == r.bf().exists("bloom", "foo")
    assert 0 == r.bf().exists("bloom", "noexist")
    assert [1, 0] == intlist(r.bf().mexists("bloom", "foo", "noexist"))


@pytest.mark.unsupported_server_types("dragonfly")
def test_bf_insert(r: redis.Redis):
    assert r.bf().create("bloom", 0.01, 1000)
    assert [1] == intlist(r.bf().insert("bloom", ["foo"]))
    assert [0, 1] == intlist(r.bf().insert("bloom", ["foo", "bar"]))
    assert [1] == intlist(r.bf().insert("captest", ["foo"], capacity=10))
    assert [1] == intlist(r.bf().insert("errtest", ["foo"], error=0.01))
    assert 1 == r.bf().exists("bloom", "foo")
    assert 0 == r.bf().exists("bloom", "noexist")
    assert [1, 0] == intlist(r.bf().mexists("bloom", "foo", "noexist"))
    info = r.bf().info("bloom")
    assert 2 == info.get("insertedNum")
    assert 1000 == info.get("capacity")
    assert 1 == info.get("filterNum")


@pytest.mark.unsupported_server_types("dragonfly")
def test_bf_scandump_and_loadchunk(r: redis.Redis):
    r.bf().create("myBloom", "0.0001", "1000")

    # Test is probabilistic and might fail. It is OK to change variables if
    # certain to not break anything

    res = 0
    for x in range(1000):
        r.bf().add("myBloom", x)
        assert r.bf().exists("myBloom", x)
        rv = r.bf().exists("myBloom", f"nonexist_{x}")
        res += rv == x
    assert res < 5

    cmds = list()
    first = 0
    while first is not None:
        cur = r.bf().scandump("myBloom", first)
        if cur[0] == 0:
            first = None
        else:
            first = cur[0]
            cmds.append(cur)

    # Remove the filter
    r.bf().client.delete("myBloom")

    # Now, load all the commands:
    for cmd in cmds:
        r.bf().loadchunk("myBloom1", *cmd)

    for x in range(1000):
        assert r.bf().exists("myBloom1", x), f"{x} not in filter"


@pytest.mark.unsupported_server_types("dragonfly")
def test_bf_info(r: redis.Redis):
    # Store a filter
    r.bf().create("nonscaling", "0.0001", "1000", noScale=True)
    info: BFInfo = r.bf().info("nonscaling")
    assert info.expansionRate is None

    expansion = 4
    r.bf().create("expanding", "0.0001", "1000", expansion=expansion)
    info = r.bf().info("expanding")
    assert info.expansionRate == 4
    assert info.capacity == 1000
    assert info.insertedNum == 0


@pytest.mark.unsupported_server_types("dragonfly")
def test_bf_card(r: redis.Redis):
    # return 0 if the key does not exist
    assert r.bf().card("not_exist") == 0

    # Store a filter
    assert r.bf().add("bf1", "item_foo") == 1
    assert r.bf().card("bf1") == 1

    # Error when key is of a type other than Bloom filter.
    with pytest.raises(redis.ResponseError):
        r.set("setKey", "value")
        r.bf().card("setKey")
