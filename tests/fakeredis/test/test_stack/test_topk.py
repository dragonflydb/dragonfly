import pytest
import redis

topk_tests = pytest.importorskip("probables")

pytestmark = []
pytestmark.extend(
    [
        pytest.mark.unsupported_server_types("dragonfly"),
    ]
)


def test_topk_incrby(r: redis.Redis):
    assert r.topk().reserve("topk", 3, 10, 3, 1)
    assert [None, None, None] == r.topk().incrby(
        "topk", ["bar", "baz", "42"], [3, 6, 2]
    )
    assert [None, "bar"] == r.topk().incrby("topk", ["42", "xyzzy"], [8, 4])
    with pytest.deprecated_call():
        assert [3, 6, 10, 4, 0] == r.topk().count(
            "topk", "bar", "baz", "42", "xyzzy", 4
        )


def test_topk(r: redis.Redis):
    # test list with empty buckets
    assert r.topk().reserve("topk", 3, 50, 4, 0.9)
    ret = r.topk().add(
        "topk",
        "A",
        "B",
        "C",
        "D",
        "D",
        "E",
        "A",
        "A",
        "B",
        "C",
        "G",
        "D",
        "B",
        "D",
        "A",
        "E",
        "E",
        1,
    )
    assert len(ret) == 18

    with pytest.deprecated_call():
        assert r.topk().count("topk", "A", "B", "C", "D", "E", "F", "G") == [
            4,
            3,
            2,
            4,
            3,
            0,
            1,
        ]
    ret = r.topk().query("topk", "A", "B", "C", "D", "E", "F", "G")
    assert (ret == [1, 0, 0, 1, 1, 0, 0]) or (ret == [1, 1, 0, 1, 0, 0, 0])
    # test full list
    assert r.topk().reserve("topklist", 3, 50, 3, 0.9)
    assert r.topk().add(
        "topklist",
        "A",
        "B",
        "D",
        "E",
        "A",
        "A",
        "B",
        "C",
        "G",
        "D",
        "B",
        "A",
        "B",
        "E",
        "E",
    )
    with pytest.deprecated_call():
        assert r.topk().count("topklist", "A", "B", "C", "D", "E", "F", "G") == [
            4,
            4,
            1,
            2,
            3,
            0,
            1,
        ]
    assert r.topk().list("topklist") == ["A", "B", "E"]
    assert r.topk().list("topklist", withcount=True) == ["A", 4, "B", 4, "E", 3]
    info = r.topk().info("topklist")
    assert 3 == info["k"]
    assert 50 == info["width"]
    assert 3 == info["depth"]
    assert 0.9 == round(float(info["decay"]), 1)
