from math import inf

import pytest
import redis

topk_tests = pytest.importorskip("probables")
pytestmark = []
pytestmark.extend(
    [
        pytest.mark.unsupported_server_types("dragonfly"),
    ]
)


def test_tdigest_reset(r: redis.Redis):
    assert r.tdigest().create("tDigest", 10)
    # reset on empty histogram
    assert r.tdigest().reset("tDigest")
    # insert data-points into sketch
    assert r.tdigest().add("tDigest", list(range(10)))

    assert r.tdigest().reset("tDigest")
    # assert we have 0 unmerged
    info = r.tdigest().info("tDigest")
    assert 0 == info.get("unmerged_weight")


def test_tdigest_merge(r: redis.Redis):
    assert r.tdigest().create("to-tDigest", 10)
    assert r.tdigest().create("from-tDigest", 10)
    # insert data-points into sketch
    assert r.tdigest().add("from-tDigest", [1.0] * 10)
    assert r.tdigest().add("to-tDigest", [2.0] * 10)
    # merge from-tdigest into to-tdigest
    assert r.tdigest().merge("to-tDigest", 1, "from-tDigest")
    # we should now have 110 weight on to-histogram
    info = r.tdigest().info("to-tDigest")
    assert 20 == float(info["merged_weight"]) + float(info["unmerged_weight"])
    # test override
    assert r.tdigest().create("from-override", 10)
    assert r.tdigest().create("from-override-2", 10)
    assert r.tdigest().add("from-override", [3.0] * 10)
    assert r.tdigest().add("from-override-2", [4.0] * 10)
    assert r.tdigest().merge(
        "to-tDigest", 2, "from-override", "from-override-2", override=True
    )
    assert 3.0 == r.tdigest().min("to-tDigest")
    assert 4.0 == r.tdigest().max("to-tDigest")


def test_tdigest_min_and_max(r: redis.Redis):
    assert r.tdigest().create("tDigest", 100)
    # insert data-points into sketch
    assert r.tdigest().add("tDigest", [1, 2, 3])
    # min/max
    assert 3 == r.tdigest().max("tDigest")
    assert 1 == r.tdigest().min("tDigest")


def test_tdigest_quantile(r: redis.Redis):
    assert r.tdigest().create("tDigest", 500)
    # insert data-points into sketch
    assert r.tdigest().add("tDigest", list([x * 0.01 for x in range(1, 10000)]))
    # assert min min/max have same result as quantile 0 and 1
    res = r.tdigest().quantile("tDigest", 1.0)
    assert r.tdigest().max("tDigest") == res[0]
    res = r.tdigest().quantile("tDigest", 0.0)
    assert r.tdigest().min("tDigest") == res[0]

    assert 1.0 == round(r.tdigest().quantile("tDigest", 0.01)[0], 2)
    assert 99.0 == round(r.tdigest().quantile("tDigest", 0.99)[0], 2)

    # test multiple quantiles
    assert r.tdigest().create("t-digest", 100)
    assert r.tdigest().add("t-digest", [1, 2, 3, 4, 5])
    assert [3.0, 5.0] == r.tdigest().quantile("t-digest", 0.5, 0.8)


def test_tdigest_cdf(r: redis.Redis):
    assert r.tdigest().create("tDigest", 100)
    # insert data-points into sketch
    assert r.tdigest().add("tDigest", list(range(1, 10)))
    assert 0.1 == round(r.tdigest().cdf("tDigest", 1.0)[0], 1)
    assert 0.9 == round(r.tdigest().cdf("tDigest", 9.0)[0], 1)
    res = r.tdigest().cdf("tDigest", 1.0, 9.0)
    assert [0.1, 0.9] == [round(x, 1) for x in res]


def test_tdigest_trimmed_mean(r: redis.Redis):
    assert r.tdigest().create("tDigest", 100)
    # insert data-points into sketch
    assert r.tdigest().add("tDigest", list(range(1, 10)))
    assert 5 == r.tdigest().trimmed_mean("tDigest", 0.1, 0.9)
    assert 4.5 == r.tdigest().trimmed_mean("tDigest", 0.4, 0.5)


def test_tdigest_rank(r: redis.Redis):
    assert r.tdigest().create("t-digest", 500)
    assert r.tdigest().add("t-digest", list(range(0, 20)))
    assert -1 == r.tdigest().rank("t-digest", -1)[0]
    assert 0 == r.tdigest().rank("t-digest", 0)[0]
    assert 10 == r.tdigest().rank("t-digest", 10)[0]
    assert [-1, 20, 9] == r.tdigest().rank("t-digest", -20, 20, 9)


def test_tdigest_revrank(r: redis.Redis):
    assert r.tdigest().create("t-digest", 500)
    assert r.tdigest().add("t-digest", list(range(0, 20)))
    assert -1 == r.tdigest().revrank("t-digest", 20)[0]
    assert 19 == r.tdigest().revrank("t-digest", 0)[0]
    assert [-1, 19, 9] == r.tdigest().revrank("t-digest", 21, 0, 10)


def test_tdigest_byrank(r: redis.Redis):
    assert r.tdigest().create("t-digest", 500)
    assert r.tdigest().add("t-digest", list(range(1, 11)))
    assert 1 == r.tdigest().byrank("t-digest", 0)[0]
    assert 10 == r.tdigest().byrank("t-digest", 9)[0]
    assert r.tdigest().byrank("t-digest", 100)[0] == inf
    with pytest.raises(redis.ResponseError):
        r.tdigest().byrank("t-digest", -1)[0]


def test_tdigest_byrevrank(r: redis.Redis):
    assert r.tdigest().create("t-digest", 500)
    assert r.tdigest().add("t-digest", list(range(1, 11)))
    assert 10 == r.tdigest().byrevrank("t-digest", 0)[0]
    assert 1 == r.tdigest().byrevrank("t-digest", 9)[0]
    assert r.tdigest().byrevrank("t-digest", 100)[0] == -inf
    with pytest.raises(redis.ResponseError):
        r.tdigest().byrevrank("t-digest", -1)[0]
