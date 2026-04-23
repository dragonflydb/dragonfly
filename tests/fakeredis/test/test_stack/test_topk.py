import pytest
import redis
import math
import time

topk_tests = pytest.importorskip("probables")
pytestmark = []


def _is_real_server(r: redis.Redis) -> bool:
    """Returns True for real Redis/Dragonfly, False for FakeRedis."""
    return "Fake" not in type(r).__name__


def test_topk_reserve_and_lifecycle(r: redis.Redis):
    """
    Tests TOPK.RESERVE parameter validation (including boundary acceptance and NaN/Inf rejection),
    WRONGTYPE collisions with existing keys, and sketch deletion via the standard DEL command.
    """
    assert r.topk().reserve("tk_default", 5, 8, 7, 0.9)
    assert r.topk().reserve("tk_bounds", 5, 100, 7, 1.0)  # decay=1.0 is valid
    assert r.topk().reserve("tk_bounds2", 5, 100, 7, 0.0)  # decay=0.0 is valid
    assert r.execute_command(
        "TOPK.RESERVE", "tk_defaults_cpp", 5
    )  # create with defaults for optional parameters

    # Parameter validation — Dragonfly enforces these; FakeRedis is more permissive
    if _is_real_server(r):
        with pytest.raises(redis.exceptions.ResponseError):
            r.topk().reserve("err", 0, 8, 7, 0.9)
        with pytest.raises(redis.exceptions.ResponseError):
            r.topk().reserve("err", 5, 0, 7, 0.9)
        with pytest.raises(redis.exceptions.ResponseError):
            r.topk().reserve("err", 5, 100, 7, 1.5)
        with pytest.raises(redis.exceptions.ResponseError):
            r.topk().reserve("err", 5, 100, 7, math.nan)
        with pytest.raises(redis.exceptions.ResponseError):
            r.topk().reserve("err", 5, 100, 7, math.inf)

    r.set("mystr", "value")
    with pytest.raises(redis.exceptions.ResponseError):
        r.topk().reserve("mystr", 5, 8, 7, 0.9)
    with pytest.raises(redis.exceptions.ResponseError):
        r.topk().add("mystr", "foo")

    r.topk().reserve("to_delete", 5, 8, 7, 0.9)
    assert r.delete("to_delete") == 1
    with pytest.raises(redis.exceptions.ResponseError):
        r.topk().query("to_delete", "foo")


def test_topk_add_and_exact_eviction(r: redis.Redis):
    """
    Tests the HeavyKeeper eviction mechanics. Verifies that additions below the
    threshold return None, while overpowering existing heap items correctly
    returns the exact evicted string.
    """
    # Disable probabilistic decay and minimize collisions to ensure
    # deterministic heap eviction for CI stability.
    r.topk().reserve("tk", 2, 2000, 7, 0.0)

    assert r.topk().incrby("tk", ["heavy1", "heavy2"], [1000, 500]) == [None, None]

    # heavy2 has a strictly smaller count than heavy1 and will be the minimum element
    # in the heap, so it will be deterministically evicted when 'king' enters.
    res = r.topk().incrby("tk", ["king"], [5000])
    assert res in ([b"heavy2"], ["heavy2"])


def test_topk_binary_safety(r: redis.Redis):
    """
    Ensures the C++ and Python boundaries correctly handle binary-safe strings
    containing null bytes and non-printable characters without truncating them.
    """
    r.topk().reserve("tk", 5, 8, 7, 0.9)
    binary_str = b"foo\x00bar\xffbaz"

    assert r.topk().add("tk", binary_str) == [None]
    assert r.topk().query("tk", binary_str) == [1]


def test_topk_query_and_count(r: redis.Redis):
    """
    Tests TOPK.QUERY for deterministic heap presence (1 or 0) and
    TOPK.COUNT for exact frequency estimation by using a massive width to prevent collisions.
    """
    # width=2000 guarantees no collisions for 3 items.
    # We assert exact counts instead of estimates because the math makes collisions practically impossible.
    r.topk().reserve("tk", 5, 2000, 7, 0.9)
    r.topk().incrby("tk", ["alpha", "beta"], [100, 50])

    assert r.topk().query("tk", "alpha", "beta", "gamma") == [1, 1, 0]

    with pytest.deprecated_call():
        counts = r.topk().count("tk", "alpha", "beta", "gamma")

    if _is_real_server(r):
        assert counts[0] == 100
        assert counts[1] == 50
        assert counts[2] == 0


def test_topk_list_and_sorting(r: redis.Redis):
    """
    Tests TOPK.LIST output formatting and verifies the deterministic lexicographical
    tie-breaker mechanism by using a large width to prevent count decay.
    """
    # width=2000 guarantees counts stay exactly at 10, forcing the lexicographical tie-breaker.
    r.topk().reserve("tk", 3, 2000, 7, 0.9)
    assert r.topk().list("tk") == []

    r.topk().incrby("tk", ["apple", "zebra", "banana"], [10, 10, 10])

    items_with_count = r.topk().list("tk", withcount=True)

    if isinstance(items_with_count, dict):
        parsed = {
            (k.decode() if isinstance(k, bytes) else k): int(v) for k, v in items_with_count.items()
        }
        keys = list(parsed.keys())
    else:
        parsed = {}
        keys = []
        for i in range(0, len(items_with_count), 2):
            k = items_with_count[i]
            k = k.decode() if isinstance(k, bytes) else k
            keys.append(k)
            parsed[k] = int(items_with_count[i + 1])

    if _is_real_server(r):
        # Assert exact counts
        for name, count in parsed.items():
            assert count == 10, f"{name} count decayed unexpectedly to {count}"

        # Assert exact lexicographical order: apple < banana < zebra
        assert keys[0] == "apple"
        assert keys[1] == "banana"
        assert keys[2] == "zebra"


def test_topk_info(r: redis.Redis):
    """
    Tests that TOPK.INFO accurately returns the configured dimensional and
    probabilistic parameters of the sketch.
    """
    r.topk().reserve("tk_info", 10, 100, 7, 0.95)
    info = r.topk().info("tk_info")

    assert info["k"] == 10
    assert info["width"] == 100
    assert info["depth"] == 7
    assert float(info["decay"]) == pytest.approx(0.95, abs=1e-6)


def test_topk_double_reserve(r: redis.Redis):
    """TOPK.RESERVE on an existing topk key must fail."""
    r.topk().reserve("tk_dup", 5, 8, 7, 0.9)
    with pytest.raises(redis.exceptions.ResponseError):
        r.topk().reserve("tk_dup", 10, 8, 7, 0.9)


def test_topk_k_equals_one(r: redis.Redis):
    """k=1 is the minimal heap — only the single heaviest item survives."""
    r.topk().reserve("tk1", 1, 50, 7, 0.9)
    # Add dominant first so it occupies the single heap slot with a large count,
    # then add rare which cannot evict it.
    r.topk().incrby("tk1", ["dominant"], [1000])
    r.topk().incrby("tk1", ["rare"], [1])
    result = r.topk().list("tk1")
    assert len(result) == 1
    val = result[0].decode() if isinstance(result[0], bytes) else result[0]
    assert val == "dominant"


def test_topk_negative_incrby(r: redis.Redis):
    """Negative increments must be rejected with an error."""
    if not _is_real_server(r):
        pytest.skip("FakeRedis does not validate negative increments")
    r.topk().reserve("tk_neg", 5, 8, 7, 0.9)
    with pytest.raises(redis.exceptions.ResponseError):
        r.topk().incrby("tk_neg", ["item"], [-1])


def test_topk_list_nonexistent_key(r: redis.Redis):
    """LIST on a non-existent key should raise an error (key not found)."""
    with pytest.raises(redis.exceptions.ResponseError):
        r.topk().list("does_not_exist")


def test_topk_type_command(r: redis.Redis):
    """TYPE should report the correct type for a topk key."""
    if not _is_real_server(r):
        pytest.skip("FakeRedis TYPE does not support topk objects")
    r.topk().reserve("tk_type", 5, 8, 7, 0.9)
    key_type = r.type("tk_type")
    t = key_type.decode() if isinstance(key_type, bytes) else key_type
    assert t in ("MBbloom--", "TopK-TYPE", "topk"), f"Unexpected TYPE: {t}"


def test_topk_rename(r: redis.Redis):
    """RENAME must preserve the topk sketch intact."""
    r.topk().reserve("tk_src", 3, 8, 7, 0.9)
    r.topk().incrby("tk_src", ["a", "b", "c"], [10, 20, 30])
    r.rename("tk_src", "tk_dst")

    result = r.topk().list("tk_dst")
    vals = {(v.decode() if isinstance(v, bytes) else v) for v in result}
    assert "c" in vals
    assert "b" in vals
    assert "a" in vals
    assert len(vals) == 3
    assert r.topk().query("tk_dst", "a", "b", "c") == [1, 1, 1]


def test_topk_expire(r: redis.Redis):
    """TTL/EXPIRE must work on topk keys like any other key."""
    r.topk().reserve("tk_ttl", 5, 8, 7, 0.9)

    # Test that setting a long TTL works and reports correctly
    assert r.expire("tk_ttl", 3600)
    ttl = r.ttl("tk_ttl")
    assert 0 < ttl <= 3600

    # Test actual expiration mechanics, verify the sketch is completely gone
    assert r.pexpire("tk_ttl", 100)

    # Poll for up to 2 seconds to allow CI jitter
    for _ in range(20):
        if r.exists("tk_ttl") == 0:
            break
        time.sleep(0.1)
    assert r.exists("tk_ttl") == 0

    # Verify commands fail correctly on the expired key
    with pytest.raises(redis.exceptions.ResponseError):
        r.topk().info("tk_ttl")


def test_topk_add_many_items(r: redis.Redis):
    """Stress test: add a large batch to prove no O(n^2) blowup or crash."""
    r.topk().reserve("tk_bulk", 10, 200, 7, 0.9)
    items = [f"item_{i}" for i in range(5000)]
    # Pipeline for speed
    pipe = r.pipeline()
    for item in items:
        pipe.execute_command("TOPK.ADD", "tk_bulk", item)
    pipe.execute()

    result = r.topk().list("tk_bulk")
    assert len(result) == 10


def test_topk_zero_increment(r: redis.Redis):
    """An increment of 0 should be rejected."""
    if not _is_real_server(r):
        pytest.skip("FakeRedis may not validate 0 increments")
    r.topk().reserve("tk_zero", 5, 8, 7, 0.9)
    with pytest.raises(redis.exceptions.ResponseError):
        r.topk().incrby("tk_zero", ["foo"], [0])


def test_topk_incrby_arity_mismatch(r: redis.Redis):
    """INCRBY must fail if item/increment counts don't match."""
    r.topk().reserve("tk_arity", 5, 8, 7, 0.9)
    with pytest.raises(redis.exceptions.ResponseError):
        r.execute_command("TOPK.INCRBY", "tk_arity", "item1", "item2", "100")


def test_topk_commands_on_missing_key(r: redis.Redis):
    """QUERY, COUNT, and INFO should error on non-existent keys."""
    r.delete("missing_tk")

    with pytest.raises(redis.exceptions.ResponseError):
        r.topk().info("missing_tk")
    with pytest.raises(redis.exceptions.ResponseError):
        r.topk().query("missing_tk", "foo")
    with pytest.deprecated_call():
        with pytest.raises(redis.exceptions.ResponseError):
            r.topk().count("missing_tk", "foo")
