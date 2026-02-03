from typing import Dict, Any

import pytest
import redis

from test import testtools


def test_geoadd_ch(r: redis.Redis):
    values = (2.1909389952632, 41.433791470673, "place1")
    assert r.geoadd("a", values) == 1
    values = (
        2.1909389952632,
        31.433791470673,
        "place1",
        2.1873744593677,
        41.406342043777,
        "place2",
    )
    assert r.geoadd("a", values, ch=True) == 2
    assert r.zrange("a", 0, -1) == [b"place1", b"place2"]


def test_geoadd(r: redis.Redis):
    values = (
        2.1909389952632,
        41.433791470673,
        "place1",
        2.1873744593677,
        41.406342043777,
        "place2",
    )
    assert r.geoadd("barcelona", values) == 2
    assert r.zcard("barcelona") == 2

    values = (2.1909389952632, 41.433791470673, "place1")
    assert r.geoadd("a", values) == 1

    with pytest.raises(redis.DataError):
        r.geoadd("barcelona", (1, 2))
    with pytest.raises(redis.DataError):
        r.geoadd("t", values, ch=True, nx=True, xx=True)
    with pytest.raises(redis.ResponseError):
        testtools.raw_command(r, "geoadd", "barcelona", "1", "2")
    with pytest.raises(redis.ResponseError):
        testtools.raw_command(
            r,
            "geoadd",
            "barcelona",
            "nx",
            "xx",
            *values,
        )


def test_geoadd_xx(r: redis.Redis):
    values = (
        2.1909389952632,
        41.433791470673,
        "place1",
        2.1873744593677,
        41.406342043777,
        "place2",
    )
    assert r.geoadd("a", values) == 2
    values = (
        2.1909389952632,
        41.433791470673,
        b"place1",
        2.1873744593677,
        41.406342043777,
        b"place2",
        2.1804738294738,
        41.405647879212,
        b"place3",
    )
    assert r.geoadd("a", values, nx=True) == 1
    assert r.zrange("a", 0, -1) == [b"place3", b"place2", b"place1"]


def test_geohash(r: redis.Redis):
    values = (
        2.1909389952632,
        41.433791470673,
        "place1",
        2.1873744593677,
        41.406342043777,
        "place2",
    )
    r.geoadd("barcelona", values)
    assert r.geohash("barcelona", "place1", "place2", "place3") == [
        "sp3e9yg3kd0",
        "sp3e9cbc3t0",
        None,
    ]


def test_geopos(r: redis.Redis):
    values = (
        2.1909389952632,
        41.433791470673,
        "place1",
        2.1873744593677,
        41.406342043777,
        "place2",
    )
    r.geoadd("barcelona", values)
    # small errors may be introduced.
    assert r.geopos("barcelona", "place1", "place4", "place2") == [
        pytest.approx((2.1909389952632, 41.433791470673), 0.00001),
        None,
        pytest.approx((2.1873744593677, 41.406342043777), 0.00001),
    ]


def test_geodist(r: redis.Redis):
    values = (
        2.1909389952632,
        41.433791470673,
        "place1",
        2.1873744593677,
        41.406342043777,
        "place2",
    )
    assert r.geoadd("barcelona", values) == 2
    assert r.geodist("barcelona", "place1", "place2") == pytest.approx(
        3067.4157, 0.0001
    )


def test_geodist_units(r: redis.Redis):
    values = (
        2.1909389952632,
        41.433791470673,
        "place1",
        2.1873744593677,
        41.406342043777,
        "place2",
    )
    r.geoadd("barcelona", values)
    assert r.geodist("barcelona", "place1", "place2", "km") == pytest.approx(
        3.0674, 0.0001
    )
    assert r.geodist("barcelona", "place1", "place2", "mi") == pytest.approx(
        1.906, 0.0001
    )
    assert r.geodist("barcelona", "place1", "place2", "ft") == pytest.approx(
        10063.6998, 0.0001
    )
    with pytest.raises(redis.RedisError):
        assert r.geodist("x", "y", "z", "inches")


def test_geodist_missing_one_member(r: redis.Redis):
    values = (2.1909389952632, 41.433791470673, "place1")
    r.geoadd("barcelona", values)
    assert r.geodist("barcelona", "place1", "missing_member", "km") is None


@pytest.mark.unsupported_server_types("dragonfly")
@pytest.mark.parametrize(
    "long,lat,radius,extra,expected",
    [
        (2.191, 41.433, 1000, {}, [b"place1"]),
        (2.187, 41.406, 1000, {}, [b"place2"]),
        (1, 2, 1000, {}, []),
        (2.191, 41.433, 1, {"unit": "km"}, [b"place1"]),
        (2.191, 41.433, 3000, {"count": 1}, [b"place1"]),
    ],
)
def test_georadius(
    r: redis.Redis,
    long: float,
    lat: float,
    radius: float,
    extra: Dict[str, Any],
    expected,
):
    values = (
        2.1909389952632,
        41.433791470673,
        "place1",
        2.1873744593677,
        41.406342043777,
        "place2",
    )
    r.geoadd("barcelona", values)
    assert r.georadius("barcelona", long, lat, radius, **extra) == expected


@pytest.mark.unsupported_server_types("dragonfly")
@pytest.mark.parametrize(
    "member,radius,extra,expected",
    [
        ("place1", 1000, {}, [b"place1"]),
        ("place2", 1000, {}, [b"place2"]),
        ("place1", 1, {"unit": "km"}, [b"place1"]),
        ("place1", 3000, {"count": 1}, [b"place1"]),
    ],
)
def test_georadiusbymember(
    r: redis.Redis, member: str, radius: float, extra: Dict[str, Any], expected
):
    values = (
        2.1909389952632,
        41.433791470673,
        "place1",
        2.1873744593677,
        41.406342043777,
        b"place2",
    )
    r.geoadd("barcelona", values)
    assert r.georadiusbymember("barcelona", member, radius, **extra) == expected
    assert r.georadiusbymember(
        "barcelona", member, radius, **extra, store_dist="extract"
    ) == len(expected)
    assert r.zcard("extract") == len(expected)


@pytest.mark.unsupported_server_types("dragonfly")
def test_georadius_with(r: redis.Redis):
    values = (
        2.1909389952632,
        41.433791470673,
        "place1",
        2.1873744593677,
        41.406342043777,
        "place2",
    )

    r.geoadd("barcelona", values)
    # test a bunch of combinations to test the parse response function.
    res = r.georadius(
        "barcelona",
        2.191,
        41.433,
        1,
        unit="km",
        withdist=True,
        withcoord=True,
    )
    assert res == [
        pytest.approx(
            [b"place1", 0.0881, pytest.approx((2.1909, 41.4337), 0.0001)], 0.001
        )
    ]

    res = r.georadius(
        "barcelona", 2.191, 41.433, 1, unit="km", withdist=True, withcoord=True
    )
    assert res == [
        pytest.approx(
            [b"place1", 0.0881, pytest.approx((2.1909, 41.4337), 0.0001)], 0.001
        )
    ]

    res = r.georadius("barcelona", 2.191, 41.433, 1, unit="km", withcoord=True)
    assert res == [[b"place1", pytest.approx((2.1909, 41.4337), 0.0001)]]

    # test no values.
    assert (
        r.georadius(
            "barcelona",
            2,
            1,
            1,
            unit="km",
            withdist=True,
            withcoord=True,
        )
        == []
    )


@pytest.mark.unsupported_server_types("dragonfly")
def test_georadius_count(r: redis.Redis):
    values = (
        2.1909389952632,
        41.433791470673,
        "place1",
        2.1873744593677,
        41.406342043777,
        "place2",
    )

    r.geoadd("barcelona", values)

    assert (
        r.georadius("barcelona", 2.191, 41.433, 3000, count=1, store="barcelona") == 1
    )
    assert r.georadius("barcelona", 2.191, 41.433, 3000, store_dist="extract") == 1
    assert r.zcard("extract") == 1
    res = r.georadius("barcelona", 2.191, 41.433, 3000, count=1, any=True)
    assert (res == [b"place2"]) or res == [b"place1"]

    values = (
        13.361389,
        38.115556,
        "Palermo",
        15.087269,
        37.502669,
        "Catania",
    )

    r.geoadd("Sicily", values)
    assert (
        testtools.raw_command(
            r,
            "GEORADIUS",
            "Sicily",
            "15",
            "37",
            "200",
            "km",
            "STOREDIST",
            "neardist",
            "STORE",
            "near",
        )
        == 2
    )
    assert r.zcard("near") == 2
    assert r.zcard("neardist") == 0


def test_georadius_errors(r: redis.Redis):
    values = (
        13.361389,
        38.115556,
        "Palermo",
        15.087269,
        37.502669,
        "Catania",
    )

    r.geoadd("Sicily", values)

    with pytest.raises(redis.DataError):  # Unsupported unit
        r.georadius("barcelona", 2.191, 41.433, 3000, unit="dsf")
    with pytest.raises(redis.ResponseError):  # Unsupported unit
        testtools.raw_command(
            r,
            "GEORADIUS",
            "Sicily",
            "15",
            "37",
            "200",
            "ddds",
            "STOREDIST",
            "neardist",
            "STORE",
            "near",
        )

    bad_values = (
        13.361389,
        38.115556,
        "Palermo",
        15.087269,
        "Catania",
    )
    with pytest.raises(redis.DataError):
        r.geoadd("newgroup", bad_values)
    with pytest.raises(redis.ResponseError):
        testtools.raw_command(r, "geoadd", "newgroup", *bad_values)


@pytest.mark.unsupported_server_types("dragonfly")
def test_geosearch(r: redis.Redis):
    values = (
        2.1909389952632,
        41.433791470673,
        b"place1",
        2.1873744593677,
        41.406342043777,
        b"place2",
        2.583333,
        41.316667,
        b"place3",
    )
    r.geoadd("barcelona", values)
    assert r.geosearch("barcelona", longitude=2.191, latitude=41.433, radius=1000) == [
        b"place1"
    ]
    assert r.geosearch("barcelona", longitude=2.187, latitude=41.406, radius=1000) == [
        b"place2"
    ]
    # assert r.geosearch("barcelona", longitude=2.191, latitude=41.433, height=1000, width=1000) == [b"place1"]
    assert set(r.geosearch("barcelona", member="place3", radius=100, unit="km")) == {
        b"place2",
        b"place1",
        b"place3",
    }
    # test count
    assert r.geosearch(
        "barcelona", member="place3", radius=100, unit="km", count=2
    ) == [
        b"place3",
        b"place2",
    ]
    assert r.geosearch(
        "barcelona", member="place3", radius=100, unit="km", count=1, any=True
    )[0] in [
        b"place1",
        b"place3",
        b"place2",
    ]
