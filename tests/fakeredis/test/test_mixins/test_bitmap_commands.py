import pytest
import redis
import redis.client

from test.testtools import raw_command


def test_getbit(r: redis.Redis):
    r.setbit("foo", 3, 1)
    assert r.getbit("foo", 0) == 0
    assert r.getbit("foo", 1) == 0
    assert r.getbit("foo", 2) == 0
    assert r.getbit("foo", 3) == 1
    assert r.getbit("foo", 4) == 0
    assert r.getbit("foo", 100) == 0


def test_getbit_wrong_type(r: redis.Redis):
    r.rpush("foo", b"x")
    with pytest.raises(redis.ResponseError):
        r.getbit("foo", 1)


@pytest.mark.min_server("7")
def test_bitcount_error(r: redis.Redis):
    with pytest.raises(redis.ResponseError) as e:
        raw_command(r, b"BITCOUNT", b"", b"", b"")
    assert str(e.value) == "value is not an integer or out of range"


@pytest.mark.min_server("7")
def test_bitcount_does_not_exist(r: redis.Redis):
    res = raw_command(r, b"BITCOUNT", b"", 0, 0)
    assert res == 0


def test_multiple_bits_set(r: redis.Redis):
    r.setbit("foo", 1, 1)
    r.setbit("foo", 3, 1)
    r.setbit("foo", 5, 1)

    assert r.getbit("foo", 0) == 0
    assert r.getbit("foo", 1) == 1
    assert r.getbit("foo", 2) == 0
    assert r.getbit("foo", 3) == 1
    assert r.getbit("foo", 4) == 0
    assert r.getbit("foo", 5) == 1
    assert r.getbit("foo", 6) == 0


def test_unset_bits(r: redis.Redis):
    r.setbit("foo", 1, 1)
    r.setbit("foo", 2, 0)
    r.setbit("foo", 3, 1)
    assert r.getbit("foo", 1) == 1
    r.setbit("foo", 1, 0)
    assert r.getbit("foo", 1) == 0
    r.setbit("foo", 3, 0)
    assert r.getbit("foo", 3) == 0


def test_get_set_bits(r: redis.Redis):
    # set bit 5
    assert not r.setbit("a", 5, True)
    assert r.getbit("a", 5)
    # unset bit 4
    assert not r.setbit("a", 4, False)
    assert not r.getbit("a", 4)
    # set bit 4
    assert not r.setbit("a", 4, True)
    assert r.getbit("a", 4)
    # set bit 5 again
    assert r.setbit("a", 5, True)
    assert r.getbit("a", 5)


def test_setbits_and_getkeys(r: redis.Redis):
    # The bit operations and the get commands
    # should play nicely with each other.
    r.setbit("foo", 1, 1)
    assert r.get("foo") == b"@"
    r.setbit("foo", 2, 1)
    assert r.get("foo") == b"`"
    r.setbit("foo", 3, 1)
    assert r.get("foo") == b"p"
    r.setbit("foo", 9, 1)
    assert r.get("foo") == b"p@"
    r.setbit("foo", 54, 1)
    assert r.get("foo") == b"p@\x00\x00\x00\x00\x02"


def test_setbit_wrong_type(r: redis.Redis):
    r.rpush("foo", b"x")
    with pytest.raises(redis.ResponseError):
        r.setbit("foo", 0, 1)


def test_setbit_expiry(r: redis.Redis):
    r.set("foo", b"0x00", ex=10)
    r.setbit("foo", 1, 1)
    assert r.ttl("foo") > 0


def test_bitcount(r: redis.Redis):
    r.delete("foo")
    assert r.bitcount("foo") == 0
    r.setbit("foo", 1, 1)
    assert r.bitcount("foo") == 1
    r.setbit("foo", 8, 1)
    assert r.bitcount("foo") == 2
    assert r.bitcount("foo", 1, 1) == 1
    r.setbit("foo", 57, 1)
    assert r.bitcount("foo") == 3
    r.set("foo", " ")
    assert r.bitcount("foo") == 1
    r.set("key", "foobar")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitcount", "key", "1", "2", "dsd")
    assert r.bitcount("key") == 26
    assert r.bitcount("key", start=0, end=0) == 4
    assert r.bitcount("key", start=1, end=1) == 6


@pytest.mark.min_server("7")
def test_bitcount_mode_redis7(r: redis.Redis):
    r.set("key", "foobar")
    assert r.bitcount("key", start=1, end=1, mode="byte") == 6
    assert r.bitcount("key", start=5, end=30, mode="bit") == 17
    with pytest.raises(redis.ResponseError):
        r.bitcount("key", start=5, end=30, mode="dscd")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitcount", "key", "1", "2", "dsd", "cd")


def test_bitcount_wrong_type(r: redis.Redis):
    r.rpush("foo", b"x")
    with pytest.raises(redis.ResponseError):
        r.bitcount("foo")


def test_bitop(r: redis.Redis):
    r.set("key1", "foobar")
    r.set("key2", "abcdef")

    assert r.bitop("and", "dest", "key1", "key2") == 6
    assert r.get("dest") == b"`bc`ab"

    assert r.bitop("not", "dest1", "key1") == 6
    assert r.get("dest1") == b"\x99\x90\x90\x9d\x9e\x8d"

    assert r.bitop("or", "dest-or", "key1", "key2") == 6
    assert r.get("dest-or") == b"goofev"

    assert r.bitop("xor", "dest-xor", "key1", "key2") == 6
    assert r.get("dest-xor") == b"\x07\r\x0c\x06\x04\x14"


def test_bitop_errors(r: redis.Redis):
    r.set("key1", "foobar")
    r.set("key2", "abcdef")
    r.sadd("key-set", "member1")
    with pytest.raises(redis.ResponseError):
        r.bitop("not", "dest", "key1", "key2")
    with pytest.raises(redis.ResponseError):
        r.bitop("badop", "dest", "key1", "key2")
    with pytest.raises(redis.ResponseError):
        r.bitop("and", "dest", "key1", "key-set")
    with pytest.raises(redis.ResponseError):
        r.bitop("and", "dest")


def test_bitpos(r: redis.Redis):
    key = "key:bitpos"
    r.set(key, b"\xff\xf0\x00")
    assert r.bitpos(key, 0) == 12
    assert r.bitpos(key, 0, 2, -1) == 16
    assert r.bitpos(key, 0, -2, -1) == 12
    r.set(key, b"\x00\xff\xf0")
    assert r.bitpos(key, 1, 0) == 8
    assert r.bitpos(key, 1, 1) == 8
    r.set(key, b"\x00\x00\x00")
    assert r.bitpos(key, 1) == -1
    r.set(key, b"\xff\xf0\x00")


@pytest.mark.min_server("7")
def test_bitops_mode_redis7(r: redis.Redis):
    key = "key:bitpos"
    r.set(key, b"\xff\xf0\x00")
    assert r.bitpos(key, 0, 8, -1, "bit") == 12
    assert r.bitpos(key, 1, 8, -1, "bit") == 8
    with pytest.raises(redis.ResponseError):
        assert r.bitpos(key, 0, 8, -1, "bad_mode") == 12


def test_bitpos_wrong_arguments(r: redis.Redis):
    key = "key:bitpos:wrong:args"
    r.set(key, b"\xff\xf0\x00")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitpos", key, "7")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitpos", key, 1, "6", "5", "BYTE", "6")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitpos", key)


def test_bitfield_empty(r: redis.Redis):
    key = "key:bitfield"
    assert r.bitfield(key).execute() == []
    for overflow in ("wrap", "sat", "fail"):
        assert raw_command(r, "bitfield", key, "overflow", overflow) == []


def test_bitfield_wrong_arguments(r: redis.Redis):
    key = "key:bitfield:wrong:args"
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitfield")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitfield", key, "foo")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitfield", key, "overflow")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitfield", key, "overflow", "foo")


def test_bitfield_get(r: redis.Redis):
    key = "key:bitfield_get"
    r.set(key, b"\xff\xf0\x00")
    for i in range(0, 12):
        assert r.bitfield(key).get("u1", i).get("i1", i).execute() == [1, -1]
    for i in range(12, 25):
        for j in range(1, 63):
            assert r.bitfield(key).get(f"u{j}", i).get(f"i{j}", i).execute() == [0, 0]

    for i in range(0, 11):
        assert r.bitfield(key).get("u2", i).get("i2", i).execute() == [3, -1]
    assert r.bitfield(key).get("u2", 11).get("i2", 11).execute() == [2, -2]
    assert r.bitfield(key).get("u8", 0).get("u8", 8).get("u8", 16).execute() == [
        0xFF,
        0xF0,
        0,
    ]
    assert r.bitfield(key).get("i8", 0).get("i8", 8).get("i8", 16).execute() == [
        ~0,
        ~0x0F,
        0,
    ]

    assert r.bitfield(key).get("u32", 8).get("u8", 100).execute() == [0xF000_0000, 0]

    r.set(key, b"\x01\x23\x45\x67\x89\xab\xcd\xef")
    for enc in ("i16", "u16"):
        assert r.bitfield(key).get(enc, 0).execute() == [0x0123]
        assert r.bitfield(key).get(enc, 4).execute() == [0x1234]
        assert r.bitfield(key).get(enc, 8).execute() == [0x2345]

        assert r.bitfield(key).get(enc, 1).execute() == [0x0246]
        assert r.bitfield(key).get(enc, 5).execute() == [0x2468]
        assert r.bitfield(key).get(enc, 9).execute() == [0x468A]

        assert r.bitfield(key).get(enc, 2).execute() == [0x048D]
        assert r.bitfield(key).get(enc, 6).execute() == [0x48D1]

    assert r.bitfield(key).get("u16", 10).get("i16", 10).execute() == [
        0x8D15,
        0xD15 - 0x8000,
    ]
    assert r.bitfield(key).get("u32", 16).get("u48", 8).execute() == [
        0x456789AB,
        0x2345_6789_ABCD,
    ]
    assert r.bitfield(key).get("i32", 16).get("i48", 8).execute() == [
        0x456789AB,
        0x2345_6789_ABCD,
    ]
    assert r.bitfield(key).get("u63", 1).execute() == [0x123456789_ABCDEF]
    assert r.bitfield(key).get("i63", 1).execute() == [0x123456789_ABCDEF]
    assert r.bitfield(key).get("i64", 0).execute() == [0x123456789_ABCDEF]
    assert raw_command(r, "bitfield", key, "get", "i16", 0) == [0x0123]


def test_bitfield_set(r: redis.Redis):
    key = "key:bitfield_set"
    r.set(key, b"\xff\xf0\x00")
    assert r.bitfield(key).set("u8", 0, 0x55).set("u8", 16, 0xAA).execute() == [0xFF, 0]
    assert r.get(key) == b"\x55\xf0\xaa"
    assert r.bitfield(key).set("u1", 0, 1).set("u1", 16, 2).execute() == [0, 1]
    assert r.get(key) == b"\xd5\xf0\x2a"
    assert r.bitfield(key).set("i1", 31, 1).set("i1", 30, 1).execute() == [0, 0]
    assert r.get(key) == b"\xd5\xf0\x2a\x03"
    assert r.bitfield(key).set("u36", 4, 0xBADC0FFE).execute() == [0x5_F02A_0300]
    assert r.get(key) == b"\xd0\xba\xdc\x0f\xfe"
    assert r.bitfield(key, "WRAP").set("u12", 8, 0xFFF).execute() == [0xBAD]
    assert r.get(key) == b"\xd0\xff\xfc\x0f\xfe"


def test_bitfield_set_sat(r: redis.Redis):
    key = "key:bitfield_set"
    r.set(key, b"\xff\xf0\x00")
    assert r.bitfield(key, "SAT").set("u8", 4, 0x123).set("u8", 8, 0x55).execute() == [
        0xFF,
        0xF0,
    ]
    assert r.get(key) == b"\xff\x55\x00"
    assert r.bitfield(key, "SAT").set("u12", 0, -1).set("u1", 1, 2).execute() == [
        0xFF5,
        1,
    ]
    assert r.get(key) == b"\xff\xf5\x00"
    assert r.bitfield(key, "SAT").set("i4", 0, 8).set("i4", 4, 7).execute() == [-1, -1]
    assert r.get(key) == b"\x77\xf5\x00"
    assert r.bitfield(key, "SAT").set("i4", 4, -8).set("i4", 0, -9).execute() == [7, 7]
    assert r.get(key) == b"\x88\xf5\x00"
    assert r.bitfield(key, "SAT").set("i60", 0, -(1 << 62) + 1).execute() == [
        0x88F5000_00000000 - (1 << 60)
    ]
    assert r.get(key) == b"\x80" + b"\0" * 7
    assert r.bitfield(key, "SAT").set("u60", 0, -(1 << 63) + 1).execute() == [1 << 59]
    assert r.get(key) == b"\xff" * 7 + b"\xf0"


def test_bitfield_set_fail(r: redis.Redis):
    key = "key:bitfield_set"
    r.set(key, b"\xff\xf0\x00")
    assert r.bitfield(key, "FAIL").set("u8", 4, 0x123).set("u8", 8, 0x55).execute() == [
        None,
        0xF0,
    ]
    assert r.get(key) == b"\xff\x55\x00"
    assert r.bitfield(key, "FAIL").set("u12", 0, -1).set("u1", 1, 2).execute() == [
        None,
        None,
    ]
    assert r.get(key) == b"\xff\x55\x00"
    assert r.bitfield(key, "FAIL").set("i4", 0, 8).set("i4", 4, 7).execute() == [
        None,
        -1,
    ]
    assert r.get(key) == b"\xf7\x55\x00"
    assert r.bitfield(key, "FAIL").set("i4", 4, -8).set("i4", 0, -9).execute() == [
        7,
        None,
    ]
    assert r.get(key) == b"\xf8\x55\x00"


def test_bitfield_incr(r: redis.Redis):
    key = "key:bitfield_incr"
    r.set(key, b"\xff\xf0\x00")
    assert r.bitfield(key).incrby("u8", 0, 0x55).incrby("u8", 16, 0xAA).execute() == [
        0x54,
        0xAA,
    ]
    assert r.get(key) == b"\x54\xf0\xaa"
    assert r.bitfield(key).incrby("u1", 0, 1).incrby("u1", 16, 2).execute() == [1, 1]
    assert r.get(key) == b"\xd4\xf0\xaa"
    assert r.bitfield(key).incrby("i1", 31, 1).incrby("i1", 30, 1).execute() == [-1, -1]
    assert r.get(key) == b"\xd4\xf0\xaa\x03"
    assert r.bitfield(key).incrby("u36", 4, 0xBADC0FFE).execute() == [0x5_AB86_12FE]
    assert r.get(key) == b"\xd5\xab\x86\x12\xfe"
    assert r.bitfield(key, "WRAP").incrby("u12", 8, 0xFFF).execute() == [0xAB7]
    assert r.get(key) == b"\xd5\xab\x76\x12\xfe"


def test_bitfield_incr_sat(r: redis.Redis):
    key = "key:bitfield_incr_sat"
    r.set(key, b"\xff\xf0\x00")
    assert r.bitfield(key, "SAT").incrby("u8", 4, 0x123).incrby(
        "u8", 8, 0x55
    ).execute() == [0xFF, 0xFF]
    assert r.get(key) == b"\xff\xff\x00"
    assert r.bitfield(key, "SAT").incrby("u12", 0, -1).incrby("u1", 1, 2).execute() == [
        0xFFE,
        1,
    ]
    assert r.get(key) == b"\xff\xef\x00"
    assert r.bitfield(key, "SAT").incrby("i4", 0, 8).incrby("i4", 4, 7).execute() == [
        7,
        6,
    ]
    assert r.get(key) == b"\x76\xef\x00"
    assert r.bitfield(key, "SAT").incrby("i4", 4, -8).incrby("i4", 0, -9).execute() == [
        -2,
        -2,
    ]
    assert r.get(key) == b"\xee\xef\x00"
    assert r.bitfield(key, "SAT").incrby("i60", 0, -(1 << 62) + 1).execute() == [
        -(1 << 59)
    ]
    assert r.get(key) == b"\x80" + b"\0" * 7
    assert r.bitfield(key, "SAT").set("u60", 0, -(1 << 63) + 1).execute() == [1 << 59]
    assert r.get(key) == b"\xff" * 7 + b"\xf0"


def test_bitfield_incr_fail(r: redis.Redis):
    key = "key:bitfield_incr_fail"
    r.set(key, b"\xff\xf0\x00")
    assert r.bitfield(key, "FAIL").incrby("u8", 4, 0x123).incrby(
        "u8", 8, 0x55
    ).execute() == [None, None]
    assert r.get(key) == b"\xff\xf0\x00"
    assert r.bitfield(key, "FAIL").incrby("u12", 0, -1).incrby(
        "u1", 1, 2
    ).execute() == [0xFFE, None]
    assert r.get(key) == b"\xff\xe0\x00"
    assert r.bitfield(key, "FAIL").incrby("i4", 0, 8).incrby("i4", 4, 7).execute() == [
        7,
        6,
    ]
    assert r.get(key) == b"\x76\xe0\x00"
    assert r.bitfield(key, "FAIL").incrby("i4", 4, -8).incrby(
        "i4", 0, -9
    ).execute() == [-2, -2]
    assert r.get(key) == b"\xee\xe0\x00"


def test_bitfield_get_wrong_arguments(r: redis.Redis):
    key = "key:bitfield_get:wrong:args"
    r.set(key, b"\xff\xf0\x00")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitfield", key, "get")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitfield", key, "get", "i16")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitfield", key, "get", "i16", -1)
    for encoding in ("I8", "i-42", "i5?", "u0", "i0", "i65", "u64", "i 60"):
        with pytest.raises(redis.ResponseError):
            raw_command(r, "bitfield", key, "get", encoding, 0)


def test_bitfield_set_wrong_arguments(r: redis.Redis):
    key = "key:bitfield_set:wrong:args"
    r.set(key, b"\xff\xf0\x00")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitfield", key, "set")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitfield", key, "set", "i16")
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitfield", key, "set", "i16", -1)
    with pytest.raises(redis.ResponseError):
        raw_command(r, "bitfield", key, "set", "i16", 0, "foo")
    for encoding in ("I8", "i-42", "i5?", "u0", "i0", "i65", "u64", "i 60"):
        with pytest.raises(redis.ResponseError):
            raw_command(r, "bitfield", key, "set", encoding, 0, 0)
