import pytest
import redis.client


def test_causes_crash(r: redis.Redis):
    key = b"}W\xfa\x87\xf4"
    key2 = b"\xf3\xba\x00\xa1\x1c\xac\x01A\x8b\xc4\xe9\xe2\xa8"
    r.rpush(key, b"!\xef\x9e\xd2", b"1175417134")
    r.rpoplpush(key, key)
    r.lrange(key, -1, 14795)
    with pytest.raises(redis.ResponseError):
        r.rename(key2, key2)
    r.lrange(key, 2, 0)
    r.sort(key, alpha=True)
    r.llen(key)
    r.keys("*")
    r.keys("*")
    r.lindex(key, 1)
    r.exists(key, key2, key, key2, key2, key2, key2, key)
    r.linsert(key, "AFTER", b"inf", b"!\xef\x9e\xd2")
    with pytest.raises(redis.ResponseError):
        r.linsert(
            key,
            b"W8\xe9&",
            b"-43950",
            b"-43950",
        )
    r.rpoplpush(key, key)
    with pytest.raises(redis.ResponseError):
        r.exists()
    r.lrem(key2, -56700, b"-6.816602725023744e+16")
    r.lrem(key, -3, b"1175417134")
    r.llen(key2)
    r.lrem(key, -3, b"!\xef\x9e\xd2")


def test_another_test_causes_crash(r: redis.Redis):
    key1 = b"\xc2\xdb"
    key2 = b"z`\xf8,\xe2\x02\xb3\x85\xc5"
    key3 = b"\xf4<\xe1\xb6\xcb\xde\xaf"
    key4 = b"\xad"
    r.rpush(key1, b"i\x05\x0b\xb1")
    r.rpush(
        key2,
        b"i\x05\x0b\xb1",
        b"i\x05\x0b\xb1",
        b"\\h\xf2",
        b"\\h\xf2",
        b"i\x05\x0b\xb1",
        b"\\h\xf2",
    )
    r.rpush(
        key3,
        b"\\h\xf2",
        b"i\x05\x0b\xb1",
        b"i\x05\x0b\xb1",
        b"\\h\xf2",
        b"\\h\xf2",
        b"\\h\xf2",
        b"\\h\xf2",
        b"i\x05\x0b\xb1",
    )
    r.rpush(
        key1,
        b"i\x05\x0b\xb1",
        b"\\h\xf2",
        b"i\x05\x0b\xb1",
        b"\\h\xf2",
        b"i\x05\x0b\xb1",
        b"\\h\xf2",
        b"\\h\xf2",
        b"i\x05\x0b\xb1",
    )
    r.rpush(key2, b"i\x05\x0b\xb1")

    r.lpop(b"")
    with pytest.raises(redis.ResponseError):
        r.rpushx(key4)
    r.move(b"", 1)
    with pytest.raises(redis.ResponseError):
        r.move(key3, -730)
    r.ltrim(key3, -51547, -2)
    r.rpoplpush(key4, b"")

    r.rpush(key4, b"i\x05\x0b\xb1", b"\\h\xf2", b"\\h\xf2")
    r.persist(key2)
    r.exists(key1, key1, key1)

    r.ltrim(b"", -12584, -3)
    r.lrem(key4, -1, b"i\x05\x0b\xb1")
    with pytest.raises(redis.ResponseError):
        r.linsert(key2, b"\xa5", b"\\h\xf2", b"i\x05\x0b\xb1")
    r.linsert(key2, "BEFORE", b"\\h\xf2", b"\\h\xf2")
    r.ltrim(key2, 1, -2_147_483_648)
    r.ltrim(key1, -4200252, 1)
    r.rpush(
        b"",
        b"i\x05\x0b\xb1",
        b"i\x05\x0b\xb1",
        b"i\x05\x0b\xb1",
        b"\\h\xf2",
        b"\\h\xf2",
        b"i\x05\x0b\xb1",
    )
    with pytest.raises(redis.ResponseError):
        r.rpop(key1, -2_147_483_648)
    r.lrem(key1, 77, b"i\x05\x0b\xb1")
    r.rpoplpush(b"", key2)
    r.ltrim(b"", 0, 1)
    r.unlink(b"")
    r.ltrim(key1, 0, 0)
    r.lrem(key3, 31029, b"\\h\xf2")
    r.lrange(key1, -2, -91)
    r.rpoplpush(key1, key2)
    r.rpush(
        key1,
        b"\\h\xf2",
        b"\\h\xf2",
        b"\\h\xf2",
        b"\\h\xf2",
        b"\\h\xf2",
        b"\\h\xf2",
        b"\\h\xf2",
        b"\\h\xf2",
        b"\\h\xf2",
        b"\\h\xf2",
        b"\\h\xf2",
        b"i\x05\x0b\xb1",
        b"i\x05\x0b\xb1",
        b"\\h\xf2",
    )
    r.ltrim(key1, 0, 18)
    r.keys("*")
    with pytest.raises(redis.ResponseError):
        r.move(key4, 993)
    r.lrange(b"", 0, 38001)
    with pytest.raises(redis.ResponseError):
        r.sort(key4)
    r.lindex(key1, -2)
    r.rpoplpush(key4, key4)
    r.lrem(key4, -18528, b"\\h\xf2")
