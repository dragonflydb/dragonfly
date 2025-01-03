import pytest
import redis.client


def test_causes_crash(r: redis.Redis):
    key = b'}W\xfa\x87\xf4'
    key2 = b'\xf3\xba\x00\xa1\x1c\xac\x01A\x8b\xc4\xe9\xe2\xa8'
    r.rpush(key, b'!\xef\x9e\xd2', b'1175417134')
    r.rpoplpush(key, key)
    r.lrange(key, -1, 14795)
    with pytest.raises(redis.ResponseError):
        r.rename(key2,
                 key2)
    r.lrange(key, 2, 0)
    r.sort(key, alpha=True)
    r.llen(key)
    r.keys('*')
    r.keys('*')
    r.lindex(key, 1)
    r.exists(key, key2, key, key2, key2, key2, key2, key)
    r.linsert(key, 'AFTER', b'inf', b'!\xef\x9e\xd2')
    with pytest.raises(redis.ResponseError):
        r.linsert(key, b'W8\xe9&', b'-43950', b'-43950', )
    r.rpoplpush(key, key)
    with pytest.raises(redis.ResponseError):
        r.exists()
    r.lrem(key2, -56700, b'-6.816602725023744e+16')
    r.lrem(key, -3, b'1175417134')
    r.llen(key2)
    r.lrem(key, -3, b'!\xef\x9e\xd2')
