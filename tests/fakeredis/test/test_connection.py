import pytest
import redis
import redis.client
from redis.exceptions import ResponseError

from fakeredis import _msgs as msgs
from test import testtools
from test.testtools import raw_command


def test_ping(r: redis.Redis):
    assert r.ping()
    assert testtools.raw_command(r, "ping", "test") == b"test"
    with pytest.raises(redis.ResponseError, match=msgs.WRONG_ARGS_MSG6.format("ping")[4:]):
        raw_command(r, "ping", "arg1", "arg2")


def test_echo(r: redis.Redis):
    assert r.echo(b"hello") == b"hello"
    assert r.echo("hello") == b"hello"


@testtools.fake_only
def test_time(r, mocker):
    fake_time = mocker.patch("time.time")
    fake_time.return_value = 1234567890.1234567
    assert r.time() == (1234567890, 123457)
    fake_time.return_value = 1234567890.000001
    assert r.time() == (1234567890, 1)
    fake_time.return_value = 1234567890.9999999
    assert r.time() == (1234567891, 0)


@pytest.mark.decode_responses
class TestDecodeResponses:
    def test_decode_str(self, r):
        r.set("foo", "bar")
        assert r.get("foo") == "bar"

    def test_decode_set(self, r):
        r.sadd("foo", "member1")
        assert r.smembers("foo") == {"member1"}

    def test_decode_list(self, r):
        r.rpush("foo", "a", "b")
        assert r.lrange("foo", 0, -1) == ["a", "b"]

    def test_decode_dict(self, r):
        r.hset("foo", "key", "value")
        assert r.hgetall("foo") == {"key": "value"}

    def test_decode_error(self, r):
        r.set("foo", "bar")
        with pytest.raises(ResponseError) as exc_info:
            r.hset("foo", "bar", "baz")
        assert isinstance(exc_info.value.args[0], str)


@pytest.mark.disconnected
@testtools.fake_only
class TestFakeStrictRedisConnectionErrors:
    def test_flushdb(self, r):
        with pytest.raises(redis.ConnectionError):
            r.flushdb()

    def test_flushall(self, r):
        with pytest.raises(redis.ConnectionError):
            r.flushall()

    def test_append(self, r):
        with pytest.raises(redis.ConnectionError):
            r.append("key", "value")

    def test_bitcount(self, r):
        with pytest.raises(redis.ConnectionError):
            r.bitcount("key", 0, 20)

    def test_decr(self, r):
        with pytest.raises(redis.ConnectionError):
            r.decr("key", 2)

    def test_exists(self, r):
        with pytest.raises(redis.ConnectionError):
            r.exists("key")

    def test_expire(self, r):
        with pytest.raises(redis.ConnectionError):
            r.expire("key", 20)

    def test_pexpire(self, r):
        with pytest.raises(redis.ConnectionError):
            r.pexpire("key", 20)

    def test_echo(self, r):
        with pytest.raises(redis.ConnectionError):
            r.echo("value")

    def test_get(self, r):
        with pytest.raises(redis.ConnectionError):
            r.get("key")

    def test_getbit(self, r):
        with pytest.raises(redis.ConnectionError):
            r.getbit("key", 2)

    def test_getset(self, r):
        with pytest.raises(redis.ConnectionError):
            r.getset("key", "value")

    def test_incr(self, r):
        with pytest.raises(redis.ConnectionError):
            r.incr("key")

    def test_incrby(self, r):
        with pytest.raises(redis.ConnectionError):
            r.incrby("key")

    def test_ncrbyfloat(self, r):
        with pytest.raises(redis.ConnectionError):
            r.incrbyfloat("key")

    def test_keys(self, r):
        with pytest.raises(redis.ConnectionError):
            r.keys()

    def test_mget(self, r):
        with pytest.raises(redis.ConnectionError):
            r.mget(["key1", "key2"])

    def test_mset(self, r):
        with pytest.raises(redis.ConnectionError):
            r.mset({"key": "value"})

    def test_msetnx(self, r):
        with pytest.raises(redis.ConnectionError):
            r.msetnx({"key": "value"})

    def test_persist(self, r):
        with pytest.raises(redis.ConnectionError):
            r.persist("key")

    def test_rename(self, r):
        server = r.connection_pool.connection_kwargs["server"]
        server.connected = True
        r.set("key1", "value")
        server.connected = False
        with pytest.raises(redis.ConnectionError):
            r.rename("key1", "key2")
        server.connected = True
        assert r.exists("key1")

    def test_eval(self, r):
        with pytest.raises(redis.ConnectionError):
            r.eval("", 0)

    def test_lpush(self, r):
        with pytest.raises(redis.ConnectionError):
            r.lpush("name", 1, 2)

    def test_lrange(self, r):
        with pytest.raises(redis.ConnectionError):
            r.lrange("name", 1, 5)

    def test_llen(self, r):
        with pytest.raises(redis.ConnectionError):
            r.llen("name")

    def test_lrem(self, r):
        with pytest.raises(redis.ConnectionError):
            r.lrem("name", 2, 2)

    def test_rpush(self, r):
        with pytest.raises(redis.ConnectionError):
            r.rpush("name", 1)

    def test_lpop(self, r):
        with pytest.raises(redis.ConnectionError):
            r.lpop("name")

    def test_lset(self, r):
        with pytest.raises(redis.ConnectionError):
            r.lset("name", 1, 4)

    def test_rpushx(self, r):
        with pytest.raises(redis.ConnectionError):
            r.rpushx("name", 1)

    def test_ltrim(self, r):
        with pytest.raises(redis.ConnectionError):
            r.ltrim("name", 1, 4)

    def test_lindex(self, r):
        with pytest.raises(redis.ConnectionError):
            r.lindex("name", 1)

    def test_lpushx(self, r):
        with pytest.raises(redis.ConnectionError):
            r.lpushx("name", 1)

    def test_rpop(self, r):
        with pytest.raises(redis.ConnectionError):
            r.rpop("name")

    def test_linsert(self, r):
        with pytest.raises(redis.ConnectionError):
            r.linsert("name", "where", "refvalue", "value")

    def test_rpoplpush(self, r):
        with pytest.raises(redis.ConnectionError):
            r.rpoplpush("src", "dst")

    def test_blpop(self, r):
        with pytest.raises(redis.ConnectionError):
            r.blpop("keys")

    def test_brpop(self, r):
        with pytest.raises(redis.ConnectionError):
            r.brpop("keys")

    def test_brpoplpush(self, r):
        with pytest.raises(redis.ConnectionError):
            r.brpoplpush("src", "dst")

    def test_hdel(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hdel("name")

    def test_hexists(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hexists("name", "key")

    def test_hget(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hget("name", "key")

    def test_hgetall(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hgetall("name")

    def test_hincrby(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hincrby("name", "key")

    def test_hincrbyfloat(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hincrbyfloat("name", "key")

    def test_hkeys(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hkeys("name")

    def test_hlen(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hlen("name")

    def test_hset(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hset("name", "key", 1)

    def test_hsetnx(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hsetnx("name", "key", 2)

    def test_hmset(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hmset("name", {"key": 1})

    def test_hmget(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hmget("name", ["a", "b"])

    def test_hvals(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hvals("name")

    def test_sadd(self, r):
        with pytest.raises(redis.ConnectionError):
            r.sadd("name", 1, 2)

    def test_scard(self, r):
        with pytest.raises(redis.ConnectionError):
            r.scard("name")

    def test_sdiff(self, r):
        with pytest.raises(redis.ConnectionError):
            r.sdiff(["a", "b"])

    def test_sdiffstore(self, r):
        with pytest.raises(redis.ConnectionError):
            r.sdiffstore("dest", ["a", "b"])

    def test_sinter(self, r):
        with pytest.raises(redis.ConnectionError):
            r.sinter(["a", "b"])

    def test_sinterstore(self, r):
        with pytest.raises(redis.ConnectionError):
            r.sinterstore("dest", ["a", "b"])

    def test_sismember(self, r):
        with pytest.raises(redis.ConnectionError):
            r.sismember("name", 20)

    def test_smembers(self, r):
        with pytest.raises(redis.ConnectionError):
            r.smembers("name")

    def test_smove(self, r):
        with pytest.raises(redis.ConnectionError):
            r.smove("src", "dest", 20)

    def test_spop(self, r):
        with pytest.raises(redis.ConnectionError):
            r.spop("name")

    def test_srandmember(self, r):
        with pytest.raises(redis.ConnectionError):
            r.srandmember("name")

    def test_srem(self, r):
        with pytest.raises(redis.ConnectionError):
            r.srem("name")

    def test_sunion(self, r):
        with pytest.raises(redis.ConnectionError):
            r.sunion(["a", "b"])

    def test_sunionstore(self, r):
        with pytest.raises(redis.ConnectionError):
            r.sunionstore("dest", ["a", "b"])

    def test_zadd(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zadd("name", {"key": "value"})

    def test_zcard(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zcard("name")

    def test_zcount(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zcount("name", 1, 5)

    def test_zincrby(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zincrby("name", 1, 1)

    def test_zinterstore(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zinterstore("dest", ["a", "b"])

    def test_zrange(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zrange("name", 1, 5)

    def test_zrangebyscore(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zrangebyscore("name", 1, 5)

    def test_rangebylex(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zrangebylex("name", 1, 4)

    def test_zrem(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zrem("name", "value")

    def test_zremrangebyrank(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zremrangebyrank("name", 1, 5)

    def test_zremrangebyscore(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zremrangebyscore("name", 1, 5)

    def test_zremrangebylex(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zremrangebylex("name", 1, 5)

    def test_zlexcount(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zlexcount("name", 1, 5)

    def test_zrevrange(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zrevrange("name", 1, 5, 1)

    def test_zrevrangebyscore(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zrevrangebyscore("name", 5, 1)

    def test_zrevrangebylex(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zrevrangebylex("name", 5, 1)

    def test_zrevran(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zrevrank("name", 2)

    def test_zscore(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zscore("name", 2)

    def test_zunionstor(self, r):
        with pytest.raises(redis.ConnectionError):
            r.zunionstore("dest", ["1", "2"])

    def test_pipeline(self, r):
        with pytest.raises(redis.ConnectionError):
            r.pipeline().watch("key")

    def test_transaction(self, r):
        with pytest.raises(redis.ConnectionError):

            def func(a):
                return a * a

            r.transaction(func, 3)

    def test_lock(self, r):
        with pytest.raises(redis.ConnectionError):
            with r.lock("name"):
                pass

    def test_pubsub(self, r):
        with pytest.raises(redis.ConnectionError):
            r.pubsub().subscribe("channel")

    def test_pfadd(self, r):
        with pytest.raises(redis.ConnectionError):
            r.pfadd("name", 1)

    def test_pfmerge(self, r):
        with pytest.raises(redis.ConnectionError):
            r.pfmerge("dest", "a", "b")

    def test_scan(self, r):
        with pytest.raises(redis.ConnectionError):
            list(r.scan())

    def test_sscan(self, r):
        with pytest.raises(redis.ConnectionError):
            r.sscan("name")

    def test_hscan(self, r):
        with pytest.raises(redis.ConnectionError):
            r.hscan("name")

    def test_scan_iter(self, r):
        with pytest.raises(redis.ConnectionError):
            list(r.scan_iter())

    def test_sscan_iter(self, r):
        with pytest.raises(redis.ConnectionError):
            list(r.sscan_iter("name"))

    def test_hscan_iter(self, r):
        with pytest.raises(redis.ConnectionError):
            list(r.hscan_iter("name"))


@pytest.mark.disconnected
@testtools.fake_only
class TestPubSubConnected:
    @pytest.fixture
    def pubsub(self, r):
        return r.pubsub()

    def test_basic_subscribe(self, pubsub):
        with pytest.raises(redis.ConnectionError):
            pubsub.subscribe("logs")

    def test_subscription_conn_lost(self, fake_server, pubsub):
        fake_server.connected = True
        pubsub.subscribe("logs")
        fake_server.connected = False
        # The initial message is already in the pipe
        msg = pubsub.get_message()
        check = {"type": "subscribe", "pattern": None, "channel": b"logs", "data": 1}
        assert msg == check, "Message was not published to channel"
        with pytest.raises(redis.ConnectionError):
            pubsub.get_message()
