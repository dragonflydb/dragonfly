from __future__ import annotations

import logging

import pytest
import redis
import redis.client
from redis.exceptions import ResponseError

import fakeredis
from test import testtools
from test.testtools import raw_command

json_tests = pytest.importorskip("lupa")


@pytest.mark.min_server("7")
def test_script_exists_redis7(r: redis.Redis):
    # test response for no arguments by bypassing the py-redis command
    # as it requires at least one argument
    with pytest.raises(redis.ResponseError):
        raw_command(r, "SCRIPT EXISTS")

    # use single character characters for non-existing scripts, as those
    # will never be equal to an actual sha1 hash digest
    assert r.script_exists("a") == [0]
    assert r.script_exists("a", "b", "c", "d", "e", "f") == [0, 0, 0, 0, 0, 0]

    sha1_one = r.script_load("return 'a'")
    assert r.script_exists(sha1_one) == [1]
    assert r.script_exists(sha1_one, "a") == [1, 0]
    assert r.script_exists("a", "b", "c", sha1_one, "e") == [0, 0, 0, 1, 0]

    sha1_two = r.script_load("return 'b'")
    assert r.script_exists(sha1_one, sha1_two) == [1, 1]
    assert r.script_exists("a", sha1_one, "c", sha1_two, "e", "f") == [0, 1, 0, 1, 0, 0]


@pytest.mark.max_server("6.2.7")
def test_script_exists_redis6(r: redis.Redis):
    # test response for no arguments by bypassing the py-redis command
    # as it requires at least one argument
    assert raw_command(r, "SCRIPT EXISTS") == []

    # use single character characters for non-existing scripts, as those
    # will never be equal to an actual sha1 hash digest
    assert r.script_exists("a") == [0]
    assert r.script_exists("a", "b", "c", "d", "e", "f") == [0, 0, 0, 0, 0, 0]

    sha1_one = r.script_load("return 'a'")
    assert r.script_exists(sha1_one) == [1]
    assert r.script_exists(sha1_one, "a") == [1, 0]
    assert r.script_exists("a", "b", "c", sha1_one, "e") == [0, 0, 0, 1, 0]

    sha1_two = r.script_load("return 'b'")
    assert r.script_exists(sha1_one, sha1_two) == [1, 1]
    assert r.script_exists("a", sha1_one, "c", sha1_two, "e", "f") == [0, 1, 0, 1, 0, 0]


@pytest.mark.parametrize("args", [("a",), tuple("abcdefghijklmn")])
def test_script_flush_errors_with_args(r, args):
    with pytest.raises(redis.ResponseError):
        raw_command(r, "SCRIPT FLUSH %s" % " ".join(args))


def test_script_flush(r: redis.Redis):
    # generate/load six unique scripts and store their sha1 hash values
    sha1_values = [r.script_load("return '%s'" % char) for char in "abcdef"]

    # assert the scripts all exist prior to flushing
    assert r.script_exists(*sha1_values) == [1] * len(sha1_values)

    # flush and assert OK response
    assert r.script_flush() is True

    # assert none of the scripts exists after flushing
    assert r.script_exists(*sha1_values) == [0] * len(sha1_values)


def test_script_no_subcommands(r: redis.Redis):
    with pytest.raises(redis.ResponseError):
        raw_command(r, "SCRIPT")


@pytest.mark.max_server("7")
def test_script_help(r: redis.Redis):
    assert raw_command(r, "SCRIPT HELP") == [
        b"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        b"DEBUG (YES|SYNC|NO)",
        b"    Set the debug mode for subsequent scripts executed.",
        b"EXISTS <sha1> [<sha1> ...]",
        b"    Return information about the existence of the scripts in the script cach" b"e.",
        b"FLUSH [ASYNC|SYNC]",
        b"    Flush the Lua scripts cache. Very dangerous on replicas.",
        b"    When called without the optional mode argument, the behavior is determin" b"ed by the",
        b"    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
        b"    * ASYNC: Asynchronously flush the scripts cache.",
        b"    * SYNC: Synchronously flush the scripts cache.",
        b"KILL",
        b"    Kill the currently executing Lua script.",
        b"LOAD <script>",
        b"    Load a script into the scripts cache without executing it.",
        b"HELP",
        b"    Prints this help.",
    ]


@pytest.mark.min_server("7.1")
def test_script_help71(r: redis.Redis):
    assert raw_command(r, "SCRIPT HELP") == [
        b"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        b"DEBUG (YES|SYNC|NO)",
        b"    Set the debug mode for subsequent scripts executed.",
        b"EXISTS <sha1> [<sha1> ...]",
        b"    Return information about the existence of the scripts in the script cach" b"e.",
        b"FLUSH [ASYNC|SYNC]",
        b"    Flush the Lua scripts cache. Very dangerous on replicas.",
        b"    When called without the optional mode argument, the behavior is determin" b"ed by the",
        b"    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
        b"    * ASYNC: Asynchronously flush the scripts cache.",
        b"    * SYNC: Synchronously flush the scripts cache.",
        b"KILL",
        b"    Kill the currently executing Lua script.",
        b"LOAD <script>",
        b"    Load a script into the scripts cache without executing it.",
        b"HELP",
        b"    Print this help.",
    ]


@pytest.mark.max_server("7.1")
def test_eval_blpop(r: redis.Redis):
    r.rpush("foo", "bar")
    with pytest.raises(redis.ResponseError, match="This Redis command is not allowed from script"):
        r.eval('return redis.pcall("BLPOP", KEYS[1], 1)', 1, "foo")


def test_eval_set_value_to_arg(r: redis.Redis):
    r.eval('redis.call("SET", KEYS[1], ARGV[1])', 1, "foo", "bar")
    val = r.get("foo")
    assert val == b"bar"


def test_eval_conditional(r: redis.Redis):
    lua = """
    local val = redis.call("GET", KEYS[1])
    if val == ARGV[1] then
        redis.call("SET", KEYS[1], ARGV[2])
    else
        redis.call("SET", KEYS[1], ARGV[1])
    end
    """
    r.eval(lua, 1, "foo", "bar", "baz")
    val = r.get("foo")
    assert val == b"bar"
    r.eval(lua, 1, "foo", "bar", "baz")
    val = r.get("foo")
    assert val == b"baz"


def test_eval_table(r: redis.Redis):
    lua = """
    local a = {}
    a[1] = "foo"
    a[2] = "bar"
    a[17] = "baz"
    return a
    """
    val = r.eval(lua, 0)
    assert val == [b"foo", b"bar"]


def test_eval_table_with_nil(r: redis.Redis):
    lua = """
    local a = {}
    a[1] = "foo"
    a[2] = nil
    a[3] = "bar"
    return a
    """
    val = r.eval(lua, 0)
    assert val == [b"foo"]


def test_eval_table_with_numbers(r: redis.Redis):
    lua = """
    local a = {}
    a[1] = 42
    return a
    """
    val = r.eval(lua, 0)
    assert val == [42]


def test_eval_nested_table(r: redis.Redis):
    lua = """
    local a = {}
    a[1] = {}
    a[1][1] = "foo"
    return a
    """
    val = r.eval(lua, 0)
    assert val == [[b"foo"]]


def test_eval_iterate_over_argv(r: redis.Redis):
    lua = """
    for i, v in ipairs(ARGV) do
    end
    return ARGV
    """
    val = r.eval(lua, 0, "a", "b", "c")
    assert val == [b"a", b"b", b"c"]


def test_eval_iterate_over_keys(r: redis.Redis):
    lua = """
    for i, v in ipairs(KEYS) do
    end
    return KEYS
    """
    val = r.eval(lua, 2, "a", "b", "c")
    assert val == [b"a", b"b"]


def test_eval_mget(r: redis.Redis):
    r.set("foo1", "bar1")
    r.set("foo2", "bar2")
    val = r.eval('return redis.call("mget", "foo1", "foo2")', 2, "foo1", "foo2")
    assert val == [b"bar1", b"bar2"]


def test_eval_mget_not_set(r: redis.Redis):
    val = r.eval('return redis.call("mget", "foo1", "foo2")', 2, "foo1", "foo2")
    assert val == [None, None]


def test_eval_hgetall(r: redis.Redis):
    r.hset("foo", "k1", "bar")
    r.hset("foo", "k2", "baz")
    val = r.eval('return redis.call("hgetall", "foo")', 1, "foo")
    sorted_val = sorted([val[:2], val[2:]])
    assert sorted_val == [[b"k1", b"bar"], [b"k2", b"baz"]]


def test_eval_hgetall_iterate(r: redis.Redis):
    r.hset("foo", "k1", "bar")
    r.hset("foo", "k2", "baz")
    lua = """
    local result = redis.call("hgetall", "foo")
    for i, v in ipairs(result) do
    end
    return result
    """
    val = r.eval(lua, 1, "foo")
    sorted_val = sorted([val[:2], val[2:]])
    assert sorted_val == [[b"k1", b"bar"], [b"k2", b"baz"]]


def test_eval_invalid_command(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.eval('return redis.call("FOO")', 0)


def test_eval_syntax_error(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.eval('return "', 0)


def test_eval_runtime_error(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.eval('error("CRASH")', 0)


def test_eval_more_keys_than_args(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.eval("return 1", 42)


def test_eval_numkeys_float_string(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.eval("return KEYS[1]", "0.7", "foo")


def test_eval_numkeys_integer_string(r: redis.Redis):
    val = r.eval("return KEYS[1]", "1", "foo")
    assert val == b"foo"


def test_eval_numkeys_negative(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.eval("return KEYS[1]", -1, "foo")


def test_eval_numkeys_float(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.eval("return KEYS[1]", 0.7, "foo")


def test_eval_global_variable(r: redis.Redis):
    # Redis doesn't allow script to define global variables
    with pytest.raises(ResponseError):
        r.eval("a=10", 0)


def test_eval_global_and_return_ok(r: redis.Redis):
    # Redis doesn't allow script to define global variables
    with pytest.raises(ResponseError):
        r.eval(
            """
            a=10
            return redis.status_reply("Everything is awesome")
            """,
            0,
        )


def test_eval_convert_number(r: redis.Redis):
    # Redis forces all Lua numbers to integer
    val = r.eval("return 3.2", 0)
    assert val == 3
    val = r.eval("return 3.8", 0)
    assert val == 3
    val = r.eval("return -3.8", 0)
    assert val == -3


def test_eval_convert_bool(r: redis.Redis):
    # Redis converts true to 1 and false to nil (which redis-py converts to None)
    assert r.eval("return false", 0) is None
    val = r.eval("return true", 0)
    assert val == 1
    assert not isinstance(val, bool)


@pytest.mark.max_server("6.2.7")
def test_eval_call_bool6(r: redis.Redis):
    # Redis doesn't allow Lua bools to be passed to [p]call
    with pytest.raises(redis.ResponseError, match=r"Lua redis\(\) command arguments must be strings or integers"):
        r.eval('return redis.call("SET", KEYS[1], true)', 1, "testkey")


@pytest.mark.min_server("7")
def test_eval_call_bool7(r: redis.Redis):
    # Redis doesn't allow Lua bools to be passed to [p]call
    with pytest.raises(redis.ResponseError, match=r"Lua redis lib command arguments must be strings or integers"):
        r.eval('return redis.call("SET", KEYS[1], true)', 1, "testkey")


def test_eval_return_error(r: redis.Redis):
    with pytest.raises(redis.ResponseError, match="Testing") as exc_info:
        r.eval('return {err="Testing"}', 0)
    assert isinstance(exc_info.value.args[0], str)
    with pytest.raises(redis.ResponseError, match="Testing") as exc_info:
        r.eval('return redis.error_reply("Testing")', 0)
    assert isinstance(exc_info.value.args[0], str)


def test_eval_return_redis_error(r: redis.Redis):
    with pytest.raises(redis.ResponseError) as exc_info:
        r.eval('return redis.pcall("BADCOMMAND")', 0)
    assert isinstance(exc_info.value.args[0], str)


def test_eval_return_ok(r: redis.Redis):
    val = r.eval('return {ok="Testing"}', 0)
    assert val == b"Testing"
    val = r.eval('return redis.status_reply("Testing")', 0)
    assert val == b"Testing"


def test_eval_return_ok_nested(r: redis.Redis):
    val = r.eval(
        """
        local a = {}
        a[1] = {ok="Testing"}
        return a
        """,
        0,
    )
    assert val == [b"Testing"]


def test_eval_return_ok_wrong_type(r: redis.Redis):
    with pytest.raises(redis.ResponseError):
        r.eval("return redis.status_reply(123)", 0)


def test_eval_pcall(r: redis.Redis):
    val = r.eval(
        """
        local a = {}
        a[1] = redis.pcall("foo")
        return a
        """,
        0,
    )
    assert isinstance(val, list)
    assert len(val) == 1
    assert isinstance(val[0], ResponseError)


def test_eval_pcall_return_value(r: redis.Redis):
    with pytest.raises(ResponseError):
        r.eval('return redis.pcall("foo")', 0)


def test_eval_delete(r: redis.Redis):
    r.set("foo", "bar")
    val = r.get("foo")
    assert val == b"bar"
    val = r.eval('redis.call("DEL", KEYS[1])', 1, "foo")
    assert val is None


def test_eval_exists(r: redis.Redis):
    val = r.eval('return redis.call("exists", KEYS[1]) == 0', 1, "foo")
    assert val == 1


def test_eval_flushdb(r: redis.Redis):
    r.set("foo", "bar")
    val = r.eval(
        """
        local value = redis.call("FLUSHDB");
        return type(value) == "table" and value.ok == "OK";
        """,
        0,
    )
    assert val == 1


def test_eval_flushall(r, create_redis):
    r1 = create_redis(db=2)
    r2 = create_redis(db=3)

    r1["r1"] = "r1"
    r2["r2"] = "r2"

    val = r.eval(
        """
        local value = redis.call("FLUSHALL");
        return type(value) == "table" and value.ok == "OK";
        """,
        0,
    )

    assert val == 1
    assert "r1" not in r1
    assert "r2" not in r2


def test_eval_incrbyfloat(r: redis.Redis):
    r.set("foo", 0.5)
    val = r.eval(
        """
        local value = redis.call("INCRBYFLOAT", KEYS[1], 2.0);
        return type(value) == "string" and tonumber(value) == 2.5;
        """,
        1,
        "foo",
    )
    assert val == 1


def test_eval_lrange(r: redis.Redis):
    r.rpush("foo", "a", "b")
    val = r.eval(
        """
        local value = redis.call("LRANGE", KEYS[1], 0, -1);
        return type(value) == "table" and value[1] == "a" and value[2] == "b";
        """,
        1,
        "foo",
    )
    assert val == 1


def test_eval_ltrim(r: redis.Redis):
    r.rpush("foo", "a", "b", "c", "d")
    val = r.eval(
        """
        local value = redis.call("LTRIM", KEYS[1], 1, 2);
        return type(value) == "table" and value.ok == "OK";
        """,
        1,
        "foo",
    )
    assert val == 1
    assert r.lrange("foo", 0, -1) == [b"b", b"c"]


def test_eval_lset(r: redis.Redis):
    r.rpush("foo", "a", "b")
    val = r.eval(
        """
        local value = redis.call("LSET", KEYS[1], 0, "z");
        return type(value) == "table" and value.ok == "OK";
        """,
        1,
        "foo",
    )
    assert val == 1
    assert r.lrange("foo", 0, -1) == [b"z", b"b"]


def test_eval_sdiff(r: redis.Redis):
    r.sadd("foo", "a", "b", "c", "f", "e", "d")
    r.sadd("bar", "b")
    val = r.eval(
        """
        local value = redis.call("SDIFF", KEYS[1], KEYS[2]);
        if type(value) ~= "table" then
            return redis.error_reply(type(value) .. ", should be table");
        else
            return value;
        end
        """,
        2,
        "foo",
        "bar",
    )
    # Note: while fakeredis sorts the result when using Lua, this isn't
    # actually part of the redis contract (see
    # https://github.com/antirez/redis/issues/5538), and for Redis 5 we
    # need to sort val to pass the test.
    assert sorted(val) == [b"a", b"c", b"d", b"e", b"f"]


def test_script(r: redis.Redis):
    script = r.register_script("return ARGV[1]")
    result = script(args=[42])
    assert result == b"42"


@testtools.fake_only
def test_lua_log(r, caplog):
    logger = fakeredis._server.LOGGER
    script = """
        redis.log(redis.LOG_DEBUG, "debug")
        redis.log(redis.LOG_VERBOSE, "verbose")
        redis.log(redis.LOG_NOTICE, "notice")
        redis.log(redis.LOG_WARNING, "warning")
    """
    script = r.register_script(script)
    with caplog.at_level("DEBUG"):
        script()
    assert caplog.record_tuples == [
        (logger.name, logging.DEBUG, "debug"),
        (logger.name, logging.INFO, "verbose"),
        (logger.name, logging.INFO, "notice"),
        (logger.name, logging.WARNING, "warning"),
    ]


def test_lua_log_no_message(r: redis.Redis):
    script = "redis.log(redis.LOG_DEBUG)"
    script = r.register_script(script)
    with pytest.raises(redis.ResponseError):
        script()


@testtools.fake_only
def test_lua_log_different_types(r, caplog):
    logger = logging.getLogger("fakeredis")
    script = "redis.log(redis.LOG_DEBUG, 'string', 1, true, 3.14, 'string')"
    script = r.register_script(script)
    with caplog.at_level("DEBUG"):
        script()
    assert caplog.record_tuples == [(logger.name, logging.DEBUG, "string 1 3.14 string")]


def test_lua_log_wrong_level(r: redis.Redis):
    script = "redis.log(10, 'string')"
    script = r.register_script(script)
    with pytest.raises(redis.ResponseError):
        script()


@testtools.fake_only
def test_lua_log_defined_vars(r, caplog):
    logger = fakeredis._server.LOGGER
    script = """
        local var='string'
        redis.log(redis.LOG_DEBUG, var)
    """
    script = r.register_script(script)
    with caplog.at_level("DEBUG"):
        script()
    assert caplog.record_tuples == [(logger.name, logging.DEBUG, "string")]


def test_hscan_cursors_are_bytes(r: redis.Redis):
    r.hset("hkey", "foo", 1)

    result = r.eval(
        """
        local results = redis.call("HSCAN", KEYS[1], "0")
        return results[1]
        """,
        1,
        "hkey",
    )

    assert result == b"0"
    assert isinstance(result, bytes)


@pytest.mark.xfail  # TODO
def test_deleting_while_scan(r: redis.Redis):
    for i in range(100):
        r.set(f"key-{i}", i)

    assert len(r.keys()) == 100

    script = """
        local cursor = 0
        local seen = {}
        repeat
            local result = redis.call('SCAN', cursor)
            for _,key in ipairs(result[2]) do
                seen[#seen+1] = key
                redis.call('DEL', key)
            end
            cursor = tonumber(result[1])
        until cursor == 0
        return seen
    """

    assert len(r.register_script(script)()) == 100
    assert len(r.keys()) == 0
