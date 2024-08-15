import functools
import math
import operator
import sys
from typing import Tuple, Any

import hypothesis
import hypothesis.stateful
import hypothesis.strategies as st
import pytest
import redis
from hypothesis.stateful import rule, initialize, precondition
from hypothesis.strategies import SearchStrategy

import fakeredis
from fakeredis._server import _create_version

self_strategy = st.runner()


def get_redis_version() -> Tuple[int]:
    try:
        r = redis.StrictRedis("localhost", port=6380, db=2)
        r.ping()
        return _create_version(r.info()["redis_version"])
    except redis.ConnectionError:
        return (6,)
    finally:
        if hasattr(r, "close"):
            r.close()  # Absent in older versions of redis-py


@st.composite
def sample_attr(draw, name):
    """Strategy for sampling a specific attribute from a state machine"""
    machine = draw(self_strategy)
    values = getattr(machine, name)
    position = draw(st.integers(min_value=0, max_value=len(values) - 1))
    return values[position]


redis_ver = get_redis_version()

keys = sample_attr("keys")
fields = sample_attr("fields")
values = sample_attr("values")
scores = sample_attr("scores")

int_as_bytes = st.builds(lambda x: str(default_normalize(x)).encode(), st.integers())
float_as_bytes = st.builds(lambda x: repr(default_normalize(x)).encode(), st.floats(width=32))
counts = st.integers(min_value=-3, max_value=3) | st.integers()
limits = st.just(()) | st.tuples(st.just("limit"), counts, counts)
# Redis has an integer overflow bug in swapdb, so we confine the numbers to
# a limited range (https://github.com/antirez/redis/issues/5737).
dbnums = st.integers(min_value=0, max_value=3) | st.integers(min_value=-1000, max_value=1000)
# The filter is to work around https://github.com/antirez/redis/issues/5632
patterns = st.text(alphabet=st.sampled_from("[]^$*.?-azAZ\\\r\n\t")) | st.binary().filter(lambda x: b"\0" not in x)
score_tests = scores | st.builds(lambda x: b"(" + repr(x).encode(), scores)
string_tests = st.sampled_from([b"+", b"-"]) | st.builds(operator.add, st.sampled_from([b"(", b"["]), fields)
# Redis has integer overflow bugs in time computations, which is why we set a maximum.
expires_seconds = st.integers(min_value=100000, max_value=10000000000)
expires_ms = st.integers(min_value=100000000, max_value=10000000000000)


class WrappedException:
    """Wraps an exception for the purposes of comparison."""

    def __init__(self, exc):
        self.wrapped = exc

    def __str__(self):
        return str(self.wrapped)

    def __repr__(self):
        return "WrappedException({!r})".format(self.wrapped)

    def __eq__(self, other):
        if not isinstance(other, WrappedException):
            return NotImplemented
        if type(self.wrapped) != type(other.wrapped):  # noqa: E721
            return False
        return True
        # return self.wrapped.args == other.wrapped.args

    def __ne__(self, other):
        if not isinstance(other, WrappedException):
            return NotImplemented
        return not self == other


def wrap_exceptions(obj):
    if isinstance(obj, list):
        return [wrap_exceptions(item) for item in obj]
    elif isinstance(obj, Exception):
        return WrappedException(obj)
    else:
        return obj


def sort_list(lst):
    if isinstance(lst, list):
        return sorted(lst)
    else:
        return lst


def normalize_if_number(x):
    try:
        res = float(x)
        return x if math.isnan(res) else res
    except ValueError:
        return x


def flatten(args):
    if isinstance(args, (list, tuple)):
        for arg in args:
            yield from flatten(arg)
    elif args is not None:
        yield args


def default_normalize(x: Any) -> Any:
    if redis_ver >= (7,) and (isinstance(x, float) or isinstance(x, int)):
        return 0 + x

    return x


class Command:
    def __init__(self, *args):
        args = list(flatten(args))
        args = [default_normalize(x) for x in args]
        self.args = tuple(args)

    def __repr__(self):
        parts = [repr(arg) for arg in self.args]
        return "Command({})".format(", ".join(parts))

    @staticmethod
    def encode(arg):
        encoder = redis.connection.Encoder("utf-8", "replace", False)
        return encoder.encode(arg)

    @property
    def normalize(self):
        command = self.encode(self.args[0]).lower() if self.args else None
        # Functions that return a list in arbitrary order
        unordered = {b"keys", b"sort", b"hgetall", b"hkeys", b"hvals", b"sdiff", b"sinter", b"sunion", b"smembers"}
        if command in unordered:
            return sort_list
        else:
            return normalize_if_number

    @property
    def testable(self):
        """Whether this command is suitable for a test.

        The fuzzer can create commands with behaviour that is
        non-deterministic, not supported, or which hits redis bugs.
        """
        N = len(self.args)
        if N == 0:
            return False
        command = self.encode(self.args[0]).lower()
        if not command.split():
            return False
        if command == b"keys" and N == 2 and self.args[1] != b"*":
            return False
        # Redis will ignore a NULL character in some commands but not others,
        # e.g., it recognises EXEC\0 but not MULTI\00.
        # Rather than try to reproduce this quirky behavior, just skip these tests.
        if b"\0" in command:
            return False
        return True


def commands(*args, **kwargs):
    return st.builds(functools.partial(Command, **kwargs), *args)


# # TODO: all expiry-related commands
common_commands = (
    commands(st.sampled_from(["del", "persist", "type", "unlink"]), keys)
    | commands(st.just("exists"), st.lists(keys))
    | commands(st.just("keys"), st.just("*"))
    # Disabled for now due to redis giving wrong answers
    # (https://github.com/antirez/redis/issues/5632)
    # | commands(st.just('keys'), patterns)
    | commands(st.just("move"), keys, dbnums)
    | commands(st.sampled_from(["rename", "renamenx"]), keys, keys)
    # TODO: find a better solution to sort instability than throwing
    #  away the sort entirely with normalize. This also prevents us
    #  using LIMIT.
    | commands(
        st.just("sort"), keys, st.none() | st.just("asc"), st.none() | st.just("desc"), st.none() | st.just("alpha")
    )
)


def build_zstore(command, dest, sources, weights, aggregate):
    args = [command, dest, len(sources)]
    args += [source[0] for source in sources]
    if weights:
        args.append("weights")
        args += [source[1] for source in sources]
    if aggregate:
        args += ["aggregate", aggregate]
    return Command(args)


zset_no_score_create_commands = commands(st.just("zadd"), keys, st.lists(st.tuples(st.just(0), fields), min_size=1))
zset_no_score_commands = (  # TODO: test incr
    commands(
        st.just("zadd"),
        keys,
        st.none() | st.just("nx"),
        st.none() | st.just("xx"),
        st.none() | st.just("ch"),
        st.none() | st.just("incr"),
        st.lists(st.tuples(st.just(0), fields)),
    )
    | commands(st.just("zlexcount"), keys, string_tests, string_tests)
    | commands(st.sampled_from(["zrangebylex", "zrevrangebylex"]), keys, string_tests, string_tests, limits)
    | commands(st.just("zremrangebylex"), keys, string_tests, string_tests)
)

bad_commands = (
    # redis-py splits the command on spaces, and hangs if that ends up
    # being an empty list
    commands(st.text().filter(lambda x: bool(x.split())), st.lists(st.binary() | st.text()))
)

attrs = st.fixed_dictionaries(
    {
        "keys": st.lists(st.binary(), min_size=2, max_size=5, unique=True),
        "fields": st.lists(st.binary(), min_size=2, max_size=5, unique=True),
        "values": st.lists(st.binary() | int_as_bytes | float_as_bytes, min_size=2, max_size=5, unique=True),
        "scores": st.lists(st.floats(width=32), min_size=2, max_size=5, unique=True),
    }
)


@hypothesis.settings(max_examples=1000)
class CommonMachine(hypothesis.stateful.RuleBasedStateMachine):
    create_command_strategy = st.nothing()

    def __init__(self):
        super().__init__()
        try:
            self.real = redis.StrictRedis("localhost", port=6380, db=2)
            self.real.ping()
        except redis.ConnectionError:
            pytest.skip("redis is not running")
        if self.real.info("server").get("arch_bits") != 64:
            self.real.connection_pool.disconnect()
            pytest.skip("redis server is not 64-bit")
        self.fake = fakeredis.FakeStrictRedis(server=fakeredis.FakeServer(version=redis_ver), port=6380, db=2)
        # Disable the response parsing so that we can check the raw values returned
        self.fake.response_callbacks.clear()
        self.real.response_callbacks.clear()
        self.transaction_normalize = []
        self.keys = []
        self.fields = []
        self.values = []
        self.scores = []
        self.initialized_data = False
        try:
            self.real.execute_command("discard")
        except redis.ResponseError:
            pass
        self.real.flushall()

    def teardown(self):
        self.real.connection_pool.disconnect()
        self.fake.connection_pool.disconnect()
        super().teardown()

    @staticmethod
    def _evaluate(client, command):
        try:
            result = client.execute_command(*command.args)
            if result != "QUEUED":
                result = command.normalize(result)
            exc = None
        except Exception as e:
            result = exc = e
        return wrap_exceptions(result), exc

    def _compare(self, command):
        fake_result, fake_exc = self._evaluate(self.fake, command)
        real_result, real_exc = self._evaluate(self.real, command)

        if fake_exc is not None and real_exc is None:
            print("{} raised on only on fake when running {}".format(fake_exc, command), file=sys.stderr)
            raise fake_exc
        elif real_exc is not None and fake_exc is None:
            assert real_exc == fake_exc, "Expected exception {} not raised".format(real_exc)
        elif real_exc is None and isinstance(real_result, list) and command.args and command.args[0].lower() == "exec":
            assert fake_result is not None
            # Transactions need to use the normalize functions of the
            # component commands.
            assert len(self.transaction_normalize) == len(real_result)
            assert len(self.transaction_normalize) == len(fake_result)
            for n, r, f in zip(self.transaction_normalize, real_result, fake_result):
                assert n(f) == n(r)
            self.transaction_normalize = []
        else:
            assert fake_result == real_result or (
                type(fake_result) is float and fake_result == pytest.approx(real_result)
            ), "Discrepancy when running command {}, fake({}) != real({})".format(command, fake_result, real_result)
            if real_result == b"QUEUED":
                # Since redis removes the distinction between simple strings and
                # bulk strings, this might not actually indicate that we're in a
                # transaction. But it is extremely unlikely that hypothesis will
                # find such examples.
                self.transaction_normalize.append(command.normalize)
        if len(command.args) == 1 and Command.encode(command.args[0]).lower() in (b"discard", b"exec"):
            self.transaction_normalize = []

    @initialize(attrs=attrs)
    def init_attrs(self, attrs):
        for key, value in attrs.items():
            setattr(self, key, value)

    # hypothesis doesn't allow ordering of @initialize, so we have to put
    # preconditions on rules to ensure we call init_data exactly once and
    # after init_attrs.
    @precondition(lambda self: not self.initialized_data)
    @rule(commands=self_strategy.flatmap(lambda self: st.lists(self.create_command_strategy)))
    def init_data(self, commands):
        for command in commands:
            self._compare(command)
        self.initialized_data = True

    @precondition(lambda self: self.initialized_data)
    @rule(command=self_strategy.flatmap(lambda self: self.command_strategy))
    def one_command(self, command):
        self._compare(command)


class BaseTest:
    """Base class for test classes."""

    command_strategy: SearchStrategy
    create_command_strategy = st.nothing()

    @pytest.mark.slow
    def test(self):
        class Machine(CommonMachine):
            create_command_strategy = self.create_command_strategy
            command_strategy = self.command_strategy

        # hypothesis.settings.register_profile("debug", max_examples=10, verbosity=hypothesis.Verbosity.debug)
        # hypothesis.settings.load_profile("debug")
        hypothesis.stateful.run_state_machine_as_test(Machine)


class TestConnection(BaseTest):
    # TODO: tests for select
    connection_commands = (
        commands(st.just("echo"), values)
        | commands(st.just("ping"), st.lists(values, max_size=2))
        | commands(st.just("swapdb"), dbnums, dbnums)
    )
    command_strategy = connection_commands | common_commands


class TestString(BaseTest):
    string_commands = (
        commands(st.just("append"), keys, values)
        | commands(st.just("bitcount"), keys)
        | commands(st.just("bitcount"), keys, values, values)
        | commands(st.sampled_from(["incr", "decr"]), keys)
        | commands(st.sampled_from(["incrby", "decrby"]), keys, values)
        | commands(st.just("get"), keys)
        | commands(st.just("getbit"), keys, counts)
        | commands(st.just("setbit"), keys, counts, st.integers(min_value=0, max_value=1) | st.integers())
        | commands(st.sampled_from(["substr", "getrange"]), keys, counts, counts)
        | commands(st.just("getset"), keys, values)
        | commands(st.just("mget"), st.lists(keys))
        | commands(st.sampled_from(["mset", "msetnx"]), st.lists(st.tuples(keys, values)))
        | commands(
            st.just("set"),
            keys,
            values,
            st.none() | st.just("nx"),
            st.none() | st.just("xx"),
            st.none() | st.just("keepttl"),
        )
        | commands(st.just("setex"), keys, expires_seconds, values)
        | commands(st.just("psetex"), keys, expires_ms, values)
        | commands(st.just("setnx"), keys, values)
        | commands(st.just("setrange"), keys, counts, values)
        | commands(st.just("strlen"), keys)
    )
    create_command_strategy = commands(st.just("set"), keys, values)
    command_strategy = string_commands | common_commands


class TestHash(BaseTest):
    hash_commands = (
        commands(st.just("hset"), keys, st.lists(st.tuples(fields, values)))
        | commands(st.just("hdel"), keys, st.lists(fields))
        | commands(st.just("hexists"), keys, fields)
        | commands(st.just("hget"), keys, fields)
        | commands(st.sampled_from(["hgetall", "hkeys", "hvals"]), keys)
        | commands(st.just("hincrby"), keys, fields, st.integers())
        | commands(st.just("hlen"), keys)
        | commands(st.just("hmget"), keys, st.lists(fields))
        | commands(st.just("hset"), keys, st.lists(st.tuples(fields, values)))
        | commands(st.just("hsetnx"), keys, fields, values)
        | commands(st.just("hstrlen"), keys, fields)
    )
    create_command_strategy = commands(st.just("hset"), keys, st.lists(st.tuples(fields, values), min_size=1))
    command_strategy = hash_commands | common_commands


class TestList(BaseTest):
    # TODO: blocking commands
    list_commands = (
        commands(st.just("lindex"), keys, counts)
        | commands(
            st.just("linsert"),
            keys,
            st.sampled_from(["before", "after", "BEFORE", "AFTER"]) | st.binary(),
            values,
            values,
        )
        | commands(st.just("llen"), keys)
        | commands(st.sampled_from(["lpop", "rpop"]), keys, st.just(None) | st.just([]) | st.integers())
        | commands(st.sampled_from(["lpush", "lpushx", "rpush", "rpushx"]), keys, st.lists(values))
        | commands(st.just("lrange"), keys, counts, counts)
        | commands(st.just("lrem"), keys, counts, values)
        | commands(st.just("lset"), keys, counts, values)
        | commands(st.just("ltrim"), keys, counts, counts)
        | commands(st.just("rpoplpush"), keys, keys)
    )
    create_command_strategy = commands(st.just("rpush"), keys, st.lists(values, min_size=1))
    command_strategy = list_commands | common_commands


class TestSet(BaseTest):
    set_commands = (
        commands(
            st.just("sadd"),
            keys,
            st.lists(
                fields,
            ),
        )
        | commands(st.just("scard"), keys)
        | commands(st.sampled_from(["sdiff", "sinter", "sunion"]), st.lists(keys))
        | commands(st.sampled_from(["sdiffstore", "sinterstore", "sunionstore"]), keys, st.lists(keys))
        | commands(st.just("sismember"), keys, fields)
        | commands(st.just("smembers"), keys)
        | commands(st.just("smove"), keys, keys, fields)
        | commands(st.just("srem"), keys, st.lists(fields))
    )
    # TODO:
    # - find a way to test srandmember, spop which are random
    # - sscan
    create_command_strategy = commands(st.just("sadd"), keys, st.lists(fields, min_size=1))
    command_strategy = set_commands | common_commands


class TestZSet(BaseTest):
    zset_commands = (
        commands(
            st.just("zadd"),
            keys,
            st.none() | st.just("nx"),
            st.none() | st.just("xx"),
            st.none() | st.just("ch"),
            st.none() | st.just("incr"),
            st.lists(st.tuples(scores, fields)),
        )
        | commands(st.just("zcard"), keys)
        | commands(st.just("zcount"), keys, score_tests, score_tests)
        | commands(st.just("zincrby"), keys, scores, fields)
        | commands(st.sampled_from(["zrange", "zrevrange"]), keys, counts, counts, st.none() | st.just("withscores"))
        | commands(
            st.sampled_from(["zrangebyscore", "zrevrangebyscore"]),
            keys,
            score_tests,
            score_tests,
            limits,
            st.none() | st.just("withscores"),
        )
        | commands(st.sampled_from(["zrank", "zrevrank"]), keys, fields)
        | commands(st.just("zrem"), keys, st.lists(fields))
        | commands(st.just("zremrangebyrank"), keys, counts, counts)
        | commands(st.just("zremrangebyscore"), keys, score_tests, score_tests)
        | commands(st.just("zscore"), keys, fields)
        | st.builds(
            build_zstore,
            command=st.sampled_from(["zunionstore", "zinterstore"]),
            dest=keys,
            sources=st.lists(st.tuples(keys, float_as_bytes)),
            weights=st.booleans(),
            aggregate=st.sampled_from([None, "sum", "min", "max"]),
        )
    )
    # TODO: zscan, zpopmin/zpopmax, bzpopmin/bzpopmax, probably more
    create_command_strategy = commands(st.just("zadd"), keys, st.lists(st.tuples(scores, fields), min_size=1))
    command_strategy = zset_commands | common_commands


class TestZSetNoScores(BaseTest):
    create_command_strategy = zset_no_score_create_commands
    command_strategy = zset_no_score_commands | common_commands


class TestTransaction(BaseTest):
    transaction_commands = (
        commands(st.sampled_from(["multi", "discard", "exec", "unwatch"]))
        | commands(st.just("watch"), keys)
        | commands(st.just("append"), keys, values)
        | commands(st.just("bitcount"), keys)
        | commands(st.just("bitcount"), keys, values, values)
        | commands(st.sampled_from(["incr", "decr"]), keys)
        | commands(st.sampled_from(["incrby", "decrby"]), keys, values)
        | commands(st.just("get"), keys)
        | commands(st.just("getbit"), keys, counts)
        | commands(st.just("setbit"), keys, counts, st.integers(min_value=0, max_value=1) | st.integers())
        | commands(st.sampled_from(["substr", "getrange"]), keys, counts, counts)
        | commands(st.just("getset"), keys, values)
        | commands(st.just("mget"), st.lists(keys))
        | commands(st.sampled_from(["mset", "msetnx"]), st.lists(st.tuples(keys, values)))
        | commands(
            st.just("set"),
            keys,
            values,
            st.none() | st.just("nx"),
            st.none() | st.just("xx"),
            st.none() | st.just("keepttl"),
        )
        | commands(st.just("setex"), keys, expires_seconds, values)
        | commands(st.just("psetex"), keys, expires_ms, values)
        | commands(st.just("setnx"), keys, values)
        | commands(st.just("setrange"), keys, counts, values)
        | commands(st.just("strlen"), keys)
    )
    create_command_strategy = TestString.create_command_strategy
    command_strategy = transaction_commands | common_commands


class TestServer(BaseTest):
    # TODO: real redis raises an error if there is a save already in progress.
    #  Find a better way to test this. commands(st.just('bgsave'))
    server_commands = (
        commands(st.just("dbsize"))
        | commands(st.sampled_from(["flushdb", "flushall"]), st.sampled_from([[], "async"]))
        # TODO: result is non-deterministic
        # | commands(st.just('lastsave'))
        | commands(st.just("save"))
    )
    create_command_strategy = TestString.create_command_strategy
    command_strategy = server_commands | TestString.string_commands | common_commands


class TestJoint(BaseTest):
    create_command_strategy = (
        TestString.create_command_strategy
        | TestHash.create_command_strategy
        | TestList.create_command_strategy
        | TestSet.create_command_strategy
        | TestZSet.create_command_strategy
    )
    command_strategy = (
        TestServer.server_commands
        | TestConnection.connection_commands
        | TestString.string_commands
        | TestHash.hash_commands
        | TestList.list_commands
        | TestSet.set_commands
        | TestZSet.zset_commands
        | common_commands
        | bad_commands
    )


@st.composite
def delete_arg(draw, commands):
    command = draw(commands)
    if command.args:
        pos = draw(st.integers(min_value=0, max_value=len(command.args) - 1))
        command.args = command.args[:pos] + command.args[pos + 1 :]
    return command


@st.composite
def command_args(draw, commands):
    """Generate an argument from some command"""
    command = draw(commands)
    hypothesis.assume(len(command.args))
    return draw(st.sampled_from(command.args))


def mutate_arg(draw, commands, mutate):
    command = draw(commands)
    if command.args:
        pos = draw(st.integers(min_value=0, max_value=len(command.args) - 1))
        arg = mutate(Command.encode(command.args[pos]))
        command.args = command.args[:pos] + (arg,) + command.args[pos + 1 :]
    return command


@st.composite
def replace_arg(draw, commands, replacements):
    return mutate_arg(draw, commands, lambda arg: draw(replacements))


@st.composite
def uppercase_arg(draw, commands):
    return mutate_arg(draw, commands, lambda arg: arg.upper())


@st.composite
def prefix_arg(draw, commands, prefixes):
    return mutate_arg(draw, commands, lambda arg: draw(prefixes) + arg)


@st.composite
def suffix_arg(draw, commands, suffixes):
    return mutate_arg(draw, commands, lambda arg: arg + draw(suffixes))


@st.composite
def add_arg(draw, commands, arguments):
    command = draw(commands)
    arg = draw(arguments)
    pos = draw(st.integers(min_value=0, max_value=len(command.args)))
    command.args = command.args[:pos] + (arg,) + command.args[pos:]
    return command


@st.composite
def swap_args(draw, commands):
    command = draw(commands)
    if len(command.args) >= 2:
        pos1 = draw(st.integers(min_value=0, max_value=len(command.args) - 1))
        pos2 = draw(st.integers(min_value=0, max_value=len(command.args) - 1))
        hypothesis.assume(pos1 != pos2)
        args = list(command.args)
        arg1 = args[pos1]
        arg2 = args[pos2]
        args[pos1] = arg2
        args[pos2] = arg1
        command.args = tuple(args)
    return command


def mutated_commands(commands):
    args = st.sampled_from(
        [b"withscores", b"xx", b"nx", b"ex", b"px", b"weights", b"aggregate", b"", b"0", b"-1", b"nan", b"inf", b"-inf"]
    ) | command_args(commands)
    affixes = st.sampled_from([b"\0", b"-", b"+", b"\t", b"\n", b"0000"]) | st.binary()
    return st.recursive(
        commands,
        lambda x: delete_arg(x)
        | replace_arg(x, args)
        | uppercase_arg(x)
        | prefix_arg(x, affixes)
        | suffix_arg(x, affixes)
        | add_arg(x, args)
        | swap_args(x),
    )


class TestFuzz(BaseTest):
    command_strategy = mutated_commands(TestJoint.command_strategy)
    command_strategy = command_strategy.filter(lambda command: command.testable)
