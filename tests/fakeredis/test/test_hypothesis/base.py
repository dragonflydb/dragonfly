import functools
import math
import sys
from typing import Any, List, Tuple, Type, Optional

import fakeredis
import hypothesis
import hypothesis.stateful
import hypothesis.strategies as st
import pytest
import redis
from hypothesis.stateful import rule, initialize, precondition
from hypothesis.strategies import SearchStrategy

from ._server_info import redis_ver

self_strategy = st.runner()

MAX_INT = 2_147_483_647
MIN_INT = -2_147_483_648


@st.composite
def sample_attr(draw, name):
    """Strategy for sampling a specific attribute from a state machine"""
    machine = draw(self_strategy)
    values = getattr(machine, name)
    position = draw(st.integers(min_value=0, max_value=len(values) - 1))
    return values[position]


keys = sample_attr("keys")
fields = sample_attr("fields")
values = sample_attr("values")
scores = sample_attr("scores")

ints = st.integers(min_value=MIN_INT, max_value=MAX_INT)
int_as_bytes = st.builds(lambda x: str(_default_normalize(x)).encode(), ints)
float_as_bytes = st.builds(
    lambda x: repr(_default_normalize(x)).encode(), st.floats(width=32)
)
counts = st.integers(min_value=-3, max_value=3) | ints
# Redis has an integer overflow bug in swapdb, so we confine the numbers to
# a limited range (https://github.com/antirez/redis/issues/5737).
dbnums = st.integers(min_value=0, max_value=3) | st.integers(
    min_value=-1000, max_value=1000
)
# The filter is to work around https://github.com/antirez/redis/issues/5632
patterns = st.text(
    alphabet=st.sampled_from("[]^$*.?-azAZ\\\r\n\t")
) | st.binary().filter(lambda x: b"\0" not in x)

# Redis has integer overflow bugs in time computations, which is why we set a maximum.
expires_seconds = st.integers(min_value=100000, max_value=MAX_INT)
expires_ms = st.integers(min_value=100000000, max_value=MAX_INT)


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


def _wrap_exceptions(obj):
    if isinstance(obj, list):
        return [_wrap_exceptions(item) for item in obj]
    elif isinstance(obj, Exception):
        return WrappedException(obj)
    else:
        return obj


def _sort_list(lst):
    if isinstance(lst, list):
        return sorted(lst)
    else:
        return lst


def _normalize_if_number(x):
    try:
        res = float(x)
        return x if math.isnan(res) else res
    except ValueError:
        return x


def _flatten(args):
    if isinstance(args, (list, tuple)):
        for arg in args:
            yield from _flatten(arg)
    elif args is not None:
        yield args


def _default_normalize(x: Any) -> Any:
    if redis_ver >= (7,) and (isinstance(x, float) or isinstance(x, int)):
        return 0 + x

    return x


class Command:
    def __init__(self, *args):
        args = list(_flatten(args))
        args = [_default_normalize(x) for x in args]
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
        unordered = {
            b"keys",
            b"sort",
            b"hgetall",
            b"hkeys",
            b"hvals",
            b"sdiff",
            b"sinter",
            b"sunion",
            b"smembers",
        }
        if command in unordered:
            return _sort_list
        else:
            return _normalize_if_number

    @property
    def testable(self) -> bool:
        """Whether this command is suitable for a test.

        The fuzzer can create commands with behaviour that is non-deterministic, not supported, or which hits redis bugs.
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


def zero_or_more(*args) -> List[SearchStrategy]:
    return [st.none() | st.just(arg) for arg in args]


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
    | commands(st.just("sort"), keys, *zero_or_more("asc", "desc", "alpha"))
)

attrs = st.fixed_dictionaries(
    {
        "keys": st.lists(st.binary(), min_size=2, max_size=5, unique=True),
        "fields": st.lists(st.binary(), min_size=2, max_size=5, unique=True),
        "values": st.lists(
            st.binary() | int_as_bytes | float_as_bytes,
            min_size=2,
            max_size=5,
            unique=True,
        ),
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
        self.fake = fakeredis.FakeStrictRedis(
            server=fakeredis.FakeServer(version=redis_ver), port=6380, db=2
        )
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

    def teardown(self) -> None:
        self.real.connection_pool.disconnect()
        self.fake.connection_pool.disconnect()
        super().teardown()

    @staticmethod
    def _evaluate(
        client: redis.Redis, command
    ) -> Tuple[Any, Optional[Type[Exception]]]:
        try:
            result = client.execute_command(*command.args)
            if result != "QUEUED":
                result = command.normalize(result)
            exc = None
        except Exception as e:
            result = exc = e
        return _wrap_exceptions(result), exc

    def _compare(self, command: Command) -> None:
        fake_result, fake_exc = self._evaluate(self.fake, command)
        real_result, real_exc = self._evaluate(self.real, command)

        if fake_exc is not None and real_exc is None:
            print(
                f"{fake_exc} raised on only on fake when running {command}",
                file=sys.stderr,
            )
            raise fake_exc
        elif real_exc is not None and fake_exc is None:
            assert real_exc == fake_exc, f"Expected exception {real_exc} not raised"
        elif (
            real_exc is None
            and isinstance(real_result, list)
            and command.args
            and command.args[0].lower() == "exec"
        ):
            assert fake_result is not None
            # Transactions need to use the normalize functions of the component commands.
            assert len(self.transaction_normalize) == len(real_result)
            assert len(self.transaction_normalize) == len(fake_result)
            for n, r, f in zip(self.transaction_normalize, real_result, fake_result):
                assert n(f) == n(r)
            self.transaction_normalize = []
        else:
            assert fake_result == real_result or (
                type(fake_result) is float and fake_result == pytest.approx(real_result)
            ), f"Discrepancy when running command {command}, fake({fake_result}) != real({real_result})"
            if real_result == b"QUEUED":
                # Since redis removes the distinction between simple strings and
                # bulk strings, this might not actually indicate that we're in a
                # transaction. But it is extremely unlikely that hypothesis will
                # find such examples.
                self.transaction_normalize.append(command.normalize)
        if len(command.args) == 1 and Command.encode(command.args[0]).lower() in (
            b"discard",
            b"exec",
        ):
            self.transaction_normalize = []

    @initialize(attrs=attrs)
    def init_attrs(self, attrs):
        for key, value in attrs.items():
            setattr(self, key, value)

    # hypothesis doesn't allow ordering of @initialize, so we have to put
    # preconditions on rules to ensure we call init_data exactly once and
    # after init_attrs.
    @precondition(lambda self: not self.initialized_data)
    @rule(
        commands=self_strategy.flatmap(
            lambda self: st.lists(self.create_command_strategy)
        )
    )
    def init_data(self, commands) -> None:
        for command in commands:
            self._compare(command)
        self.initialized_data = True

    @precondition(lambda self: self.initialized_data)
    @rule(command=self_strategy.flatmap(lambda self: self.command_strategy))
    def one_command(self, command: Command) -> None:
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

        # hypothesis.settings.register_profile(
        #     "debug", max_examples=10, verbosity=hypothesis.Verbosity.debug
        # )
        hypothesis.settings.register_profile(
            "debug", verbosity=hypothesis.Verbosity.debug
        )
        hypothesis.settings.load_profile("debug")
        hypothesis.stateful.run_state_machine_as_test(Machine)
