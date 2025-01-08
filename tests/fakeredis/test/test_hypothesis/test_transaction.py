import hypothesis.strategies as st

from test.test_hypothesis.base import (
    BaseTest,
    commands,
    values,
    keys,
    common_commands,
    counts,
    zero_or_more,
    ints,
    expires_seconds,
    expires_ms,
)
from test.test_hypothesis.test_string import TestString


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
        | commands(
            st.just("setbit"),
            keys,
            counts,
            st.integers(min_value=0, max_value=1) | ints,
        )
        | commands(st.sampled_from(["substr", "getrange"]), keys, counts, counts)
        | commands(st.just("getset"), keys, values)
        | commands(st.just("mget"), st.lists(keys))
        | commands(
            st.sampled_from(["mset", "msetnx"]), st.lists(st.tuples(keys, values))
        )
        | commands(
            st.just("set"),
            keys,
            values,
            *zero_or_more("nx", "xx", "keepttl"),
        )
        | commands(st.just("setex"), keys, expires_seconds, values)
        | commands(st.just("psetex"), keys, expires_ms, values)
        | commands(st.just("setnx"), keys, values)
        | commands(st.just("setrange"), keys, counts, values)
        | commands(st.just("strlen"), keys)
    )
    create_command_strategy = TestString.create_command_strategy
    command_strategy = transaction_commands | common_commands
