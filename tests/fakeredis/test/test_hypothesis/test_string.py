import hypothesis.strategies as st

from test.test_hypothesis.base import (
    BaseTest,
    commands,
    values,
    keys,
    common_commands,
    counts,
    int_as_bytes,
    zero_or_more,
    ints,
    expires_seconds,
    expires_ms,
)

optional_bitcount_range = st.just(()) | st.tuples(int_as_bytes, int_as_bytes)


class TestString(BaseTest):
    string_commands = (
        commands(st.just("append"), keys, values)
        | commands(st.just("bitcount"), keys, optional_bitcount_range)
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
    create_command_strategy = commands(st.just("set"), keys, values)
    command_strategy = string_commands | common_commands
