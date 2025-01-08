from test.test_hypothesis.base import (
    BaseTest,
    commands,
    values,
    keys,
    common_commands,
    counts,
    int_as_bytes,
    fields,
)
import hypothesis.strategies as st


class TestSet(BaseTest):
    set_commands = (
        commands(st.just("sadd"), keys, st.lists(fields))
        | commands(st.just("scard"), keys)
        | commands(st.sampled_from(["sdiff", "sinter", "sunion"]), st.lists(keys))
        | commands(
            st.sampled_from(["sdiffstore", "sinterstore", "sunionstore"]),
            keys,
            st.lists(keys),
        )
        | commands(st.just("sismember"), keys, fields)
        | commands(st.just("smembers"), keys)
        | commands(st.just("smove"), keys, keys, fields)
        | commands(st.just("srem"), keys, st.lists(fields))
    )
    # TODO:
    # - find a way to test srandmember, spop which are random
    # - sscan
    create_command_strategy = commands(
        st.just("sadd"), keys, st.lists(fields, min_size=1)
    )
    command_strategy = set_commands | common_commands
