import hypothesis.strategies as st

from test.test_hypothesis.base import (
    BaseTest,
    commands,
    keys,
    common_commands,
    fields,
)


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
    create_command_strategy = commands(
        st.just("sadd"), keys, st.lists(fields, min_size=1)
    )
    command_strategy = set_commands | common_commands
