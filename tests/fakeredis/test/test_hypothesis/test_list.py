import hypothesis.strategies as st

from test.test_hypothesis.base import (
    BaseTest,
    commands,
    values,
    keys,
    common_commands,
    counts,
    ints,
)


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
        | commands(
            st.sampled_from(["lpop", "rpop"]),
            keys,
            st.just(None) | st.just([]) | ints,
        )
        | commands(
            st.sampled_from(["lpush", "lpushx", "rpush", "rpushx"]),
            keys,
            st.lists(values),
        )
        | commands(st.just("lrange"), keys, counts, counts)
        | commands(st.just("lrem"), keys, counts, values)
        | commands(st.just("lset"), keys, counts, values)
        | commands(st.just("ltrim"), keys, counts, counts)
        | commands(st.just("rpoplpush"), keys, keys)
    )
    create_command_strategy = commands(
        st.just("rpush"), keys, st.lists(values, min_size=1)
    )
    command_strategy = list_commands | common_commands
