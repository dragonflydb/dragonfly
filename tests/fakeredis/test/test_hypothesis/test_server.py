import hypothesis.strategies as st

from test.test_hypothesis.base import (
    BaseTest,
    commands,
    common_commands,
    keys,
    values,
)
from test.test_hypothesis.test_string import string_commands


class TestServer(BaseTest):
    # TODO: real redis raises an error if there is a save already in progress.
    #  Find a better way to test this. commands(st.just('bgsave'))
    server_commands = (
        commands(st.just("dbsize"))
        | commands(st.sampled_from(["flushdb", "flushall"]))
        # TODO: result is non-deterministic
        # | commands(st.just('lastsave'))
        | commands(st.just("save"))
    )
    create_command_strategy = commands(st.just("set"), keys, values)
    command_strategy = server_commands | string_commands | common_commands
