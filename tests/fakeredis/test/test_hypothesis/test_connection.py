import hypothesis.strategies as st

from test.test_hypothesis.base import BaseTest, commands, values, common_commands


class TestConnection(BaseTest):
    # TODO: tests for select
    connection_commands = (
        commands(st.just("echo"), values)
        | commands(st.just("ping"), st.lists(values, max_size=2))
        # | commands(st.just("swapdb"), dbnums, dbnums)
    )
    command_strategy = connection_commands | common_commands
