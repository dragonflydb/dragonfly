import hypothesis.strategies as st

from .. import test_hypothesis as tests
from ..test_hypothesis.base import BaseTest, common_commands, commands
from ..test_hypothesis.test_string import string_commands

bad_commands = (
    # redis-py splits the command on spaces, and hangs if that ends up being an empty list
    commands(
        st.text().filter(lambda x: bool(x.split())), st.lists(st.binary() | st.text())
    )
)


class TestJoint(BaseTest):
    create_command_strategy = (
        tests.TestString.create_command_strategy
        | tests.TestHash.create_command_strategy
        | tests.TestList.create_command_strategy
        | tests.TestSet.create_command_strategy
        | tests.TestZSet.create_command_strategy
    )
    command_strategy = (
        tests.TestServer.server_commands
        | tests.TestConnection.connection_commands
        | string_commands
        | tests.TestHash.hash_commands
        | tests.TestList.list_commands
        | tests.TestSet.set_commands
        | tests.TestZSet.zset_commands
        | common_commands
        | bad_commands
    )
