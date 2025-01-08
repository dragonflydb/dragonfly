import hypothesis.strategies as st

from test.test_hypothesis.base import BaseTest, common_commands, commands
from test.test_hypothesis.test_connection import TestConnection
from test.test_hypothesis.test_hash import TestHash
from test.test_hypothesis.test_list import TestList
from test.test_hypothesis.test_server import TestServer
from test.test_hypothesis.test_set import TestSet
from test.test_hypothesis.test_string import TestString, string_commands
from test.test_hypothesis.test_zset import TestZSet

bad_commands = (
    # redis-py splits the command on spaces, and hangs if that ends up being an empty list
    commands(
        st.text().filter(lambda x: bool(x.split())), st.lists(st.binary() | st.text())
    )
)


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
        | string_commands
        | TestHash.hash_commands
        | TestList.list_commands
        | TestSet.set_commands
        | TestZSet.zset_commands
        | common_commands
        | bad_commands
    )
