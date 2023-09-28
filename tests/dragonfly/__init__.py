import pytest


def dfly_args(*args):
    """Used to define a singular set of arguments for dragonfly test"""
    return pytest.mark.parametrize("df_factory", args, indirect=True)


def dfly_multi_test_args(*args):
    """Used to define multiple sets of arguments to test multiple dragonfly configurations"""
    return pytest.mark.parametrize("df_factory", args, indirect=True)


class PortPicker:
    """A simple port manager to allocate available ports for tests"""

    def __init__(self):
        self.next_port = 5555

    def get_available_port(self):
        while not self.is_port_available(self.next_port):
            self.next_port += 1
        self.next_port += 1
        return self.next_port - 1

    def is_port_available(self, port):
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(("localhost", port)) != 0
