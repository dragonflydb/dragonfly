import pytest


def dfly_args(*args):
    """Used to define a singular set of arguments for dragonfly test"""
    return pytest.mark.parametrize("df_factory", args, indirect=True)


def dfly_multi_test_args(*args):
    """Used to define multiple sets of arguments to test multiple dragonfly configurations"""
    return pytest.mark.parametrize("df_factory", args, indirect=True)


class PortPicker:
    """A simple port manager to allocate available ports for tests"""

    MIN_PORT = 5555
    # Kept below the kernel's ephemeral port range (starts at 32768 by default on Linux, see
    # /proc/sys/net/ipv4/ip_local_port_range). A long-running repeat job that never wraps its
    # counter eventually climbs into that range and starts racing the OS's own automatic port
    # allocator for outbound connections, causing spurious "Address already in use" bind failures.
    MAX_PORT = 30000

    def __init__(self):
        self.next_port = self.MIN_PORT

    def get_available_port(self):
        while not self.is_port_available(self.next_port):
            self._advance()
        port = self.next_port
        self._advance()
        return port

    def _advance(self):
        self.next_port += 1
        if self.next_port > self.MAX_PORT:
            self.next_port = self.MIN_PORT

    def is_port_available(self, port):
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(("localhost", port)) != 0
