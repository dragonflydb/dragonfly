import pytest


def dfly_args(*args):
    """ used to define a singular set of arguments for dragonfly test """
    return pytest.mark.parametrize("df_server", [args], indirect=True)


def dfly_multi_test_args(*args):
    """ used to define multiple sets of arguments to test multiple dragonfly configurations """
    return pytest.mark.parametrize("df_server", args, indirect=True)
