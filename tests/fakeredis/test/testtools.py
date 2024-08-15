import importlib.util

import pytest
import redis
from packaging.version import Version

REDIS_VERSION = Version(redis.__version__)


def key_val_dict(size=100):
    return {f"key:{i}".encode(): f"val:{i}".encode() for i in range(size)}


def raw_command(r: redis.Redis, *args):
    """Like execute_command, but does not do command-specific response parsing"""
    response_callbacks = r.response_callbacks
    try:
        r.response_callbacks = {}
        return r.execute_command(*args)
    finally:
        r.response_callbacks = response_callbacks


ALLOWED_CONDITIONS = {"eq", "gte", "lte", "lt", "gt", "ne"}


def run_test_if_redispy_ver(condition: str, ver: str):
    if condition not in ALLOWED_CONDITIONS:
        raise ValueError(f"condition {condition} is not in allowed conditions ({ALLOWED_CONDITIONS})")
    cond = False
    cond = cond or condition == "eq" and REDIS_VERSION == Version(ver)
    cond = cond or condition == "gte" and REDIS_VERSION >= Version(ver)
    cond = cond or condition == "lte" and REDIS_VERSION <= Version(ver)
    cond = cond or condition == "lt" and REDIS_VERSION < Version(ver)
    cond = cond or condition == "gt" and REDIS_VERSION > Version(ver)
    cond = cond or condition == "ne" and REDIS_VERSION != Version(ver)
    return pytest.mark.skipif(
        not cond, reason=f"Test is not applicable to redis-py {REDIS_VERSION} ({condition}, {ver})"
    )


_lua_module = importlib.util.find_spec("lupa")
run_test_if_lupa = pytest.mark.skipif(_lua_module is None, reason="Test is only applicable if lupa is installed")

fake_only = pytest.mark.parametrize(
    "create_redis", [pytest.param("FakeStrictRedis", marks=pytest.mark.fake)], indirect=True
)
