"""
Tests that --proactor_threads is respected even inside containers with CPU limits.

Background: UpdateResourceLimitsIfInsideContainer() reads the cgroup CPU quota and
passes it as pool_size to ProactorPool. When pool_size != 0, helio's ProactorPool
ignores FLAGS_proactor_threads entirely. The fix resets pool_size to 0 when
--proactor_threads (or DFLY_proactor_threads) is explicitly set, so the flag wins.
"""

import pytest
from .instance import DflyInstanceFactory


@pytest.mark.asyncio
async def test_proactor_threads_flag_is_respected(df_factory: DflyInstanceFactory):
    """Starting with --proactor_threads=N must result in exactly N threads."""
    server = df_factory.create(proactor_threads=3)
    server.start()
    client = server.client()
    try:
        info = await client.info("server")
        assert int(info["thread_count"]) == 3
    finally:
        await client.aclose()
        server.stop()


@pytest.mark.asyncio
async def test_proactor_threads_env_var_is_respected(df_factory: DflyInstanceFactory, monkeypatch):
    """DFLY_proactor_threads env var must behave identically to the CLI flag.

    The subprocess inherits the parent environment, so setting the variable here
    is equivalent to setting it in a Kubernetes pod's env: section.
    """
    monkeypatch.setenv("DFLY_proactor_threads", "2")
    # No proactor_threads kwarg — only the env var should drive the thread count.
    server = df_factory.create()
    server.start()
    client = server.client()
    try:
        info = await client.info("server")
        assert int(info["thread_count"]) == 2
    finally:
        await client.aclose()
        server.stop()


@pytest.mark.asyncio
async def test_proactor_threads_flag_overrides_env_var(
    df_factory: DflyInstanceFactory, monkeypatch
):
    """CLI flag takes priority over DFLY_proactor_threads env var.

    ParseFlagsFromEnv() skips env vars when the flag was already set on the
    command line (WasPresentOnCommandLine check), so the CLI value must win.
    """
    monkeypatch.setenv("DFLY_proactor_threads", "2")
    server = df_factory.create(proactor_threads=4)
    server.start()
    client = server.client()
    try:
        info = await client.info("server")
        assert int(info["thread_count"]) == 4
    finally:
        await client.aclose()
        server.stop()
