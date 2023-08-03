import pytest
import time
import subprocess
import aiohttp
import logging
import os
from typing import Optional
from prometheus_client.parser import text_string_to_metric_families
from redis.asyncio import Redis as RedisClient

from dataclasses import dataclass

START_DELAY = 0.4
START_GDB_DELAY = 3.0


@dataclass
class DflyParams:
    path: str
    cwd: str
    gdb: bool
    args: list
    existing_port: int
    existing_admin_port: int
    existing_mc_port: int
    env: any


class DflyStartException(Exception):
    pass


class DflyInstance:
    """
    Represents a runnable and stoppable Dragonfly instance
    with fixed arguments.
    """

    def __init__(self, params: DflyParams, args):
        self.args = args
        self.params = params
        self.proc: Optional[subprocess.Popen] = None
        self._client: Optional[RedisClient] = None

        self.dynamic_port = False
        if self.params.existing_port:
            self._port = self.params.existing_port
        elif "port" in self.args:
            self._port = int(self.args["port"])
        else:
            self.args["random_port"] = None
            self._port = None
            self.dynamic_port = True

    def client(self, *args, **kwargs) -> RedisClient:
        return RedisClient(port=self.port, *args, **kwargs)

    def start(self):
        if self.params.existing_port:
            return

        self._start()
        self._wait_for_server()

    def _wait_for_server(self):
        # Give Dragonfly time to start and detect possible failure causes
        # Gdb starts slowly
        delay = START_DELAY if not self.params.gdb else START_GDB_DELAY

        # Wait until the process is listening on the port.
        s = time.time()
        while time.time() - s < delay:
            self._check_status()
            try:
                self.get_port_from_lsof()
                break
            except RuntimeError:
                time.sleep(0.05)
        else:
            raise DflyStartException("Process didn't start listening on port in time")

    def stop(self, kill=False):
        proc, self.proc = self.proc, None
        if proc is None:
            return

        logging.debug(f"Stopping instance on {self._port}")
        try:
            if kill:
                proc.kill()
            else:
                proc.terminate()
            proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            raise Exception("Unable to terminate DragonflyDB gracefully, it was killed")

    def _start(self):
        if self.params.existing_port:
            return

        if self.dynamic_port:
            self._port = None

        base_args = ["--use_zset_tree"] + [f"--{v}" for v in self.params.args]
        all_args = self.format_args(self.args) + base_args
        logging.debug(f"Starting instance with arguments {all_args} from {self.params.path}")

        run_cmd = [self.params.path, *all_args]
        if self.params.gdb:
            run_cmd = ["gdb", "--ex", "r", "--args"] + run_cmd
        self.proc = subprocess.Popen(run_cmd, cwd=self.params.cwd)

    def _check_status(self):
        if not self.params.existing_port:
            return_code = self.proc.poll()
            if return_code is not None:
                raise DflyStartException(f"Failed to start instance, return code {return_code}")

    def __getitem__(self, k):
        return self.args.get(k)

    @property
    def port(self) -> int:
        if self._port is None:
            self._port = self.get_port_from_lsof()
        return self._port

    @property
    def admin_port(self) -> Optional[int]:
        if self.params.existing_admin_port:
            return self.params.existing_admin_port
        if "admin_port" in self.args:
            return int(self.args["admin_port"])
        return None

    @property
    def mc_port(self) -> Optional[int]:
        if self.params.existing_mc_port:
            return self.params.existing_mc_port
        if "memcached_port" in self.args:
            return int(self.args["memcached_port"])
        return None

    def get_port_from_lsof(self) -> int:
        if self.proc is None:
            raise RuntimeError("port is not available yet")
        try:
            lsof_output = subprocess.check_output(
                ["lsof", "-i", "-a", "-p", str(self.proc.pid), "-sTCP:LISTEN", "-P", "-F", "n"],
                stderr=subprocess.DEVNULL,
            )
        except subprocess.CalledProcessError:
            raise RuntimeError("lsof problem")
        ports = set()
        for line in lsof_output.split(b"\n"):
            if line.startswith(b"n*:"):
                ports.add(int(line[3:]))
        ports.difference_update({self.admin_port, self.mc_port})
        assert len(ports) < 2, "Open ports detection found too many ports"
        if ports:
            return ports.pop()
        raise RuntimeError("Couldn't parse port")

    @staticmethod
    def format_args(args):
        out = []
        for k, v in args.items():
            if v is not None:
                out.append(f"--{k}={v}")
            else:
                out.append(f"--{k}")
        return out

    async def metrics(self):
        session = aiohttp.ClientSession()
        resp = await session.get(f"http://localhost:{self.port}/metrics")
        data = await resp.text()
        await session.close()
        return {
            metric_family.name: metric_family
            for metric_family in text_string_to_metric_families(data)
        }


class DflyInstanceFactory:
    """
    A factory for creating dragonfly instances with pre-supplied arguments.
    """

    def __init__(self, params: DflyParams, args):
        self.args = args
        self.params = params
        self.instances = []

    def create(self, **kwargs) -> DflyInstance:
        args = {**self.args, **kwargs}
        args.setdefault("dbfilename", "")
        for k, v in args.items():
            args[k] = v.format(**self.params.env) if isinstance(v, str) else v

        instance = DflyInstance(self.params, args)
        self.instances.append(instance)
        return instance

    def start_all(self, instances):
        """Start multiple instances in parallel"""
        for instance in instances:
            instance._start()

        for instance in instances:
            instance._wait_for_server()

    def stop_all(self):
        """Stop all lanched instances."""
        for instance in self.instances:
            instance.stop()

    def __str__(self):
        return f"Factory({self.args})"


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
