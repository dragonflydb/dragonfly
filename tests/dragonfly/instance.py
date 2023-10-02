import time
import subprocess
import aiohttp
import logging
from dataclasses import dataclass
from typing import Dict, Optional, List, Union
import re
import psutil
from prometheus_client.parser import text_string_to_metric_families
from redis.asyncio import Redis as RedisClient


START_DELAY = 0.8
START_GDB_DELAY = 5.0


@dataclass
class DflyParams:
    path: str
    cwd: str
    gdb: bool
    args: Dict[str, Union[str, None]]
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
        self.args.update(params.args)
        self.params = params
        self.proc: Optional[subprocess.Popen] = None
        self._client: Optional[RedisClient] = None
        self.log_files: List[str] = []

        self.dynamic_port = False
        if self.params.existing_port:
            self._port = self.params.existing_port
        elif "port" in self.args:
            self._port = int(self.args["port"])
        else:
            # Tell DF to choose a random open port.
            # We'll find out what port it is using lsof.
            self.args["port"] = -1
            self._port = None
            self.dynamic_port = True

        # Some tests check the log files, so make sure the log files
        # exist even when people try to debug their test.
        if "logtostderr" in self.args:
            del self.args["logtostderr"]
            self.args["alsologtostderr"] = None

    def __del__(self):
        assert self.proc == None

    def client(self, *args, **kwargs) -> RedisClient:
        return RedisClient(port=self.port, *args, **kwargs)

    def admin_client(self, *args, **kwargs) -> RedisClient:
        return RedisClient(port=self.admin_port, *args, **kwargs)

    def __enter__(self):
        self.start()
        return self

    def __repr__(self):
        return f":{self.port}"

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()

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
                self.get_port_from_psutil()
                logging.debug(
                    f"Process started after {time.time() - s:.2f} seconds. port={self.port}"
                )
                break
            except RuntimeError:
                time.sleep(0.05)
        else:
            raise DflyStartException("Process didn't start listening on port in time")
        self.log_files = self.get_logs_from_psutil()

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

        base_args = ["--use_zset_tree"]
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
            self._port = self.get_port_from_psutil()
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

    def get_port_from_psutil(self) -> int:
        if self.proc is None:
            raise RuntimeError("port is not available yet")
        p = psutil.Process(self.proc.pid)

        # If running with gdb, look for port on child
        children = p.children()
        if len(children) == 1 and children[0].name() == "dragonfly":
            p = children[0]

        ports = set()
        for connection in p.connections():
            if connection.status == "LISTEN":
                ports.add(connection.laddr.port)

        ports.difference_update({self.admin_port, self.mc_port})
        assert len(ports) < 2, "Open ports detection found too many ports"
        if ports:
            return ports.pop()
        raise RuntimeError("Couldn't parse port")

    def get_logs_from_psutil(self) -> List[str]:
        p = psutil.Process(self.proc.pid)
        rv = []
        for file in p.open_files():
            if ".log." in file.path and "dragonfly" in file.path:
                rv.append(file.path)
        return rv

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

    def is_in_logs(self, pattern):
        if self.proc is not None:
            raise RuntimeError("Must close server first")

        matcher = re.compile(pattern)
        for path in self.log_files:
            for line in open(path):
                if matcher.search(line):
                    return True
        return False


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
        """Stop all launched instances."""
        for instance in self.instances:
            instance.stop()

    def __repr__(self) -> str:
        return f"Factory({self.args})"
