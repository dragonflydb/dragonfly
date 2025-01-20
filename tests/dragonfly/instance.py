import dataclasses
import os
import threading
import time
import subprocess
import random
import aiohttp
import logging
from dataclasses import dataclass
from typing import Dict, Optional, List, Union
import re
import psutil
import itertools
from prometheus_client.parser import text_string_to_metric_families
from redis.asyncio import Redis as RedisClient
from redis.asyncio import RedisCluster as RedisCluster
import signal


START_DELAY = 0.8
START_GDB_DELAY = 5.0


@dataclass
class DflyParams:
    path: str
    cwd: str
    gdb: bool
    direct_output: bool
    buffered_out: bool
    args: Dict[str, Union[str, None]]
    existing_port: int
    existing_admin_port: int
    existing_mc_port: int
    env: any
    log_dir: str


class Colors:
    CLEAR = "\\o33[0m"
    COLORS = [f"\\o33[0;{i}m" for i in range(31, 37)]
    last_color = -1

    @classmethod
    def next(clz):
        clz.last_color = (clz.last_color + 1) % len(clz.COLORS)
        return clz.COLORS[clz.last_color]


class DflyStartException(Exception):
    pass


def symbolize_stack_trace(binary_path, lines):
    addr2line_proc = subprocess.Popen(
        ["/usr/bin/addr2line", "-fCa", "-e", binary_path], stdin=subprocess.PIPE
    )
    for line in lines:
        addr2line_proc.stdin.write(line.encode())

    addr2line_proc.stdin.close()
    addr2line_proc.wait()


def read_sedout(pipe, stacktrace):
    try:
        seen = set()
        pattern = r"@\s*(0x[0-9a-fA-F]+)"
        matcher = re.compile(pattern)

        for line in iter(pipe.readline, b""):
            # Deduplicate output - we somewhere duplicate the output, probably due
            # to tty redirections.
            if line not in seen:
                seen.add(line)
                print(line)
                res = matcher.search(line)
                if res:
                    stacktrace.append(res.group(1) + "\n")
    except ValueError:
        pass
    finally:
        pipe.close()


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
        self.sed_proc = None
        self.clients = []

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

        # Run with num_shards = (proactor_threads - 1) if possible, so help expose bugs
        if "num_shards" not in self.args:
            threads = psutil.cpu_count()
            if "proactor_threads" in self.args:
                threads = int(self.args["proactor_threads"])
            if threads > 1:
                self.args["num_shards"] = threads - 1

    def __del__(self):
        assert self.proc == None

    def client(self, *args, **kwargs) -> RedisClient:
        host = "localhost" if self["bind"] is None else self["bind"]
        client = RedisClient(host=host, port=self.port, decode_responses=True, *args, **kwargs)
        self.clients.append(client)
        return client

    def admin_client(self, *args, **kwargs) -> RedisClient:
        client = RedisClient(
            port=self.admin_port,
            single_connection_client=True,
            decode_responses=True,
            *args,
            **kwargs,
        )
        self.clients.append(client)
        return client

    def cluster_client(self, *args, **kwargs) -> RedisCluster:
        client = RedisCluster(
            host="localhost", port=self.port, decode_responses=True, *args, **kwargs
        )
        self.clients.append(client)
        return client

    async def close_clients(self):
        for client in self.clients:
            await client.aclose() if hasattr(client, "aclose") else await client.close()

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
        if self.params.existing_port:
            return
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
                    f"Process {self.proc.pid} started after {time.time() - s:.2f} seconds. port={self.port}"
                )
                break
            except RuntimeError:
                time.sleep(0.05)
        else:
            raise DflyStartException("Process didn't start listening on port in time")

        self.log_files = self.get_logs_from_psutil()

        # Remove first 6 lines - our default header with log locations (as it carries no useful information)
        # Next, replace log-level + date with port and colored arrow
        sed_format = f"1,6d;s/[^ ]*/{self.port}{Colors.next()}➜{Colors.CLEAR}/"
        sed_cmd = ["sed", "-u", "-e", sed_format]
        if self.params.buffered_out:
            sed_cmd.remove("-u")
        if not self.params.direct_output:
            self.sed_proc = subprocess.Popen(
                sed_cmd,
                stdin=self.proc.stdout,
                stdout=subprocess.PIPE,
                bufsize=1,
                universal_newlines=True,
            )
            self.stacktrace = []
            self.sed_thread = threading.Thread(
                target=read_sedout, args=(self.sed_proc.stdout, self.stacktrace), daemon=True
            )
            self.sed_thread.start()

    def set_proc_to_none(self):
        self.proc = None

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
                proc.communicate(timeout=120)
                # if the return code is 0 it means normal termination
                # if the return code is negative it means termination by signal
                # if the return code is positive it means abnormal exit
                if proc.returncode != 0:
                    raise Exception(
                        f"Dragonfly did not terminate gracefully, exit code {proc.returncode}, "
                        f"pid: {proc.pid}"
                    )

        except subprocess.TimeoutExpired:
            # We need to send SIGUSR1 to DF such that it prints the stacktrace
            proc.send_signal(signal.SIGUSR1)
            # Then we sleep for 5 seconds such that DF has enough time to print the stacktraces
            # We can't really synchronize here because SIGTERM and SIGKILL do not block even if
            # sigaction explicitly blocks other incoming signals until it handles SIGUSR1.
            # Even worse, on SIGTERM and SIGKILL none of the handlers registered via sigaction
            # are guranteed to run
            time.sleep(5)
            logging.debug(f"Unable to kill the process on port {self._port}")
            logging.debug(f"INFO LOGS of DF are:")
            self.print_info_logs_to_debug_log()
            proc.kill()
            proc.communicate()
            raise Exception("Unable to terminate DragonflyDB gracefully, it was killed")
        finally:
            if self.sed_proc:
                self.sed_proc.communicate()
                self.sed_thread.join()
                symbolize_stack_trace(proc.args[0], self.stacktrace)

    def _start(self):
        if self.params.existing_port:
            return

        if self.dynamic_port:
            self._port = None

        all_args = self.format_args(self.args)
        real_path = os.path.realpath(self.params.path)

        run_cmd = [self.params.path, *all_args]
        if self.params.gdb:
            run_cmd = ["gdb", "--ex", "r", "--args"] + run_cmd

        self.proc = subprocess.Popen(
            run_cmd,
            cwd=self.params.cwd,
            stdout=None if self.params.direct_output else subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        logging.debug(f"Starting {real_path} {' '.join(all_args)}, pid {self.proc.pid}")

    def _check_status(self):
        if not self.params.existing_port:
            return_code = self.proc.poll()
            if return_code is not None:
                # log stdout of the failed process
                logging.error("Dragonfly process error:\n%s", self.proc.stdout.read().decode())
                self.proc = None
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
        try:
            for connection in p.connections():
                if connection.status == "LISTEN":
                    ports.add(connection.laddr.port)
        except psutil.AccessDenied:
            raise RuntimeError("Access denied")

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

    def print_info_logs_to_debug_log(self):
        logs = self.log_files
        sed_format = f"s/[^ ]*/{self.port}{Colors.next()}➜{Colors.CLEAR}/"
        sed_cmd = ["sed", "-e", sed_format]
        for log in logs:
            if "INFO" in log:
                with open(log) as file:
                    print(f"🪵🪵🪵🪵🪵🪵 LOG name {log} 🪵🪵🪵🪵🪵🪵")
                    subprocess.call(sed_cmd, stdin=file)

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
        data = await resp.text(encoding="utf-8")
        await session.close()
        return {
            metric_family.name: metric_family
            for metric_family in text_string_to_metric_families(data)
        }

    def find_in_logs(self, pattern):
        if self.proc is not None:
            raise RuntimeError("Must close server first")

        results = []
        matcher = re.compile(pattern)
        for path in self.log_files:
            for line in open(path):
                if matcher.search(line):
                    results.append(line)
        return results

    @property
    def rss(self):
        if self.proc is None:
            return 0
        process = psutil.Process(self.proc.pid)
        mem_info = process.memory_info()
        return mem_info.rss


class DflyInstanceFactory:
    """
    A factory for creating dragonfly instances with pre-supplied arguments.
    """

    def __init__(self, params: DflyParams, args):
        self.args = args
        self.params = params
        self.instances = []

    def create(self, existing_port=None, path=None, version=100, **kwargs) -> DflyInstance:
        args = {**self.args, **kwargs}
        args.setdefault("dbfilename", "")
        args.setdefault("noversion_check", None)
        # MacOs does not set it automatically, so we need to set it manually
        args.setdefault("maxmemory", "8G")
        vmod = "dragonfly_connection=1,accept_server=1,listener_interface=1,main_service=1,rdb_save=1,replica=1,cluster_family=1,proactor_pool=1,dflycmd=1,snapshot=1,streamer=1"
        args.setdefault("vmodule", vmod)
        args.setdefault("jsonpathv2")

        # If path is not set, we assume that we are running the latest dragonfly.
        if not path:
            args.setdefault("list_experimental_v2")
        args.setdefault("log_dir", self.params.log_dir)

        if version >= 1.21 and "serialization_max_chunk_size" not in args:
            args.setdefault("serialization_max_chunk_size", 300000)

        if version >= 1.26:
            args.setdefault("fiber_safety_margin=4096")

        for k, v in args.items():
            args[k] = v.format(**self.params.env) if isinstance(v, str) else v

        if existing_port is not None:
            params = dataclasses.replace(self.params, existing_port=existing_port)
        else:
            params = self.params

        if path is not None:
            params = dataclasses.replace(self.params, path=path)

        instance = DflyInstance(params, args)
        self.instances.append(instance)
        return instance

    def start_all(self, instances: List[DflyInstance]):
        """Start multiple instances in parallel"""
        for instance in instances:
            instance._start()

        for instance in instances:
            instance._wait_for_server()

    async def stop_all(self):
        """Stop all launched instances."""
        exceptions = []  # To collect exceptions
        for instance in self.instances:
            await instance.close_clients()
            try:
                instance.stop()
            except Exception as e:
                exceptions.append(e)  # Collect the exception
        if exceptions:
            first_exception = exceptions[0]
            raise Exception(
                f"One or more errors occurred while stopping instances. "
                f"First exception: {first_exception}"
            ) from first_exception

    def __repr__(self) -> str:
        return f"Factory({self.args})"


class RedisServer:
    def __init__(self, port):
        self.port = port
        self.proc = None

    def start(self, **kwargs):
        servers = ["redis-server-6.2.11", "redis-server-7.2.2", "valkey-server-8.0.1"]
        command = [
            random.choice(servers),
            f"--port {self.port}",
            "--save ''",
            "--appendonly no",
            "--protected-mode no",
            "--repl-diskless-sync yes",
            "--repl-diskless-sync-delay 0",
        ]
        # Convert kwargs to command-line arguments
        for key, value in kwargs.items():
            if value is None:
                command.append(f"--{key}")
            else:
                command.append(f"--{key} {value}")

        self.proc = subprocess.Popen(command)
        logging.debug(self.proc.args)

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except Exception as e:
            pass
