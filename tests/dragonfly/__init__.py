import pytest
import time
import subprocess

from dataclasses import dataclass

START_DELAY = 0.4
START_GDB_DELAY = 3.0


@dataclass
class DflyParams:
    path: str
    cwd: str
    gdb: bool
    args: list
    env: any


class DflyInstance:
    """
    Represents a runnable and stoppable Dragonfly instance
    with fixed arguments.
    """

    def __init__(self, params: DflyParams, args):
        self.args = args
        self.params = params
        self.proc = None

    def start(self):
        self._start()

        # Give Dragonfly time to start and detect possible failure causes
        # Gdb starts slowly
        time.sleep(START_DELAY if not self.params.gdb else START_GDB_DELAY)

        self._check_status()

    def stop(self, kill=False):
        proc, self.proc = self.proc, None
        if proc is None:
            return

        print(f"Stopping instance on {self.port}")
        try:
            if kill:
                proc.kill()
            else:
                proc.terminate()
            proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            print("Unable to terminate DragonflyDB gracefully, it was killed")
            proc.kill()

    def _start(self):
        base_args = [f"--{v}" for v in self.params.args]
        all_args = self.format_args(self.args) + base_args
        print(f"Starting instance on {self.port} with arguments {all_args}")

        run_cmd = [self.params.path, *all_args]
        if self.params.gdb:
            run_cmd = ["gdb", "--ex", "r", "--args"] + run_cmd
        self.proc = subprocess.Popen(run_cmd, cwd=self.params.cwd)

    def _check_status(self):
        return_code = self.proc.poll()
        if return_code is not None:
            raise Exception(
                f"Failed to start instance, return code {return_code}")

    def __getitem__(self, k):
        return self.args.get(k)

    @property
    def port(self) -> int:
        return int(self.args.get("port", "6379"))

    @staticmethod
    def format_args(args):
        out = []
        for (k, v) in args.items():
            out.extend((str(s) for s in ("--"+k, v) if s != ""))
        return out


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
        for k, v in args.items():
            args[k] = v.format(**self.params.env) if isinstance(v, str) else v

        instance = DflyInstance(self.params, args)
        self.instances.append(instance)
        return instance

    def start_all(self, instances):
        """ Start multiple instances in parallel """
        for instance in instances:
            instance._start()

        delay = START_DELAY if not self.params.gdb else START_GDB_DELAY
        time.sleep(delay * (1 + len(instances) / 2))

        for instance in instances:
            instance._check_status()

    def stop_all(self):
        """Stop all lanched instances."""
        for instance in self.instances:
            instance.stop()


def dfly_args(*args):
    """ Used to define a singular set of arguments for dragonfly test """
    return pytest.mark.parametrize("df_factory", args, indirect=True)


def dfly_multi_test_args(*args):
    """ Used to define multiple sets of arguments to test multiple dragonfly configurations """
    return pytest.mark.parametrize("df_factory", args, indirect=True)
