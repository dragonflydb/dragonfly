import pytest
import typing
import time
import subprocess

import time
import subprocess


class DflyInstance:
    """
    Represents a runnable and stoppable Dragonfly instance
    with fixed arguments.
    """

    def __init__(self, path, args, cwd):
        self.path = path
        self.args = args
        self.cwd = cwd
        self.proc = None

    def start(self):
        arglist = DflyInstance.format_args(self.args)

        print(f"Starting instance on {self.port} with arguments {arglist}")
        self.proc = subprocess.Popen([self.path, *arglist], cwd=self.cwd)

        # Give Dragonfly time to start and detect possible failure causes
        time.sleep(0.3)

        return_code = self.proc.poll()
        if return_code is not None:
            raise Exception(
                f"Failed to start instance, return code {return_code}")

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
            outs, errs = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            print("Unable to terminate DragonflyDB gracefully, it was killed")
            outs, errs = proc.communicate()
            print(outs, errs)

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

    def __init__(self, env, cwd, path, args):
        self.env = env
        self.cwd = cwd
        self.path = path
        self.args = args
        self.instances = []

    def create(self, **kwargs) -> DflyInstance:
        args = {**self.args, **kwargs}
        for k, v in args.items():
            args[k] = v.format(**self.env) if isinstance(v, str) else v

        instance = DflyInstance(self.path, args, self.cwd)
        self.instances.append(instance)
        return instance

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
