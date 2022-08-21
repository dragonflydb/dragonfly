import pytest
import redis
import os
import subprocess
import time
from threading import Thread

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

DRAGONFLY_PATH = os.environ.get("DRAGONFLY_HOME", os.path.join(SCRIPT_DIR, '../../build-dbg/dragonfly'))

# used to define a singular set of arguments for dragonfly test
def dfly_args(*args: str):
    return pytest.mark.parametrize("df_server", [args], indirect=True)

# used to define multiple sets of arguments to test multiple dragonfly configurations
def dfly_multi_test_args(*args: list[str]):
    return pytest.mark.parametrize("df_server", args, indirect=True)

@pytest.fixture(scope="class", params=[[]])
def df_server(request):
    """ Starts a single DragonflyDB process, runs only once. """
    print("Starting DragonflyDB [{}]".format(DRAGONFLY_PATH))
    arguments = request.param
    p = subprocess.Popen([DRAGONFLY_PATH, *arguments])
    time.sleep(0.1)
    return_code = p.poll()
    if return_code is not None:
        pytest.exit("Failed to start DragonflyDB [{}]".format(DRAGONFLY_PATH))

    yield

    print("Terminating DragonflyDB process [{}]".format(p.pid))
    try:
        p.terminate()
        outs, errs = p.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        print("Unable to terminate DragonflyDB gracefully, it was killed")
        outs, errs = p.communicate()
        print(outs)
        print(errs)


@pytest.fixture(scope="class")
def connection(df_server):
    pool = redis.ConnectionPool(decode_responses=True)
    client = redis.Redis(connection_pool=pool)
    return client


@pytest.fixture
def client(connection):
    """ Flushes all the records, runs before each test. """
    connection.flushall()
    return connection


class BLPopWorkerThread:
    def __init__(self):
        self.result = None
        self.thread = None

    def async_blpop(self, client: redis.Redis):
        self.result = None

        def blpop_task(self, client):
            self.result = client.blpop(
                ['list1{t}', 'list2{t}', 'list2{t}', 'list1{t}'], 0.5)

        self.thread = Thread(target=blpop_task, args=(self, client))
        self.thread.start()

    def wait(self, timeout):
        self.thread.join(timeout)
        return not self.thread.is_alive()


@pytest.mark.usefixtures("client")
@pytest.mark.parametrize('index', range(50))
class TestBlPop:
    def test_blpop_multiple_keys(self, client, index):
        wt_blpop = BLPopWorkerThread()
        wt_blpop.async_blpop(client)

        client.lpush('list1{t}', 'a')
        assert wt_blpop.wait(2)
        assert wt_blpop.result[1] == 'a'
        watched = client.execute_command('DEBUG WATCHED')
        assert watched == []

        wt_blpop.async_blpop(client)
        client.lpush('list2{t}', 'b')
        assert wt_blpop.wait(2)
        assert wt_blpop.result[1] == 'b'


@pytest.mark.usefixtures("client")
@dfly_multi_test_args(["--keys_output_limit", "512"], ["--keys_output_limit", "1024"])
class TestKeys:
    def test_max_keys(self, client):
        for x in range(8192):
            client.set(str(x), str(x))
        keys = client.keys()
        assert len(keys) in [513, 1025]