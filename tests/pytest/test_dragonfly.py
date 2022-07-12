import pytest
import redis
import os
import subprocess
import time
from threading import Thread


@pytest.fixture(scope="module")
def dragonfly_db():
    dragonfly_path = os.environ.get("DRAGONFLY_HOME", '../../build-dbg/dragonfly')
    print("Starting DragonflyDB [{}]".format(dragonfly_path))
    # TODO: parse arguments and pass them over
    p = subprocess.Popen([dragonfly_path])
    time.sleep(0.1)
    return_code = p.poll()
    if return_code is not None:
        pytest.exit("Failed to start DragonflyDB [{}]".format(dragonfly_path))

    yield

    print("Terminating DragonflyDB process [{}]".format(p.id))
    try:
        p.terminate()
        outs, errs = p.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        print("Unable to terminate DragonflyDB gracefully, it was killed")
        outs, errs = p.communicate()
        print(outs)
        print(errs)


@pytest.fixture(scope="module")
def client(dragonfly_db):
    pool = redis.ConnectionPool(decode_responses=True)
    client = redis.Redis(connection_pool=pool)
    return client


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


@pytest.mark.parametrize('index', range(50))
def test_blpop_multiple_keys(client: redis.Redis, index):
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
