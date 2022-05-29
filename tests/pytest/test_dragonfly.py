import pytest
import redis
from threading import Thread
from redis.client import NEVER_DECODE

@pytest.fixture
def client():
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
def test_blpop_multiple_keys(client : redis.Redis, index):
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
