
import pytest
import redis
from threading import Thread



@pytest.fixture
def client():
    pool = redis.ConnectionPool(decode_responses=True)
    return redis.Redis(connection_pool=pool)


class BLPopWorkerThread:
    def __init__(self):
        self.result = None

    def async_blpop(self, client: redis.Redis):
        self.result = None

        def blpop_task(self, client):
            self.result = client.blpop(
                ['list1{t}', 'list2{t}', 'list2{t}', 'list1{t}'], 0.5)

        result = Thread(target=blpop_task, args=(self, client))
        result.start()
        return result


@pytest.mark.parametrize('execution_number', range(5))
def test_blpop_multiple_keys(client, execution_number):
    wt_blpop = BLPopWorkerThread()
    thread = wt_blpop.async_blpop(client)
    client.lpush('list1{t}', 'a')
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert wt_blpop.result[1] == 'a'

    thread = wt_blpop.async_blpop(client)
    client.lpush('list2{t}', 'b')
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert wt_blpop.result[1] == 'b'
