import pytest
import redis


def test_quit(connection):
    connection.send_command("QUIT")
    assert connection.read_response() == b'OK'

    with pytest.raises(redis.exceptions.ConnectionError) as e:
        connection.read_response()


def test_quit_after_sub(connection):
    connection = redis.Connection()
    connection.send_command("SUBSCRIBE", "foo")
    connection.read_response()

    connection.send_command("QUIT")
    assert connection.read_response() == b'OK'

    with pytest.raises(redis.exceptions.ConnectionError) as e:
        connection.read_response()


def test_multi_exec(client):
    pipeline = client.pipeline()
    pipeline.set("foo", "bar")
    pipeline.get("foo")
    val = pipeline.execute()
    assert val == [True, "bar"]


'''
see https://github.com/dragonflydb/dragonfly/issues/457
For now we would not allow for eval command inside multi
As this would create to level transactions (in effect recursive call
to Schedule function).
When this issue is fully fixed, this test would failed, and then it should
change to match the fact that we supporting this operation.
For now we are expecting to get an error
'''
def test_multi_eval(client):
    try:
        pipeline = client.pipeline()
        pipeline.set("foo", "bar")
        pipeline.get("foo")
        pipeline.eval("return 43", 0)
        assert True, "This part should not executed due to issue #457"

        val = pipeline.execute()
        assert val == "foo"
    except Exception as e:
        msg = str(e)
        assert "Dragonfly does not allow execution of" in msg


def test_connection_name(client):
    name = client.execute_command("CLIENT GETNAME")
    assert not name
    client.execute_command("CLIENT SETNAME test_conn_name")
    name = client.execute_command("CLIENT GETNAME")
    assert name == "test_conn_name"
