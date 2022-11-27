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
