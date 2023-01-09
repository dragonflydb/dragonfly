import pytest
import redis
from redis.commands.json.path import Path
from .utility import *

jane = {
    'name': "Jane",
    'Age': 33,
    'Location': "Chawton"
}

json_num = {
    "a": {"a": 1, "b": 2, "c": 3}
}


def get_type(connection, key):
    return connection.execute_command("type", key)


def get_set_json(connection, key, value, path="$"):

    try:
        connection.json().set(key, path, value)

        result = connection.json().get(key, path)
        return result
    except redis.exceptions.ResponseError as re:
        return str(re)
    except Exception as e:
        return None


def get_str(connect, key):
    try:
        return connect.get(key)
    except redis.exceptions.ResponseError as re:
        return str(re)
    except Exception as e:
        return None


def test_basic_json_get_set(client):
    key_name = "test-json-key"
    result = get_set_json(connection=client, key=key_name, value=jane)
    assert result is not None, "failed to set JSON value"
    the_type = get_type(connection=client, key=key_name)
    assert the_type == "ReJSON-RL"
    assert len(result) == 1
    assert result[0]['name'] == 'Jane'
    assert result[0]['Age'] == 33


def test_access_json_value_as_string(client):
    key_name = "test-json-key"
    result = get_set_json(connection=client, key=key_name, value=jane)
    assert result is not None, "failed to set JSON value"
    # make sure that we have valid JSON here
    the_type = get_type(connection=client, key=key_name)
    assert the_type == "ReJSON-RL"
    # you cannot access this key as string
    result = get_str(connect=client, key=key_name)
    assert result is not None
    assert result == "WRONGTYPE Operation against a key holding the wrong kind of value"


def test_reset_key_to_string(client):
    key_name = "test-json-key"
    result = get_set_json(connection=client, key=key_name, value=jane)
    assert result is not None, "failed to set JSON value"
    # make sure that we have valid JSON here
    the_type = get_type(connection=client, key=key_name)
    assert the_type == "ReJSON-RL"
    # set the key to be string - this is legal
    assert client.set(
        key_name, "some random value"), "we should be able to set type to string"
    result = get_str(connect=client, key=key_name)
    assert result == "some random value"
    # For JSON set the update the root path, we are allowing
    # to change the type to JSON and override it
    result = get_set_json(connection=client, key=key_name, value=jane)
    the_type = get_type(connection=client, key=key_name)
    assert the_type == "ReJSON-RL"


def test_update_value(client):
    key_name = "test-json-key"
    result = get_set_json(connection=client, key=key_name, value=json_num)
    assert result is not None, "failed to set JSON value"
    # make sure that we have valid JSON here
    the_type = get_type(connection=client, key=key_name)
    assert the_type == "ReJSON-RL"
    result = get_set_json(connection=client, value="0",
                          key=key_name, path="$.a.*")
    assert len(result) == 3
    # make sure that all the values under 'a' where set to 0
    assert result == ['0', '0', '0']
    # Ensure that after we're changing this into STRING type, it will no longer work
    assert client.set(key_name, "some random value")
    assert get_type(connection=client, key=key_name) == "string"
    assert get_set_json(connection=client, value="0", key=key_name,
                        path="$.a.*") == "WRONGTYPE Operation against a key holding the wrong kind of value"
    assert get_type(connection=client, key=key_name) == "string"
