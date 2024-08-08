import pytest
import redis
from redis import asyncio as aioredis
from .utility import *
from json import JSONDecoder, JSONEncoder, dumps

jane = {"name": "Jane", "Age": 33, "Location": "Chawton"}

json_num = {"a": {"a": 1, "b": 2, "c": 3}}


async def get_set_json(connection: aioredis.Redis, key, value, path="$"):
    encoder = JSONEncoder()
    await connection.execute_command("json.set", key, path, encoder.encode(value))
    result = await connection.execute_command("json.get", key, path)
    decoder = JSONDecoder()
    return decoder.decode(result)


async def test_basic_json_get_set(async_client: aioredis.Redis):
    key_name = "test-json-key"
    result = await get_set_json(connection=async_client, key=key_name, value=jane)
    assert result, "failed to set JSON value"
    the_type = await async_client.type(key_name)
    assert the_type == "ReJSON-RL"
    assert len(result) == 1
    assert result[0]["name"] == "Jane"
    assert result[0]["Age"] == 33


async def test_access_json_value_as_string(async_client: aioredis.Redis):
    key_name = "test-json-key"
    result = await get_set_json(async_client, key_name, value=jane)
    assert result is not None, "failed to set JSON value"
    # make sure that we have valid JSON here
    the_type = await async_client.type(key_name)
    assert the_type == "ReJSON-RL"
    # you cannot access this key as string
    try:
        result = await async_client.get(key_name)
        assert False, "should not be able to access JSON value as string"
    except redis.exceptions.ResponseError as e:
        assert e.args[0] == "WRONGTYPE Operation against a key holding the wrong kind of value"


async def test_reset_key_to_string(async_client: aioredis.Redis):
    key_name = "test-json-key"
    result = await get_set_json(async_client, key=key_name, value=jane)
    assert result is not None, "failed to set JSON value"
    # make sure that we have valid JSON here
    the_type = await async_client.type(key_name)
    assert the_type == "ReJSON-RL"

    # set the key to be string - this is legal
    await async_client.set(key_name, "some random value")
    result = await async_client.get(key_name)
    assert result == "some random value"

    # For JSON set the update the root path, we are allowing
    # to change the type to JSON and override it
    result = await get_set_json(async_client, key=key_name, value=jane)
    the_type = await async_client.type(key_name)
    assert the_type == "ReJSON-RL"


async def test_update_value(async_client: aioredis.Redis):
    key_name = "test-json-key"
    result = await get_set_json(async_client, key=key_name, value=json_num)
    assert result is not None, "failed to set JSON value"
    # make sure that we have valid JSON here
    the_type = await async_client.type(key_name)
    assert the_type == "ReJSON-RL"
    result = await get_set_json(async_client, value="0", key=key_name, path="$.a.*")
    assert len(result) == 3
    # make sure that all the values under 'a' where set to 0
    assert result == ["0", "0", "0"]

    # Ensure that after we're changing this into STRING type, it will no longer work
    await async_client.set(key_name, "some random value")
    assert await async_client.type(key_name) == "string"
    try:
        await get_set_json(async_client, value="0", key=key_name, path="$.a.*")
        assert False, "should not be able to modify JSON value as string"
    except redis.exceptions.ResponseError as e:
        assert e.args[0] == "WRONGTYPE Operation against a key holding the wrong kind of value"
    assert await async_client.type(key_name) == "string"


@pytest.mark.parametrize(
    "description,expected_value,expected_type",
    (
        ("array", "[]", "array"),
        ("string", dumps("dragonfly"), "string"),
        ("number", dumps(3.50), "number"),
        ("object", dumps({"dragon": "fly"}, separators=(",", ":")), "object"),
        ("boolean true", "true", "boolean"),
        ("boolean false", "false", "boolean"),
    ),
)
@pytest.mark.asyncio
async def test_arrappend(async_client: aioredis.Redis, description, expected_value, expected_type):
    key_name = "test-json-key"

    await async_client.execute_command("json.set", key_name, "$", "[]")
    await async_client.execute_command("json.arrappend", key_name, "$", expected_value)

    # make sure the value is as expected
    first_element = await async_client.execute_command("json.get", key_name, "$[0]")
    assert first_element == "[{}]".format(expected_value)

    # make sure the type is as expected
    actual_type = await async_client.execute_command("json.type", key_name, "$[0]")
    assert actual_type[0] == expected_type
