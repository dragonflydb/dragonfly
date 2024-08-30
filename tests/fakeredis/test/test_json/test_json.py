"""
Tests for `fakeredis-py`'s emulation of Redis's JSON.GET command subset.
"""

from __future__ import annotations

import json
from test import testtools

import pytest
import redis
from redis.commands.json.path import Path

json_tests = pytest.importorskip("jsonpath_ng")


def test_jsonget(r: redis.Redis):
    data = {"x": "bar", "y": {"x": 33}}
    r.json().set("foo", Path.root_path(), data)
    assert r.json().get("foo") == data
    assert r.json().get("foo", Path("$..x")) == ["bar", 33]

    data2 = {"x": "bar"}
    r.json().set(
        "foo2",
        Path.root_path(),
        data2,
    )
    assert r.json().get("foo2") == data2
    assert r.json().get("foo2", "$") == [
        data2,
    ]
    assert r.json().get("foo2", Path("$.a"), Path("$.x")) == {"$.a": [], "$.x": ["bar"]}

    assert r.json().get("non-existing-key") is None

    r.json().set(
        "foo2",
        Path.root_path(),
        {"x": "bar", "y": {"x": 33}},
    )
    assert r.json().get("foo2") == {"x": "bar", "y": {"x": 33}}
    assert r.json().get("foo2", Path("$..x")) == ["bar", 33]

    r.json().set(
        "foo",
        Path.root_path(),
        {"x": "bar"},
    )
    assert r.json().get("foo") == {"x": "bar"}
    assert r.json().get("foo", Path("$.a"), Path("$.x")) == {"$.a": [], "$.x": ["bar"]}
    assert r.json().get("unknown", "$") is None


def test_json_setgetdeleteforget(r: redis.Redis):
    data = {"x": "bar"}
    assert r.json().set("foo", Path.root_path(), data) == 1
    assert r.json().get("foo") == data
    assert r.json().get("baz") is None
    assert r.json().delete("foo") == 1
    assert r.json().forget("foo") == 0  # second delete
    assert r.exists("foo") == 0


def test_json_delete_with_dollar(r: redis.Redis):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert r.json().set("doc1", Path.root_path(), doc1)
    assert r.json().delete("doc1", "$..a") == 2
    assert r.json().get("doc1", Path.root_path()) == {"nested": {"b": 3}}

    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [True, "a", "b"]}}
    r.json().set("doc2", "$", doc2)
    assert r.json().delete("doc2", "$..a") == 1
    assert r.json().get("doc2", Path.root_path()) == {
        "nested": {"b": [True, "a", "b"]},
        "b": ["a", "b"],
    }

    doc3 = [
        {
            "ciao": ["non ancora"],
            "nested": [
                {"ciao": [1, "a"]},
                {"ciao": [2, "a"]},
                {"ciaoc": [3, "non", "ciao"]},
                {"ciao": [4, "a"]},
                {"e": [5, "non", "ciao"]},
            ],
        }
    ]
    assert r.json().set("doc3", Path.root_path(), doc3)
    assert r.json().delete("doc3", '$.[0]["nested"]..ciao') == 3

    doc3val = [
        [
            {
                "ciao": ["non ancora"],
                "nested": [
                    {},
                    {},
                    {"ciaoc": [3, "non", "ciao"]},
                    {},
                    {"e": [5, "non", "ciao"]},
                ],
            }
        ]
    ]
    assert r.json().get("doc3", Path.root_path()) == doc3val[0]

    # Test default path
    assert r.json().delete("doc3") == 1
    assert r.json().get("doc3", Path.root_path()) is None

    r.json().delete("not_a_document", "..a")


def test_json_et_non_dict_value(r: redis.Redis):
    r.json().set(
        "str",
        Path.root_path(),
        "str_val",
    )
    assert r.json().get("str") == "str_val"

    r.json().set("bool", Path.root_path(), True)
    assert r.json().get("bool") is True

    r.json().set("bool", Path.root_path(), False)
    assert r.json().get("bool") is False


def test_jsonset_existential_modifiers_should_succeed(r: redis.Redis):
    obj = {"foo": "bar"}
    assert r.json().set("obj", Path.root_path(), obj)

    # Test that flags prevent updates when conditions are unmet
    assert (
        r.json().set(
            "obj",
            Path("foo"),
            "baz",
            nx=True,
        )
        is None
    )
    assert r.json().get("obj") == obj

    assert (
        r.json().set(
            "obj",
            Path("qaz"),
            "baz",
            xx=True,
        )
        is None
    )
    assert r.json().get("obj") == obj

    # Test that flags allow updates when conditions are met
    assert r.json().set("obj", Path("foo"), "baz", xx=True) == 1
    assert r.json().set("obj", Path("foo2"), "qaz", nx=True) == 1
    assert r.json().get("obj") == {"foo": "baz", "foo2": "qaz"}

    # Test with raw
    obj = {"foo": "bar"}
    testtools.raw_command(r, "json.set", "obj", "$", json.dumps(obj))
    assert r.json().get("obj") == obj


def test_jsonset_flags_should_be_mutually_exclusive(r: redis.Redis):
    with pytest.raises(Exception):
        r.json().set("obj", Path("foo"), "baz", nx=True, xx=True)
    with pytest.raises(redis.ResponseError):
        testtools.raw_command(
            r, "json.set", "obj", "$", json.dumps({"foo": "bar"}), "NX", "XX"
        )


def test_json_unknown_param(r: redis.Redis):
    with pytest.raises(redis.ResponseError):
        testtools.raw_command(
            r, "json.set", "obj", "$", json.dumps({"foo": "bar"}), "unknown"
        )


def test_jsonmget(r: redis.Redis):
    # Test mget with multi paths
    r.json().set(
        "doc1",
        "$",
        {"a": 1, "b": 2, "nested": {"a": 3}, "c": None, "nested2": {"a": None}},
    )
    r.json().set(
        "doc2",
        "$",
        {"a": 4, "b": 5, "nested": {"a": 6}, "c": None, "nested2": {"a": [None]}},
    )
    r.json().set(
        "doc3",
        "$",
        {
            "a": 5,
            "b": 5,
            "nested": {"a": 8},
            "c": None,
            "nested2": {"a": {"b": "nested3"}},
        },
    )
    # Compare also to single JSON.GET
    assert r.json().get("doc1", Path("$..a")) == [1, 3, None]
    assert r.json().get("doc2", "$..a") == [4, 6, [None]]
    assert r.json().get("doc3", "$..a") == [5, 8, {"b": "nested3"}]

    # Test mget with single path
    assert r.json().mget(["doc1"], "$..a") == [[1, 3, None]]

    # Test mget with multi path
    assert r.json().mget(["doc1", "doc2", "doc3"], "$..a") == [
        [1, 3, None],
        [4, 6, [None]],
        [5, 8, {"b": "nested3"}],
    ]

    # Test missing key
    assert r.json().mget(["doc1", "missing_doc"], "$..a") == [[1, 3, None], None]

    assert r.json().mget(["missing_doc1", "missing_doc2"], "$..a") == [None, None]


def test_jsonmget_should_succeed(r: redis.Redis):
    r.json().set("1", Path.root_path(), 1)
    r.json().set("2", Path.root_path(), 2)

    assert r.json().mget(["1"], Path.root_path()) == [1]

    assert r.json().mget([1, 2], Path.root_path()) == [1, 2]


def test_jsonclear(r: redis.Redis):
    r.json().set(
        "arr",
        Path.root_path(),
        [0, 1, 2, 3, 4],
    )

    assert 1 == r.json().clear(
        "arr",
        Path.root_path(),
    )
    assert [] == r.json().get("arr")


def test_jsonclear_dollar(r: redis.Redis):
    data = {
        "nested1": {"a": {"foo": 10, "bar": 20}},
        "a": ["foo"],
        "nested2": {"a": "claro"},
        "nested3": {"a": {"baz": 50}},
    }
    r.json().set("doc1", "$", data)
    # Test multi
    assert r.json().clear("doc1", "$..a") == 3

    assert r.json().get("doc1", "$") == [
        {"nested1": {"a": {}}, "a": [], "nested2": {"a": "claro"}, "nested3": {"a": {}}}
    ]

    # Test single
    r.json().set("doc1", "$", data)
    assert r.json().clear("doc1", "$.nested1.a") == 1
    assert r.json().get("doc1", "$") == [
        {
            "nested1": {"a": {}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        }
    ]

    # Test missing path (defaults to root)
    assert r.json().clear("doc1") == 1
    assert r.json().get("doc1", "$") == [{}]


def test_jsonclear_no_doc(r: redis.Redis):
    # Test missing key
    with pytest.raises(redis.ResponseError):
        r.json().clear("non_existing_doc", "$..a")


def test_jsonstrlen(r: redis.Redis):
    data = {"x": "bar", "y": {"x": 33}}
    r.json().set("foo", Path.root_path(), data)
    assert r.json().strlen("foo", Path("$..x")) == [3, None]

    r.json().set("foo2", Path.root_path(), "data2")
    assert r.json().strlen("foo2") == 5
    assert r.json().strlen("foo2", Path.root_path()) == 5

    r.json().set("foo3", Path.root_path(), {"x": "string"})
    assert r.json().strlen("foo3", Path("$.x")) == [
        6,
    ]

    assert r.json().strlen("non-existing") is None

    r.json().set("str", Path.root_path(), "foo")
    assert r.json().strlen("str", Path.root_path()) == 3
    # Test multi
    r.json().set(
        "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
    )
    assert r.json().strlen("doc1", "$..a") == [3, 5, None]

    res2 = r.json().strappend("doc1", "bar", "$..a")
    res1 = r.json().strlen("doc1", "$..a")
    assert res1 == res2

    # Test single
    assert r.json().strlen("doc1", "$.nested1.a") == [8]
    assert r.json().strlen("doc1", "$.nested2.a") == [None]

    # Test missing key
    # Note: Dragonfly returns NIL in the accordance to the official docs
    # with pytest.raises(redis.ResponseError):
    #    r.json().strlen("non_existing_doc", "$..a")


def test_toggle(r: redis.Redis):
    r.json().set("bool", Path.root_path(), False)
    assert r.json().toggle("bool", Path.root_path())
    assert r.json().toggle("bool", Path.root_path()) is False

    r.json().set("num", Path.root_path(), 1)

    with pytest.raises(redis.exceptions.ResponseError):
        r.json().toggle("num", Path.root_path())


def test_toggle_dollar(r: redis.Redis):
    data = {
        "a": ["foo"],
        "nested1": {"a": False},
        "nested2": {"a": 31},
        "nested3": {"a": True},
    }
    r.json().set("doc1", "$", data)
    # Test multi
    assert r.json().toggle("doc1", "$..a") == [None, 1, None, 0]
    data["nested1"]["a"] = True
    data["nested3"]["a"] = False
    assert r.json().get("doc1", "$") == [data]

    # Test missing key
    with pytest.raises(redis.exceptions.ResponseError):
        r.json().toggle("non_existing_doc", "$..a")


def test_json_commands_in_pipeline(r: redis.Redis):
    p = r.json().pipeline()
    p.set("foo", Path.root_path(), "bar")
    p.get("foo")
    p.delete("foo")
    assert [True, "bar", 1] == p.execute()
    assert r.keys() == []
    assert r.get("foo") is None

    # now with a true, json object
    r.flushdb()
    p = r.json().pipeline()
    d = {"hello": "world", "oh": "snap"}

    with pytest.deprecated_call():
        p.jsonset("foo", Path.root_path(), d)
        p.jsonget("foo")

    p.exists("not-a-real-key")
    p.delete("foo")

    assert [True, d, 0, 1] == p.execute()
    assert r.keys() == []
    assert r.get("foo") is None


def test_strappend(r: redis.Redis):
    # Test single
    r.json().set("json-key", Path.root_path(), "foo")
    assert r.json().strappend("json-key", "bar") == 6
    assert "foobar" == r.json().get("json-key", Path.root_path())

    # Test multi
    r.json().set(
        "doc1",
        Path.root_path(),
        {
            "a": "foo",
            "nested1": {"a": "hello"},
            "nested2": {"a": 31},
        },
    )
    assert r.json().strappend("doc1", "bar", "$..a") == [6, 8, None]
    assert r.json().get("doc1") == {
        "a": "foobar",
        "nested1": {"a": "hellobar"},
        "nested2": {"a": 31},
    }

    # Test single
    assert r.json().strappend(
        "doc1",
        "baz",
        "$.nested1.a",
    ) == [11]
    assert r.json().get("doc1") == {
        "a": "foobar",
        "nested1": {"a": "hellobarbaz"},
        "nested2": {"a": 31},
    }

    # Test missing key
    with pytest.raises(redis.exceptions.ResponseError):
        r.json().strappend("non_existing_doc", "$..a", "err")

    # Test multi
    r.json().set(
        "doc2",
        Path.root_path(),
        {
            "a": "foo",
            "nested1": {"a": "hello"},
            "nested2": {"a": "hi"},
        },
    )
    assert r.json().strappend("doc2", "bar", "$.*.a") == [8, 5]
    assert r.json().get("doc2") == {
        "a": "foo",
        "nested1": {"a": "hellobar"},
        "nested2": {"a": "hibar"},
    }

    # Test missing path
    r.json().set(
        "doc1",
        Path.root_path(),
        {
            "a": "foo",
            "nested1": {"a": "hello"},
            "nested2": {"a": 31},
        },
    )
    with pytest.raises(redis.exceptions.ResponseError):
        r.json().strappend("doc1", "add", "piu")

    # Test raw command with no arguments
    with pytest.raises(redis.ResponseError):
        testtools.raw_command(r, "json.strappend", "")


@pytest.mark.decode_responses(True)
def test_decode_null(r: redis.Redis):
    assert r.json().get("abc") is None


def test_decode_response_disabaled_null(r: redis.Redis):
    assert r.json().get("abc") is None


def test_json_get_jset(r: redis.Redis):
    assert r.json().set("foo", Path.root_path(), "bar") == 1
    assert "bar" == r.json().get("foo")
    assert r.json().get("baz") is None
    assert 1 == r.json().delete("foo")
    assert r.exists("foo") == 0


def test_nonascii_setgetdelete(r: redis.Redis):
    assert r.json().set(
        "not-ascii",
        Path.root_path(),
        "hyvää-élève",
    )
    assert "hyvää-élève" == r.json().get(
        "not-ascii",
        no_escape=True,
    )
    assert 1 == r.json().delete("not-ascii")
    assert r.exists("not-ascii") == 0


def test_json_setbinarykey(r: redis.Redis):
    data = {"hello": "world", b"some": "value"}

    with pytest.raises(TypeError):
        r.json().set("some-key", Path.root_path(), data)

    assert r.json().set("some-key", Path.root_path(), data, decode_keys=True)


def test_set_file(r: redis.Redis):
    # Standard Library Imports
    import json
    import tempfile

    obj = {"hello": "world"}
    jsonfile = tempfile.NamedTemporaryFile(suffix=".json")
    with open(jsonfile.name, "w+") as fp:
        fp.write(json.dumps(obj))

    no_json_file = tempfile.NamedTemporaryFile()
    no_json_file.write(b"Hello World")

    assert r.json().set_file("test", Path.root_path(), jsonfile.name)
    assert r.json().get("test") == obj
    with pytest.raises(json.JSONDecodeError):
        r.json().set_file("test2", Path.root_path(), no_json_file.name)


def test_set_path(r: redis.Redis):
    # Standard Library Imports
    import json
    import tempfile

    root = tempfile.mkdtemp()
    jsonfile = tempfile.NamedTemporaryFile(mode="w+", dir=root, delete=False)
    no_json_file = tempfile.NamedTemporaryFile(mode="a+", dir=root, delete=False)
    jsonfile.write(json.dumps({"hello": "world"}))
    jsonfile.close()
    no_json_file.write("hello")

    result = {jsonfile.name: True, no_json_file.name: False}
    assert r.json().set_path(Path.root_path(), root) == result
    assert r.json().get(jsonfile.name.rsplit(".")[0]) == {"hello": "world"}


def test_type(r: redis.Redis):
    r.json().set("1", Path.root_path(), 1)

    assert r.json().type("1", Path.root_path()) == b"integer"
    assert r.json().type("1") == b"integer"  # noqa: E721

    meta_data = {
        "object": {},
        "array": [],
        "string": "str",
        "integer": 42,
        "number": 1.2,
        "boolean": False,
        "null": None,
    }
    data = {k: {"a": meta_data[k]} for k in meta_data}
    r.json().set("doc1", "$", data)

    # Dragonfly does not guarantee the traversal order for multi field traversal
    # json.type api assumes a predefined order and is not designed very well.
    # Test multi by comparing unordered sets
    assert set(r.json().type("doc1", "$..a")) == set(
        [k.encode() for k in meta_data.keys()]
    )  # noqa: E721

    # Test single
    assert r.json().type("doc1", "$.integer.a") == [b"integer"]  # noqa: E721
    assert r.json().type("doc1") == b"object"  # noqa: E721

    # Test missing key
    assert r.json().type("non_existing_doc", "..a") is None


def test_objlen(r: redis.Redis):
    # Test missing key, and path
    with pytest.raises(redis.ResponseError):
        r.json().objlen("non_existing_doc", "$..a")

    obj = {"foo": "bar", "baz": "qaz"}

    r.json().set("obj", Path.root_path(), obj)
    assert len(obj) == r.json().objlen("obj", Path.root_path())

    r.json().set("obj", Path.root_path(), obj)
    assert len(obj) == r.json().objlen("obj")
    r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "nested2": {"a": {"baz": 50}},
        },
    )
    # Test multi
    assert r.json().objlen("doc1", "$..a") == [None, 2, 1]
    # Test single
    assert r.json().objlen("doc1", "$.nested1.a") == [2]

    assert r.json().objlen("doc1", "$.nowhere") == []

    # Test legacy
    assert r.json().objlen("doc1", ".*.a") == 2

    # Test single
    assert r.json().objlen("doc1", ".nested2.a") == 1

    # Test missing key
    assert r.json().objlen("non_existing_doc", "..a") is None

    # Test missing path
    # with pytest.raises(exceptions.ResponseError):
    r.json().objlen("doc1", ".nowhere")


def test_objkeys(r: redis.Redis):
    obj = {"foo": "bar", "baz": "qaz"}
    r.json().set("obj", Path.root_path(), obj)
    keys = r.json().objkeys("obj", Path.root_path())
    keys.sort()
    exp = list(obj.keys())
    exp.sort()
    assert exp == keys

    r.json().set("obj", Path.root_path(), obj)

    # Dragonfly does not guarantee the order (implementation detail)
    assert set(r.json().objkeys("obj")) == obj.keys()

    assert r.json().objkeys("fakekey") is None

    r.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": {"baz": 50}},
        },
    )

    # Test single
    assert set(r.json().objkeys("doc1", "$.nested1.a")[0]) == {b"foo", b"bar"}

    # Test legacy
    assert set(r.json().objkeys("doc1", ".*.a")) == {"foo", "bar"}
    # Test single
    assert r.json().objkeys("doc1", ".nested2.a") == ["baz"]

    # Test missing key
    assert r.json().objkeys("non_existing_doc", "..a") is None

    # Test non existing doc
    with pytest.raises(redis.ResponseError):
        assert r.json().objkeys("non_existing_doc", "$..a") == []

    assert r.json().objkeys("doc1", "$..nowhere") == []


def test_numincrby(r: redis.Redis):
    r.json().set("num", Path.root_path(), 1)

    assert 2 == r.json().numincrby("num", Path.root_path(), 1)
    assert 2.5 == r.json().numincrby("num", Path.root_path(), 0.5)
    assert 1.25 == r.json().numincrby("num", Path.root_path(), -1.25)
    # Test NUMINCRBY
    r.json().set("doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]})
    # Test multi
    assert r.json().numincrby("doc1", "$..a", 2) == [None, 4, 7.0, None]

    assert r.json().numincrby("doc1", "$..a", 2.5) == [None, 6.5, 9.5, None]
    # Test single
    assert r.json().numincrby("doc1", "$.b[1].a", 2) == [11.5]

    assert r.json().numincrby("doc1", "$.b[2].a", 2) == [None]
    assert r.json().numincrby("doc1", "$.b[1].a", 3.5) == [15.0]


def test_nummultby(r: redis.Redis):
    r.json().set("num", Path.root_path(), 1)

    with pytest.deprecated_call():
        assert r.json().nummultby("num", Path.root_path(), 2) == 2
        assert r.json().nummultby("num", Path.root_path(), 2.5) == 5
        assert r.json().nummultby("num", Path.root_path(), 0.5) == 2.5

    r.json().set("doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]})

    # test list
    with pytest.deprecated_call():
        assert r.json().nummultby("doc1", "$..a", 2) == [None, 4, 10, None]
        assert r.json().nummultby("doc1", "$..a", 2.5) == [None, 10.0, 25.0, None]

    # Test single
    with pytest.deprecated_call():
        assert r.json().nummultby("doc1", "$.b[1].a", 2) == [50.0]
        assert r.json().nummultby("doc1", "$.b[2].a", 2) == [None]
        assert r.json().nummultby("doc1", "$.b[1].a", 3) == [150.0]

    # test missing keys
    with pytest.raises(redis.ResponseError):
        r.json().numincrby("non_existing_doc", "$..a", 2)
        r.json().nummultby("non_existing_doc", "$..a", 2)

    # Test legacy NUMINCRBY
    r.json().set("doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]})
    assert r.json().numincrby("doc1", ".b[0].a", 3) == 5

    # Test legacy NUMMULTBY
    r.json().set("doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]})

    with pytest.deprecated_call():
        assert r.json().nummultby("doc1", ".b[0].a", 3) == 6


@testtools.run_test_if_redispy_ver("gte", "4.6")
@pytest.mark.min_server("7.1")
def test_json_merge(r: redis.Redis):
    # Test with root path $
    assert r.json().set(
        "person_data",
        "$",
        {"person1": {"personal_data": {"name": "John"}}},
    )
    assert r.json().merge(
        "person_data", "$", {"person1": {"personal_data": {"hobbies": "reading"}}}
    )
    assert r.json().get("person_data") == {
        "person1": {"personal_data": {"name": "John", "hobbies": "reading"}}
    }

    # Test with root path path $.person1.personal_data
    assert r.json().merge(
        "person_data", "$.person1.personal_data", {"country": "Israel"}
    )
    assert r.json().get("person_data") == {
        "person1": {
            "personal_data": {"name": "John", "hobbies": "reading", "country": "Israel"}
        }
    }

    # Test with null value to delete a value
    assert r.json().merge("person_data", "$.person1.personal_data", {"name": None})
    assert r.json().get("person_data") == {
        "person1": {"personal_data": {"country": "Israel", "hobbies": "reading"}}
    }


@testtools.run_test_if_redispy_ver("gte", "4.6")
@pytest.mark.min_server("7.1")
def test_mset(r: redis.Redis):
    r.json().mset([("1", Path.root_path(), 1), ("2", Path.root_path(), 2)])

    assert r.json().mget(["1"], Path.root_path()) == [1]
    assert r.json().mget(["1", "2"], Path.root_path()) == [1, 2]
