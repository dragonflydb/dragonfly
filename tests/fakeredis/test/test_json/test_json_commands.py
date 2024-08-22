"""Tests for `fakeredis-py`'s emulation of Redis's JSON command subset."""

from __future__ import annotations

from typing import (
    Any,
    Dict,
    List,
    Tuple,
)

import pytest

json_tests = pytest.importorskip("jsonpath_ng")


SAMPLE_DATA = {
    "a": ["foo"],
    "nested1": {"a": ["hello", None, "world"]},
    "nested2": {"a": 31},
}


@pytest.fixture(scope="function")
def json_data() -> Dict[str, Any]:
    """A module-scoped "blob" of JSON-encodable data."""
    return {
        "L1": {
            "a": {
                "A1_B1": 10,
                "A1_B2": False,
                "A1_B3": {
                    "A1_B3_C1": None,
                    "A1_B3_C2": [
                        "A1_B3_C2_D1_1",
                        "A1_B3_C2_D1_2",
                        -19.5,
                        "A1_B3_C2_D1_4",
                        "A1_B3_C2_D1_5",
                        {"A1_B3_C2_D1_6_E1": True},
                    ],
                    "A1_B3_C3": [1],
                },
                "A1_B4": {"A1_B4_C1": "foo"},
            }
        },
        "L2": {
            "a": {
                "A2_B1": 20,
                "A2_B2": False,
                "A2_B3": {
                    "A2_B3_C1": None,
                    "A2_B3_C2": [
                        "A2_B3_C2_D1_1",
                        "A2_B3_C2_D1_2",
                        -37.5,
                        "A2_B3_C2_D1_4",
                        "A2_B3_C2_D1_5",
                        {"A2_B3_C2_D1_6_E1": False},
                    ],
                    "A2_B3_C3": [2],
                },
                "A2_B4": {"A2_B4_C1": "bar"},
            }
        },
    }


def load_types_data(nested_key_name: str) -> Tuple[Dict[str, Any], List[bytes]]:
    """Generate a structure with sample of all types"""
    type_samples = {
        "object": {},
        "array": [],
        "string": "str",
        "integer": 42,
        "number": 1.2,
        "boolean": False,
        "null": None,
    }
    jdata = {}

    for k, v in type_samples.items():
        jdata[f"nested_{k}"] = {nested_key_name: v}

    return jdata, [k.encode() for k in type_samples.keys()]
