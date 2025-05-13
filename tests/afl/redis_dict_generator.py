#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import os
import re
import string
import argparse

# Constants
DICT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "redis.dict")

# Special characters to include in fuzzing
SPECIAL_CHARS = "!@#$%^&*()-_=+[]{}|;:'\",.<>/?\\"
ESCAPED_CHARS = [r"\\", r"\n", r"\r", r"\t", r"\"", r"\'", r"\0", r"\a", r"\b", r"\f", r"\v"]

# List of supported Redis commands with parameters
REDIS_COMMANDS = {
    # General commands
    "PING": {"args": [], "optional_args": []},
    "ECHO": {"args": ["message"], "optional_args": []},
    "INFO": {"args": [], "optional_args": ["section"]},
    "TIME": {"args": [], "optional_args": []},
    "QUIT": {"args": [], "optional_args": []},
    # Key operations commands
    "DEL": {"args": ["key"], "optional_args": ["key ..."]},
    "EXISTS": {"args": ["key"], "optional_args": ["key ..."]},
    "EXPIRE": {"args": ["key", "seconds"], "optional_args": ["NX|XX|GT|LT"]},
    "TTL": {"args": ["key"], "optional_args": []},
    "PERSIST": {"args": ["key"], "optional_args": []},
    "TYPE": {"args": ["key"], "optional_args": []},
    "RENAME": {"args": ["key", "newkey"], "optional_args": []},
    "RENAMENX": {"args": ["key", "newkey"], "optional_args": []},
    "KEYS": {"args": ["pattern"], "optional_args": []},
    "SCAN": {"args": ["cursor"], "optional_args": ["MATCH pattern", "COUNT count"]},
    # String operations commands
    "SET": {"args": ["key", "value"], "optional_args": ["EX seconds", "PX milliseconds", "NX|XX"]},
    "GET": {"args": ["key"], "optional_args": []},
    "MGET": {"args": ["key"], "optional_args": ["key ..."]},
    "MSET": {"args": ["key value"], "optional_args": ["key value ..."]},
    "INCR": {"args": ["key"], "optional_args": []},
    "INCRBY": {"args": ["key", "increment"], "optional_args": []},
    "DECR": {"args": ["key"], "optional_args": []},
    "DECRBY": {"args": ["key", "decrement"], "optional_args": []},
    "APPEND": {"args": ["key", "value"], "optional_args": []},
    "STRLEN": {"args": ["key"], "optional_args": []},
    "GETRANGE": {"args": ["key", "start", "end"], "optional_args": []},
    "SETRANGE": {"args": ["key", "offset", "value"], "optional_args": []},
}

# Additional Valkey/Redis commands based on their documentation
ADDITIONAL_COMMANDS = {
    # Transaction commands
    "MULTI": {"args": [], "optional_args": []},
    "EXEC": {"args": [], "optional_args": []},
    "DISCARD": {"args": [], "optional_args": []},
    "WATCH": {"args": ["key"], "optional_args": ["key ..."]},
    "UNWATCH": {"args": [], "optional_args": []},
    # Scripting
    "EVAL": {"args": ["script", "numkeys", "key"], "optional_args": ["key ...", "arg", "arg ..."]},
    "EVALSHA": {"args": ["sha1", "numkeys", "key"], "optional_args": ["key ...", "arg", "arg ..."]},
    "SCRIPT": {"args": ["subcommand"], "optional_args": ["arg", "arg ..."]},
    # DragonFly specific commands
    "DF.STATS": {"args": [], "optional_args": []},
    "DF.INFO": {"args": [], "optional_args": []},
    "MEMORY": {"args": ["USAGE", "key"], "optional_args": ["SAMPLES", "count"]},
    "CAS": {"args": ["key", "oldval", "newval"], "optional_args": []},
}

# Merge commands
REDIS_COMMANDS.update(ADDITIONAL_COMMANDS)

# Data types for random value generation
DATA_TYPES = {
    "string": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
        for _ in range(random.randint(1, 20))
    ),
    "integer": lambda: str(random.randint(-1000000, 1000000)),
    "float": lambda: str(random.uniform(-1000000, 1000000)),
    "key": lambda: "key:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "field": lambda: "field:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "member": lambda: "member:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "channel": lambda: "channel:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "pattern": lambda: "*:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789*?[]")
        for _ in range(random.randint(1, 10))
    ),
    "score": lambda: str(random.uniform(-1000, 1000)),
    "index": lambda: str(random.randint(-100, 100)),
    "count": lambda: str(random.randint(1, 100)),
    "cursor": lambda: str(random.randint(0, 10000)),
    "increment": lambda: str(random.randint(-100, 100)),
    "decrement": lambda: str(random.randint(-100, 100)),
    "seconds": lambda: str(random.randint(1, 3600)),
    "milliseconds": lambda: str(random.randint(1, 3600000)),
    "offset": lambda: str(random.randint(0, 100)),
    "start": lambda: str(random.randint(-100, 100)),
    "end": lambda: str(random.randint(-100, 100)),
    "stop": lambda: str(random.randint(-100, 100)),
    "min": lambda: str(random.randint(-1000, 1000)),
    "max": lambda: str(random.randint(-1000, 1000)),
    "subcommand": lambda: random.choice(["CHANNELS", "NUMPAT", "NUMSUB"]),
    "section": lambda: random.choice(
        [
            "SERVER",
            "CLIENTS",
            "MEMORY",
            "PERSISTENCE",
            "STATS",
            "REPLICATION",
            "CPU",
            "COMMANDSTATS",
            "CLUSTER",
            "KEYSPACE",
        ]
    ),
    "message": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
        for _ in range(random.randint(1, 50))
    ),
    "value": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
        for _ in range(random.randint(1, 50))
    ),
    "element": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
        for _ in range(random.randint(1, 50))
    ),
    "script": lambda: "return {KEYS[1],ARGV[1]}",
    "numkeys": lambda: str(random.randint(0, 3)),
    "sha1": lambda: "".join(random.choice("0123456789abcdef") for _ in range(40)),
    "password": lambda: "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(random.randint(4, 12))
    ),
    "username": lambda: "".join(
        random.choice(string.ascii_letters) for _ in range(random.randint(3, 8))
    ),
}


# Enhanced DATA_TYPES with special characters and escaped sequences
def enhance_data_types():
    global DATA_TYPES

    # Add functions to generate special characters and escaped strings
    DATA_TYPES.update(
        {
            "special_string": lambda: "".join(
                random.choice(string.ascii_letters + string.digits + SPECIAL_CHARS)
                for _ in range(random.randint(1, 20))
            ),
            "escaped_string": lambda: random.choice(ESCAPED_CHARS)
            + "".join(
                random.choice(string.ascii_letters + string.digits)
                for _ in range(random.randint(1, 10))
            ),
            "mixed_string": lambda: "".join(
                random.choice(
                    [
                        lambda: random.choice(string.ascii_letters + string.digits),
                        lambda: random.choice(SPECIAL_CHARS),
                        lambda: random.choice(ESCAPED_CHARS),
                    ]
                )()
                for _ in range(random.randint(5, 20))
            ),
            "binary_string": lambda: "\\x"
            + "".join(format(random.randint(0, 255), "02x") for _ in range(random.randint(1, 10))),
        }
    )

    # Create enhanced versions of existing types
    enhanced_types = {}
    for key, func in DATA_TYPES.items():
        if key.endswith("string") or key in [
            "value",
            "message",
            "element",
            "key",
            "field",
            "member",
            "pattern",
        ]:
            enhanced_types[f"special_{key}"] = lambda k=key: DATA_TYPES[k]() + random.choice(
                SPECIAL_CHARS
            )
            enhanced_types[f"escaped_{key}"] = lambda k=key: DATA_TYPES[k]() + random.choice(
                ESCAPED_CHARS
            )

    DATA_TYPES.update(enhanced_types)


def create_afl_dictionary():
    """Creates AFL++ dictionary with Redis commands"""
    # Enhance data types with special characters
    enhance_data_types()

    dictionary = []

    # Adding all commands
    for command in REDIS_COMMANDS:
        dictionary.append(f'"{command}"')

    # Adding typical arguments
    for data_type, generator in DATA_TYPES.items():
        # Skip enhanced data types that are just variations of base types
        if data_type.startswith("special_") or data_type.startswith("escaped_"):
            continue

        for _ in range(10):  # Adding 10 examples of each type
            try:
                value = generator()
                # Escape special characters for dictionary
                value = re.sub(r'([\\"])', r"\\\1", str(value))
                dictionary.append(f'"{value}"')
            except Exception as e:
                print(f"Error generating value for {data_type}: {e}")

    # Add special characters as standalone entries
    for char in SPECIAL_CHARS:
        # Escape special characters
        escaped_char = re.sub(r'([\\"])', r"\\\1", char)
        dictionary.append(f'"{escaped_char}"')

    # Add escaped sequences
    for esc in ESCAPED_CHARS:
        dictionary.append(f'"{esc}"')

    # Add some complex mixed values
    for _ in range(20):
        try:
            value = DATA_TYPES["mixed_string"]()
            value = re.sub(r'([\\"])', r"\\\1", value)
            dictionary.append(f'"{value}"')
        except:
            pass

    # Writing dictionary to file
    dict_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "redis.dict")
    with open(dict_path, "w") as f:
        f.write("\n".join(dictionary))

    print(f"Dictionary created: {dict_path}")
    return dict_path


def main():
    parser = argparse.ArgumentParser(description="Generate dictionary for Redis fuzzing with AFL++")
    parser.add_argument(
        "--output", default=DICT_FILE, help=f"Dictionary output file (default: {DICT_FILE})"
    )
    args = parser.parse_args()

    global DICT_FILE
    DICT_FILE = args.output

    create_afl_dictionary()


if __name__ == "__main__":
    main()
