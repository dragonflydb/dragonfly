#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import argparse
from redis_commands import (
    DICT_FILE,
    REDIS_COMMANDS,
    DATA_TYPES,
    SPECIAL_CHARS,
    ESCAPED_CHARS,
    enhance_data_types,
)


def create_afl_dictionary(output_file=DICT_FILE):
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
    with open(output_file, "w") as f:
        f.write("\n".join(dictionary))

    print(f"Dictionary created: {output_file}")
    return output_file


def main():
    parser = argparse.ArgumentParser(description="Generate dictionary for Redis fuzzing with AFL++")
    parser.add_argument(
        "--output", default=DICT_FILE, help=f"Dictionary output file (default: {DICT_FILE})"
    )
    args = parser.parse_args()

    create_afl_dictionary(args.output)


if __name__ == "__main__":
    main()
