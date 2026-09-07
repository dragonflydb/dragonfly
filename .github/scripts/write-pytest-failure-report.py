#!/usr/bin/env python3
"""Append failed Pytest cases for manual regression workflow artifacts.

The utility is used for failed iterations when manual regression runs are
enabled; it is not part of the scheduled regression workflow.
"""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def main() -> int:
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} JUNIT_XML ITERATION OUTPUT_FILE", file=sys.stderr)
        return 2

    junit_file = Path(sys.argv[1])
    iteration = sys.argv[2]
    output_file = Path(sys.argv[3])

    if not junit_file.is_file():
        print(f"JUnit XML not found: {junit_file}", file=sys.stderr)
        return 1

    try:
        root = ET.parse(junit_file).getroot()
    except ET.ParseError as error:
        print(f"Could not parse JUnit XML {junit_file}: {error}", file=sys.stderr)
        return 1

    failed_tests = []
    for testcase in root.iter():
        if local_name(testcase.tag) != "testcase":
            continue
        if any(local_name(child.tag) in {"failure", "error"} for child in testcase):
            classname = testcase.attrib.get("classname", "")
            name = testcase.attrib.get("name", "unknown")
            failed_tests.append(f"{classname}::{name}" if classname else name)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("a", encoding="utf-8") as output:
        output.write(f"Pytest iteration {iteration}\n")
        if failed_tests:
            output.writelines(f"  {test_name}\n" for test_name in failed_tests)
        else:
            output.write("  No failed test cases were recorded in the JUnit XML.\n")
        output.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
