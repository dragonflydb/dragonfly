#!/usr/bin/env python3
"""Convert GTest JUnit XML files into compact dashboard JSON."""

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path


def main():
    args = parse_args()
    input_dir = args.input_dir.resolve()
    output_json = args.output_json.resolve()

    if not input_dir.exists():
        print(f"GTest JUnit directory does not exist: {input_dir}", file=sys.stderr)
        return 0

    xml_files = sorted(input_dir.rglob("*.xml"))
    records = []
    parse_errors = []

    for xml_file in xml_files:
        try:
            records.extend(read_file(input_dir, xml_file, args))
        except Exception as e:
            parse_errors.append(
                {"file": xml_file.relative_to(input_dir).as_posix(), "error": str(e)}
            )

    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "suite": "cpp",
        "level": "gtest",
        "workflow": args.workflow,
        "run_id": args.run_id,
        "run_attempt": args.run_attempt,
        "job": args.job,
        "variant": args.variant,
        "source_files": len(xml_files),
        "parse_errors": parse_errors,
        "tests": records,
    }

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {output_json} with {len(records)} GTest records from {len(xml_files)} XML files")
    if parse_errors:
        print(f"Encountered {len(parse_errors)} parse errors")
    return 0


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=Path, help="Directory containing GTest JUnit XML")
    parser.add_argument("output_json", type=Path, help="Where to write dashboard JSON")
    parser.add_argument("--workflow", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--run-attempt", default="")
    parser.add_argument("--job", default="")
    parser.add_argument("--variant", default="")
    return parser.parse_args()


def read_file(root, xml_file, args):
    tree = ET.parse(xml_file)
    root_element = tree.getroot()
    suites = (
        [root_element]
        if strip_ns(root_element.tag) == "testsuite"
        else find_children(root_element, "testsuite")
    )

    relative = xml_file.relative_to(root).as_posix()
    binary = xml_file.stem
    group = xml_file.parent.name
    records = []

    for suite in suites:
        suite_name = suite.attrib.get("name", "")
        suite_timestamp = suite.attrib.get("timestamp", "")
        for testcase in direct_children(suite, "testcase"):
            classname = testcase.attrib.get("classname") or suite_name or binary
            name = testcase.attrib.get("name") or "unknown"
            status, message = testcase_status(testcase)

            records.append(
                {
                    "suite": "cpp",
                    "level": "gtest",
                    "binary": binary,
                    "group": group,
                    "classname": classname,
                    "name": name,
                    "status": status,
                    "time": float_or_zero(testcase.attrib.get("time")),
                    "timestamp": testcase.attrib.get("timestamp") or suite_timestamp,
                    "file": testcase.attrib.get("file", ""),
                    "line": int_or_none(testcase.attrib.get("line")),
                    "message": message,
                    "workflow": args.workflow,
                    "run_id": args.run_id,
                    "run_attempt": args.run_attempt,
                    "job": args.job,
                    "variant": args.variant,
                    "source": relative,
                }
            )

    return records


def testcase_status(testcase: ET.Element):
    failure = first_child(testcase, "failure")
    if failure is not None:
        return "failed", message_from(failure)

    error = first_child(testcase, "error")
    if error is not None:
        return "error", message_from(error)

    skipped = first_child(testcase, "skipped")
    if skipped is not None:
        return "skipped", message_from(skipped)

    if testcase.attrib.get("status") == "notrun":
        return "skipped", ""

    return "passed", ""


def first_child(element: ET.Element, wanted: str):
    for child in list(element):
        if strip_ns(child.tag) == wanted:
            return child
    return None


def find_children(element: ET.Element, wanted: str):
    return [child for child in element.iter() if strip_ns(child.tag) == wanted]


def direct_children(element: ET.Element, wanted: str):
    return [child for child in list(element) if strip_ns(child.tag) == wanted]


def strip_ns(tag: str):
    return tag.rsplit("}", 1)[-1]


def message_from(element: ET.Element):
    message = element.attrib.get("message", "")
    text = "".join(element.itertext()).strip()
    return " ".join((message or text).split())[:500]


def float_or_zero(value) -> float:
    try:
        return float(value or 0)
    except ValueError:
        return 0.0


def int_or_none(value):
    try:
        return int(value) if value else None
    except ValueError:
        return None


if __name__ == "__main__":
    raise SystemExit(main())
