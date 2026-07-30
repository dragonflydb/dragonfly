#!/usr/bin/env python3
"""Build compact dashboard JSON from downloaded JUnit XML."""

from __future__ import annotations

import argparse
import json
import math
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

FAIL_STATUSES = {"failed", "error"}
RECENT_LIMIT = 12
EXAMPLE_LIMIT = 4
SET_LIMIT = 8


@dataclass
class Metadata:
    suite: str
    date: str
    workflow: str
    run_id: str
    attempt: str
    job: str
    variant: str
    level: str
    group: str
    report_name: str
    relative_path: str


@dataclass
class TestAggregate:
    test_id: str
    suite: str
    level: str
    classname: str
    name: str
    display_name: str
    total: int = 0
    passed: int = 0
    failed: int = 0
    errored: int = 0
    skipped: int = 0
    total_time: float = 0.0
    first_seen: str | None = None
    last_seen: str | None = None
    last_failed: str | None = None
    last_status: str = "unknown"
    last_workflow: str = ""
    last_run_id: str = ""
    last_variant: str = ""
    last_report: str = ""
    workflows: set[str] = field(default_factory=set)
    variants: set[str] = field(default_factory=set)
    groups: set[str] = field(default_factory=set)
    dates: set[str] = field(default_factory=set)
    segments: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)
    recent: list[dict[str, str]] = field(default_factory=list)
    failure_examples: list[dict[str, str]] = field(default_factory=list)

    def add(
        self, meta: Metadata, status: str, timestamp: str, duration: float, message: str
    ) -> None:
        self.total += 1
        self.total_time += duration
        if status == "passed":
            self.passed += 1
        elif status == "failed":
            self.failed += 1
        elif status == "error":
            self.errored += 1
        elif status == "skipped":
            self.skipped += 1

        if self.first_seen is None or timestamp < self.first_seen:
            self.first_seen = timestamp
        if self.last_seen is None or timestamp >= self.last_seen:
            self.last_seen = timestamp
            self.last_status = status
            self.last_workflow = meta.workflow
            self.last_run_id = meta.run_id
            self.last_variant = meta.variant
            self.last_report = meta.relative_path
        if status in FAIL_STATUSES and (self.last_failed is None or timestamp >= self.last_failed):
            self.last_failed = timestamp

        self.workflows.add(meta.workflow)
        self.variants.add(meta.variant)
        self.groups.add(meta.group)
        self.dates.add(meta.date)
        self.add_segment(meta, status, timestamp, duration)
        self.recent.append(
            {
                "status": status,
                "time": timestamp,
            }
        )
        self.recent.sort(key=lambda item: item["time"])
        if len(self.recent) > RECENT_LIMIT:
            self.recent = self.recent[-RECENT_LIMIT:]

        if status in FAIL_STATUSES and message:
            self.failure_examples.append(
                {
                    "status": status,
                    "time": timestamp,
                    "workflow": meta.workflow,
                    "run_id": meta.run_id,
                    "run_attempt": meta.attempt,
                    "variant": meta.variant,
                    "report": meta.relative_path,
                    "message": message,
                }
            )
            self.failure_examples.sort(key=lambda item: item["time"], reverse=True)
            if len(self.failure_examples) > EXAMPLE_LIMIT:
                self.failure_examples = self.failure_examples[:EXAMPLE_LIMIT]

    def to_json(self) -> dict[str, Any]:
        actionable = self.passed + self.failed + self.errored
        failures = self.failed + self.errored
        failure_rate = failures / actionable if actionable else 0.0
        return {
            "id": self.test_id,
            "suite": self.suite,
            "level": self.level,
            "classname": self.classname,
            "name": self.name,
            "display_name": self.display_name,
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "errored": self.errored,
            "skipped": self.skipped,
            "failures": failures,
            "failure_rate": round(failure_rate, 4),
            "avg_time": round(self.total_time / self.total, 4) if self.total else 0.0,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "last_failed": self.last_failed,
            "last_status": self.last_status,
            "last_workflow": self.last_workflow,
            "last_run_id": self.last_run_id,
            "last_variant": self.last_variant,
            "last_report": self.last_report,
            "workflows": sorted_limited(self.workflows),
            "variants": sorted_limited(self.variants),
            "groups": sorted_limited(self.groups),
            "filters": {
                "dates": sorted(self.dates),
                "workflows": sorted(self.workflows),
                "variants": sorted(self.variants),
            },
            "segments": self.segments_json(),
            "recent": self.recent,
            "failure_examples": self.failure_examples,
            "is_currently_failing": self.last_status in FAIL_STATUSES,
            "is_flaky": failures > 0 and self.passed > 0,
            "started_failing_in_sample": started_failing_in_sample(self.recent),
        }

    def add_segment(self, meta: Metadata, status: str, timestamp: str, duration: float) -> None:
        key = (meta.date, meta.workflow, meta.variant)
        segment = self.segments.get(key)
        if segment is None:
            segment = {
                "date": meta.date,
                "workflow": meta.workflow,
                "variant": meta.variant,
                "total": 0,
                "passed": 0,
                "failed": 0,
                "errored": 0,
                "skipped": 0,
                "total_time": 0.0,
                "first_seen": timestamp,
                "last_seen": timestamp,
                "last_failed": timestamp if status in FAIL_STATUSES else None,
                "last_failed_run_id": meta.run_id if status in FAIL_STATUSES else "",
                "last_failed_run_attempt": meta.attempt if status in FAIL_STATUSES else "",
                "last_failed_report": meta.relative_path if status in FAIL_STATUSES else "",
                "last_status": status,
                "last_run_id": meta.run_id,
                "last_report": meta.relative_path,
            }
            self.segments[key] = segment

        segment["total"] += 1
        segment["total_time"] += duration
        if status == "passed":
            segment["passed"] += 1
        elif status == "failed":
            segment["failed"] += 1
        elif status == "error":
            segment["errored"] += 1
        elif status == "skipped":
            segment["skipped"] += 1

        if timestamp < segment["first_seen"]:
            segment["first_seen"] = timestamp
        if timestamp >= segment["last_seen"]:
            segment["last_seen"] = timestamp
            segment["last_status"] = status
            segment["last_run_id"] = meta.run_id
            segment["last_report"] = meta.relative_path
        if status in FAIL_STATUSES and (
            segment["last_failed"] is None or timestamp >= segment["last_failed"]
        ):
            segment["last_failed"] = timestamp
            segment["last_failed_run_id"] = meta.run_id
            segment["last_failed_run_attempt"] = meta.attempt
            segment["last_failed_report"] = meta.relative_path

    def segments_json(self) -> list[dict[str, Any]]:
        rows = []
        for segment in sorted(
            self.segments.values(),
            key=lambda item: (item["date"], item["workflow"], item["variant"]),
        ):
            row = dict(segment)
            row["total_time"] = round(row["total_time"], 4)
            rows.append(row)
        return rows


def sorted_limited(values: set[str]) -> list[str]:
    clean = sorted(value for value in values if value)
    if len(clean) <= SET_LIMIT:
        return clean
    return clean[:SET_LIMIT] + [f"+{len(clean) - SET_LIMIT} more"]


def started_failing_in_sample(recent: list[dict[str, str]]) -> bool:
    if len(recent) < 4:
        return False
    split = max(1, math.floor(len(recent) * 0.7))
    earlier = recent[:split]
    later = recent[split:]
    return all(item["status"] not in FAIL_STATUSES for item in earlier) and any(
        item["status"] in FAIL_STATUSES for item in later
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input_dir", type=Path, help="Root containing downloaded JUnit XML and dashboard JSON"
    )
    parser.add_argument("output_json", type=Path, help="Where to write summary JSON")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional maximum XML files to parse, useful while iterating",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_dir = args.input_dir.resolve()
    output_json = args.output_json.resolve()

    if not input_dir.exists():
        print(f"Input directory does not exist: {input_dir}", file=sys.stderr)
        return 2

    xml_root = input_dir / "junit" if (input_dir / "junit").is_dir() else input_dir
    dashboard_root = input_dir / "dashboard"

    xml_files = sorted(xml_root.rglob("*.xml"))
    dashboard_json_files = (
        sorted(dashboard_root.rglob("gtest-summary.json")) if dashboard_root.is_dir() else []
    )
    if args.limit:
        xml_files = xml_files[: args.limit]

    tests: dict[str, TestAggregate] = {}
    reports_by_status: Counter[str] = Counter()
    tests_by_status: Counter[str] = Counter()
    by_suite: Counter[str] = Counter()
    by_level: Counter[str] = Counter()
    by_workflow: Counter[str] = Counter()
    by_variant: Counter[str] = Counter()
    parse_errors: list[dict[str, str]] = []
    run_keys: set[str] = set()
    dates: set[str] = set()

    total_input_files = len(xml_files) + len(dashboard_json_files)

    for index, xml_file in enumerate(xml_files, 1):
        if index == 1 or index % 500 == 0:
            print(f"Parsing {index}/{total_input_files}: {xml_file.relative_to(xml_root)}")

        meta = metadata_for(xml_root, xml_file)
        add_report_metadata(meta, run_keys, dates, by_suite, by_level, by_workflow, by_variant)

        try:
            records = list(read_testcases(xml_file, meta))
        except ET.ParseError as exc:
            parse_errors.append({"file": meta.relative_path, "error": str(exc)})
            reports_by_status["parse_error"] += 1
            continue

        report_has_failure = False
        for record in records:
            if add_test_record(tests, tests_by_status, meta, record):
                report_has_failure = True

        reports_by_status["failed" if report_has_failure else "passed"] += 1

    for index, json_file in enumerate(dashboard_json_files, len(xml_files) + 1):
        if index == len(xml_files) + 1 or index % 500 == 0:
            print(f"Parsing {index}/{total_input_files}: {json_file.relative_to(dashboard_root)}")

        meta = metadata_for_dashboard_json(dashboard_root, json_file)
        add_report_metadata(meta, run_keys, dates, by_suite, by_level, by_workflow, by_variant)

        try:
            records, embedded_parse_errors = read_dashboard_testcases(json_file, meta)
        except (json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
            parse_errors.append({"file": meta.relative_path, "error": str(exc)})
            reports_by_status["parse_error"] += 1
            continue

        for error in embedded_parse_errors:
            parse_errors.append(
                {
                    "file": f"{meta.relative_path}:{error.get('file', 'unknown')}",
                    "error": str(error.get("error", "unknown error")),
                }
            )
        reports_by_status["parse_error"] += len(embedded_parse_errors)

        report_has_failure = False
        for record_meta, record in records:
            if add_test_record(tests, tests_by_status, record_meta, record):
                report_has_failure = True

        reports_by_status["failed" if report_has_failure else "passed"] += 1

    test_rows = [aggregate.to_json() for aggregate in tests.values()]
    test_rows.sort(
        key=lambda item: (
            item["failures"],
            item["failure_rate"],
            item["is_currently_failing"],
            item["total"],
        ),
        reverse=True,
    )

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    summary = {
        "generated_at": generated_at,
        "input_dir": str(input_dir),
        "date_range": {
            "first": min(dates) if dates else None,
            "last": max(dates) if dates else None,
            "days": sorted(dates),
        },
        "totals": {
            "xml_files": len(xml_files),
            "dashboard_json_files": len(dashboard_json_files),
            "runs": len(run_keys),
            "unique_tests": len(test_rows),
            "test_occurrences": sum(tests_by_status.values()),
            "reports_passed": reports_by_status["passed"],
            "reports_failed": reports_by_status["failed"],
            "parse_errors": len(parse_errors),
            "passed": tests_by_status["passed"],
            "failed": tests_by_status["failed"],
            "errored": tests_by_status["error"],
            "skipped": tests_by_status["skipped"],
            "currently_failing": sum(1 for row in test_rows if row["is_currently_failing"]),
            "flaky": sum(1 for row in test_rows if row["is_flaky"]),
            "started_failing_in_sample": sum(
                1 for row in test_rows if row["started_failing_in_sample"]
            ),
        },
        "breakdowns": {
            "by_suite": dict(sorted(by_suite.items())),
            "by_level": dict(sorted(by_level.items())),
            "by_workflow": dict(sorted(by_workflow.items())),
            "by_variant": dict(sorted(by_variant.items())),
            "tests_by_status": dict(sorted(tests_by_status.items())),
        },
        "facets": {
            "dates": sorted(dates),
            "workflows": sorted(by_workflow),
            "variants": sorted(by_variant),
        },
        "tests": test_rows,
        "parse_errors": parse_errors[:100],
    }

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(summary, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {output_json}")
    print(
        "Parsed "
        f"{len(xml_files)} XML files and {len(dashboard_json_files)} dashboard JSON files, "
        f"{summary['totals']['test_occurrences']} occurrences, "
        f"{summary['totals']['unique_tests']} unique tests."
    )
    return 0


def add_report_metadata(
    meta: Metadata,
    run_keys: set[str],
    dates: set[str],
    by_suite: Counter[str],
    by_level: Counter[str],
    by_workflow: Counter[str],
    by_variant: Counter[str],
) -> None:
    run_keys.add("/".join([meta.workflow, meta.run_id, meta.attempt, meta.job, meta.variant]))
    dates.add(meta.date)
    by_suite[meta.suite] += 1
    by_level[meta.level] += 1
    by_workflow[meta.workflow] += 1
    by_variant[meta.variant] += 1


def add_test_record(
    tests: dict[str, TestAggregate],
    tests_by_status: Counter[str],
    meta: Metadata,
    record: dict[str, Any],
) -> bool:
    status = record["status"]
    tests_by_status[status] += 1

    aggregate = tests.get(record["test_id"])
    if aggregate is None:
        aggregate = TestAggregate(
            test_id=record["test_id"],
            suite=meta.suite,
            level=meta.level,
            classname=record["classname"],
            name=record["name"],
            display_name=record["display_name"],
        )
        tests[record["test_id"]] = aggregate

    aggregate.add(
        meta=meta,
        status=status,
        timestamp=record["timestamp"],
        duration=record["duration"],
        message=record["message"],
    )
    return status in FAIL_STATUSES


def metadata_for(root: Path, xml_file: Path) -> Metadata:
    relative = xml_file.relative_to(root)
    parts = relative.parts

    suite = parts[0] if len(parts) > 0 else "unknown"
    year = value_part(parts, "year", "0000")
    month = value_part(parts, "month", "00")
    day = value_part(parts, "day", "00")
    date = f"{year}-{month}-{day}"

    try:
        day_index = next(i for i, part in enumerate(parts) if part.startswith("day="))
    except StopIteration:
        day_index = 3

    workflow = get_part(parts, day_index + 1, "unknown-workflow")
    run_id = get_part(parts, day_index + 2, "unknown-run")
    attempt = get_part(parts, day_index + 3, "1")
    job = get_part(parts, day_index + 4, "unknown-job")
    variant = get_part(parts, day_index + 5, "unknown-variant")
    tail = parts[day_index + 6 :]

    level = "junit"
    group = ""
    report_name = xml_file.stem

    if suite == "cpp" and tail:
        level = tail[0]
        if level == "gtest" and len(tail) >= 3:
            group = f"{tail[1]}/{Path(tail[2]).stem}"
        elif level == "ctest":
            group = xml_file.stem
        else:
            group = "/".join(tail[:-1])
    elif suite == "regression":
        level = "pytest"
        group = xml_file.stem

    return Metadata(
        suite=suite,
        date=date,
        workflow=workflow,
        run_id=run_id,
        attempt=attempt,
        job=job,
        variant=variant,
        level=level,
        group=group,
        report_name=report_name,
        relative_path=relative.as_posix(),
    )


def metadata_for_dashboard_json(root: Path, json_file: Path) -> Metadata:
    relative = json_file.relative_to(root)
    parts = relative.parts

    suite = parts[0] if len(parts) > 0 else "unknown"
    year = value_part(parts, "year", "0000")
    month = value_part(parts, "month", "00")
    day = value_part(parts, "day", "00")
    date = f"{year}-{month}-{day}"

    try:
        day_index = next(i for i, part in enumerate(parts) if part.startswith("day="))
    except StopIteration:
        day_index = 3

    return Metadata(
        suite=suite,
        date=date,
        workflow=get_part(parts, day_index + 1, "unknown-workflow"),
        run_id=get_part(parts, day_index + 2, "unknown-run"),
        attempt=get_part(parts, day_index + 3, "1"),
        job=get_part(parts, day_index + 4, "unknown-job"),
        variant=get_part(parts, day_index + 5, "unknown-variant"),
        level="gtest",
        group="gtest",
        report_name=json_file.stem,
        relative_path=relative.as_posix(),
    )


def value_part(parts: tuple[str, ...], key: str, default: str) -> str:
    prefix = f"{key}="
    for part in parts:
        if part.startswith(prefix):
            return part[len(prefix) :]
    return default


def get_part(parts: tuple[str, ...], index: int, default: str) -> str:
    if 0 <= index < len(parts):
        return parts[index]
    return default


def read_dashboard_testcases(
    json_file: Path, meta: Metadata
) -> tuple[list[tuple[Metadata, dict[str, Any]]], list[dict[str, Any]]]:
    payload = json.loads(json_file.read_text(encoding="utf-8"))
    tests_payload = payload.get("tests", [])
    if not isinstance(tests_payload, list):
        raise ValueError("dashboard JSON 'tests' must be a list")

    parse_errors = payload.get("parse_errors", [])
    if not isinstance(parse_errors, list):
        parse_errors = []

    records = []
    for item in tests_payload:
        if not isinstance(item, dict):
            continue

        record_meta = metadata_for_dashboard_record(meta, item)
        classname = str(
            item.get("classname") or item.get("binary") or record_meta.group or "unknown"
        )
        name = str(item.get("name") or "unknown")
        status = normalize_status(item.get("status"))
        timestamp = normalize_timestamp(str(item.get("timestamp") or ""), record_meta.date)
        duration = float_or_zero(item.get("time", item.get("duration", 0)))
        message = normalize_message(item.get("message", ""))

        records.append(
            (
                record_meta,
                {
                    "test_id": make_test_id(record_meta, classname, name),
                    "classname": classname,
                    "name": name,
                    "display_name": display_name(record_meta, classname, name),
                    "status": status,
                    "timestamp": timestamp,
                    "duration": duration,
                    "message": message,
                },
            )
        )

    return records, parse_errors


def metadata_for_dashboard_record(meta: Metadata, item: dict[str, Any]) -> Metadata:
    source = str(item.get("source") or "")
    relative_path = meta.relative_path if not source else f"{meta.relative_path}#{source}"
    return replace(
        meta,
        suite=str(item.get("suite") or meta.suite),
        level=str(item.get("level") or meta.level),
        workflow=str(item.get("workflow") or meta.workflow),
        run_id=str(item.get("run_id") or meta.run_id),
        attempt=str(item.get("run_attempt") or meta.attempt),
        job=str(item.get("job") or meta.job),
        variant=str(item.get("variant") or meta.variant),
        group=str(item.get("group") or item.get("binary") or meta.group),
        report_name=source or meta.report_name,
        relative_path=relative_path,
    )


def normalize_status(value: Any) -> str:
    status = str(value or "unknown").lower()
    if status == "errored":
        return "error"
    if status in {"passed", "failed", "error", "skipped"}:
        return status
    return "unknown"


def normalize_message(value: Any) -> str:
    return " ".join(str(value or "").split())[:500]


def read_testcases(xml_file: Path, meta: Metadata) -> list[dict[str, Any]]:
    tree = ET.parse(xml_file)
    root = tree.getroot()
    suites = [root] if strip_ns(root.tag) == "testsuite" else find_children(root, "testsuite")
    records: list[dict[str, Any]] = []

    for suite in suites:
        suite_name = suite.attrib.get("name", "")
        suite_timestamp = normalize_timestamp(suite.attrib.get("timestamp", ""), meta.date)
        for testcase in find_direct_children(suite, "testcase"):
            classname = testcase.attrib.get("classname") or suite_name or meta.group or "unknown"
            name = testcase.attrib.get("name") or "unknown"
            timestamp = normalize_timestamp(
                testcase.attrib.get("timestamp", "") or suite_timestamp,
                meta.date,
            )
            status, message = status_for(testcase)
            duration = float_or_zero(testcase.attrib.get("time", "0"))

            test_id = make_test_id(meta, classname, name)
            records.append(
                {
                    "test_id": test_id,
                    "classname": classname,
                    "name": name,
                    "display_name": display_name(meta, classname, name),
                    "status": status,
                    "timestamp": timestamp,
                    "duration": duration,
                    "message": message,
                }
            )

    return records


def find_children(element: ET.Element, wanted: str) -> list[ET.Element]:
    return [child for child in element.iter() if strip_ns(child.tag) == wanted]


def find_direct_children(element: ET.Element, wanted: str) -> list[ET.Element]:
    return [child for child in list(element) if strip_ns(child.tag) == wanted]


def strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def status_for(testcase: ET.Element) -> tuple[str, str]:
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


def first_child(element: ET.Element, wanted: str) -> ET.Element | None:
    for child in list(element):
        if strip_ns(child.tag) == wanted:
            return child
    return None


def message_from(element: ET.Element) -> str:
    message = element.attrib.get("message", "")
    text = "".join(element.itertext()).strip()
    combined = message or text
    combined = " ".join(combined.split())
    return combined[:500]


def make_test_id(meta: Metadata, classname: str, name: str) -> str:
    return f"{meta.suite}/{meta.level}/{classname}::{name}"


def display_name(meta: Metadata, classname: str, name: str) -> str:
    if meta.level == "ctest":
        return name
    return f"{classname}::{name}"


def normalize_timestamp(value: str, fallback_date: str) -> str:
    if not value:
        return f"{fallback_date}T00:00:00Z"
    normalized = value.strip()
    if normalized.endswith("+00:00"):
        normalized = normalized[:-6] + "Z"
    if "T" not in normalized:
        normalized = f"{fallback_date}T00:00:00Z"
    if normalized.endswith("Z"):
        return normalized
    if "+" in normalized[10:] or "-" in normalized[10:]:
        return normalized
    return normalized + "Z"


def float_or_zero(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


if __name__ == "__main__":
    raise SystemExit(main())
