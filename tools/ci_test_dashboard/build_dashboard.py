#!/usr/bin/env python3
"""Build static dashboard JSON from downloaded JUnit XML and compact test JSON."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

FAIL_STATUSES = {"failed", "error"}
RECENT_LIMIT = 12
EXAMPLE_LIMIT = 4
RANGE_OPTIONS = [
    ("all", "All history", None),
    ("7", "Last 7 days", 7),
    ("14", "Last 14 days", 14),
    ("30", "Last 30 days", 30),
]


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
    groups: set[str] = field(default_factory=set)
    segments: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = field(
        default_factory=dict
    )
    failure_examples: list[dict[str, str]] = field(default_factory=list)

    def add(
        self, meta: Metadata, status: str, timestamp: str, duration: float, message: str
    ) -> None:
        self.groups.add(meta.group)
        self.add_segment(meta, status, timestamp, duration)

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
        return {
            "id": self.test_id,
            "suite": self.suite,
            "level": self.level,
            "classname": self.classname,
            "name": self.name,
            "display_name": self.display_name,
            "groups": unique_sorted(self.groups),
            "segments": self.segments_json(),
            "failure_examples": self.failure_examples,
        }

    def add_segment(self, meta: Metadata, status: str, timestamp: str, duration: float) -> None:
        key = (meta.date, meta.workflow, meta.run_id, meta.attempt, meta.job, meta.variant)
        segment = self.segments.get(key)
        if segment is None:
            segment = {
                "date": meta.date,
                "workflow": meta.workflow,
                "run_id": meta.run_id,
                "run_attempt": meta.attempt,
                "job": meta.job,
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
            key=lambda item: (
                item["date"],
                item["workflow"],
                item["run_id"],
                item["run_attempt"],
                item["job"],
                item["variant"],
            ),
        ):
            row = dict(segment)
            row["total_time"] = round(row["total_time"], 4)
            rows.append(row)
        return rows


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
    parser.add_argument("output_json", type=Path, help="Where to write dashboard JSON files")
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
    parse_errors: list[dict[str, str]] = []
    run_keys: set[str] = set()
    dates: set[str] = set()

    total_input_files = len(xml_files) + len(dashboard_json_files)

    for index, xml_file in enumerate(xml_files, 1):
        if index == 1 or index % 500 == 0:
            print(f"Parsing {index}/{total_input_files}: {xml_file.relative_to(xml_root)}")

        meta = metadata_for(xml_root, xml_file)
        run_keys.add("/".join([meta.workflow, meta.run_id, meta.attempt, meta.job, meta.variant]))
        dates.add(meta.date)

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
        run_keys.add("/".join([meta.workflow, meta.run_id, meta.attempt, meta.job, meta.variant]))
        dates.add(meta.date)

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

    test_rows = []
    for aggregate in tests.values():
        row = aggregate.to_json()
        row["detail_file"] = detail_file_for(row["id"])
        test_rows.append(row)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    output_dir = prepare_output_dir(output_json)

    for row in test_rows:
        write_json(
            output_dir / row["detail_file"],
            {
                "schema_version": 2,
                "generated_at": generated_at,
                "id": row["id"],
                "segments": row["segments"],
                "failure_examples": row["failure_examples"],
            },
        )

    input_counts = {
        "xml_files": len(xml_files),
        "dashboard_json_files": len(dashboard_json_files),
        "reports_passed": reports_by_status["passed"],
        "reports_failed": reports_by_status["failed"],
        "parse_errors": len(parse_errors),
    }

    ranges = []
    for range_id, label, days in RANGE_OPTIONS:
        range_summary = build_range_summary(
            test_rows=test_rows,
            range_id=range_id,
            label=label,
            days=days,
            latest_day=max(dates) if dates else None,
            generated_at=generated_at,
        )
        range_file = f"ranges/{range_id}.json"
        write_json(output_dir / range_file, range_summary)
        ranges.append(
            {
                "id": range_id,
                "label": label,
                "file": range_file,
                "tests": len(range_summary["tests"]),
                "date_range": range_summary["date_range"],
            }
        )

    all_range = next(item for item in ranges if item["id"] == "all")
    default_range = "30" if any(item["id"] == "30" for item in ranges) else all_range["id"]
    manifest = {
        "schema_version": 2,
        "generated_at": generated_at,
        "input_dir": str(input_dir),
        "date_range": {
            "first": min(dates) if dates else None,
            "last": max(dates) if dates else None,
            "days": sorted(dates),
        },
        "default_range": default_range,
        "ranges": ranges,
        "totals": {
            **input_counts,
            "runs": len(run_keys),
            "unique_tests": len(test_rows),
            "test_occurrences": sum(tests_by_status.values()),
        },
        "parse_errors": parse_errors[:100],
    }
    write_json(output_dir / "manifest.json", manifest)

    print(f"Wrote dashboard data under {output_dir}")
    print(
        "Parsed "
        f"{len(xml_files)} XML files and {len(dashboard_json_files)} dashboard JSON files, "
        f"{sum(tests_by_status.values())} occurrences, "
        f"{len(test_rows)} unique tests."
    )
    return 0


def prepare_output_dir(output_path: Path) -> Path:
    output_dir = output_path.parent if output_path.suffix else output_path
    output_dir.mkdir(parents=True, exist_ok=True)

    for name in ("ranges", "tests"):
        shutil.rmtree(output_dir / name, ignore_errors=True)
        (output_dir / name).mkdir(parents=True, exist_ok=True)

    return output_dir


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


def detail_file_for(test_id: str) -> str:
    digest = hashlib.sha256(test_id.encode("utf-8")).hexdigest()[:20]
    return f"tests/{digest}.json"


def build_range_summary(
    test_rows: list[dict[str, Any]],
    range_id: str,
    label: str,
    days: int | None,
    latest_day: str | None,
    generated_at: str,
) -> dict[str, Any]:
    rows = []
    for row in test_rows:
        segments = segments_for_range(row["segments"], latest_day, days)
        summary_row = summary_row_for_segments(row, segments)
        if summary_row:
            rows.append(summary_row)

    sort_rows(rows)
    active_dates = unique_sorted(date for row in rows for date in row.get("active_dates", []))

    return {
        "schema_version": 2,
        "generated_at": generated_at,
        "range": {
            "id": range_id,
            "label": label,
            "days": days,
        },
        "date_range": {
            "first": min(active_dates) if active_dates else None,
            "last": max(active_dates) if active_dates else None,
            "days": active_dates,
        },
        "tests": rows,
    }


def segments_for_range(
    segments: list[dict[str, Any]], latest_day: str | None, days: int | None
) -> list[dict[str, Any]]:
    if days is None or latest_day is None:
        return segments

    try:
        latest = datetime.strptime(latest_day, "%Y-%m-%d").date()
    except ValueError:
        return segments

    cutoff = (latest - timedelta(days=days - 1)).isoformat()
    return [segment for segment in segments if cutoff <= segment.get("date", "") <= latest_day]


def summary_row_for_segments(
    row: dict[str, Any], segments: list[dict[str, Any]]
) -> dict[str, Any] | None:
    if not segments:
        return None

    total = sum_int(segments, "total")
    passed = sum_int(segments, "passed")
    failed = sum_int(segments, "failed")
    errored = sum_int(segments, "errored")
    skipped = sum_int(segments, "skipped")
    total_time = sum_float(segments, "total_time")
    failures = failed + errored
    actionable = passed + failed + errored
    failure_rate = failures / actionable if actionable else 0.0
    last_segment = max(segments, key=lambda item: item.get("last_seen") or "")
    failed_segments = [segment for segment in segments if segment.get("last_failed")]
    last_failed_segment = (
        max(failed_segments, key=lambda item: item.get("last_failed") or "")
        if failed_segments
        else None
    )
    recent = recent_from_segments(segments)
    active_dates = unique_sorted(segment.get("date") for segment in segments)
    active_workflows = unique_sorted(segment.get("workflow") for segment in segments)
    active_variants = unique_sorted(segment.get("variant") for segment in segments)

    return {
        "id": row["id"],
        "detail_file": row["detail_file"],
        "suite": row["suite"],
        "level": row["level"],
        "classname": row["classname"],
        "name": row["name"],
        "display_name": row["display_name"],
        "total": total,
        "passed": passed,
        "failed": failed,
        "errored": errored,
        "skipped": skipped,
        "failures": failures,
        "failure_rate": round(failure_rate, 4),
        "avg_time": round(total_time / total, 4) if total else 0.0,
        "first_seen": min_present(segment.get("first_seen") for segment in segments),
        "last_seen": last_segment.get("last_seen"),
        "last_failed": last_failed_segment.get("last_failed") if last_failed_segment else None,
        "last_failed_run_id": (
            last_failed_segment.get("last_failed_run_id", "") if last_failed_segment else ""
        ),
        "last_failed_run_attempt": (
            last_failed_segment.get("last_failed_run_attempt", "") if last_failed_segment else ""
        ),
        "last_failed_report": (
            last_failed_segment.get("last_failed_report", "") if last_failed_segment else ""
        ),
        "last_status": last_segment.get("last_status", "unknown"),
        "last_workflow": last_segment.get("workflow", ""),
        "last_run_id": last_segment.get("last_run_id", ""),
        "last_variant": last_segment.get("variant", ""),
        "last_report": last_segment.get("last_report", ""),
        "groups": row.get("groups", []),
        "active_dates": active_dates,
        "active_workflows": active_workflows,
        "active_variants": active_variants,
        "is_currently_failing": last_segment.get("last_status") in FAIL_STATUSES,
        "is_flaky": failures > 0 and passed > 0,
        "started_failing_in_sample": started_failing_in_sample(recent),
    }


def sum_int(rows: list[dict[str, Any]], key: str) -> int:
    return sum(int(row.get(key) or 0) for row in rows)


def sum_float(rows: list[dict[str, Any]], key: str) -> float:
    return sum(float(row.get(key) or 0) for row in rows)


def min_present(values: Any) -> str | None:
    present = [value for value in values if value]
    return min(present) if present else None


def recent_from_segments(segments: list[dict[str, Any]]) -> list[dict[str, str]]:
    recent = [
        {
            "status": str(segment.get("last_status", "unknown")),
            "time": str(segment.get("last_seen", "")),
        }
        for segment in segments
        if segment.get("last_seen")
    ]
    recent.sort(key=lambda item: item["time"])
    return recent[-RECENT_LIMIT:]


def unique_sorted(values: Any) -> list[str]:
    return sorted({str(value) for value in values if value})


def sort_rows(rows: list[dict[str, Any]]) -> None:
    rows.sort(
        key=lambda item: (
            item["failures"],
            item["failure_rate"],
            item["is_currently_failing"],
            item["total"],
        ),
        reverse=True,
    )


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
        group=dashboard_record_group(meta, item),
        report_name=source or meta.report_name,
        relative_path=relative_path,
    )


def dashboard_record_group(meta: Metadata, item: dict[str, Any]) -> str:
    group = str(item.get("group") or "")
    binary = str(item.get("binary") or "")
    if group and binary:
        return f"{group}/{binary}"
    return group or binary or meta.group


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
    if meta.level == "gtest":
        source = meta.group or meta.report_name
        if source:
            return f"{meta.suite}/{meta.level}/{source}/{classname}::{name}"
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
