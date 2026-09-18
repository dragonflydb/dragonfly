#!/usr/bin/env python3
"""Build the dashboard from S3, reusing fingerprinted daily summaries."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import build_dashboard as dashboard

SOURCE_PREFIX = "test-results/"
DEFAULT_CACHE_PREFIX = "test-results/ci-dashboard-cache"
REPORT_SOURCES = (
    ("junit/cpp", "*/ctest/*.xml"),
    ("junit/regression", "*.xml"),
    ("dashboard/cpp", "*/gtest-summary.json"),
)


def report_sources(day: str) -> Iterator[tuple[str, str]]:
    partition = date.fromisoformat(day).strftime("year=%Y/month=%m/day=%d")
    for root, pattern in REPORT_SOURCES:
        yield f"{SOURCE_PREFIX}{root}/{partition}/", pattern


class S3:
    """Use the workflow's AWS CLI credentials and concurrent bulk transfers."""

    def __init__(self, bucket: str):
        self.bucket = bucket

    def command(self, *args: str, missing_ok: bool = False) -> str | None:
        result = subprocess.run(
            ["aws", *args],
            capture_output=True,
            text=True,
            env={**os.environ, "AWS_PAGER": ""},
        )
        if result.returncode:
            if missing_ok and "(NoSuchKey)" in result.stderr:
                return None
            raise RuntimeError(f"AWS {args[0]} {args[1]} failed: {result.stderr.strip()}")
        return result.stdout

    def list_reports(self, day: str) -> list[dict[str, Any]]:
        reports = []
        for prefix, pattern in report_sources(day):
            # The CLI automatically combines all pages when output is JSON.
            response = json.loads(
                self.command(
                    "s3api",
                    "list-objects-v2",
                    "--bucket",
                    self.bucket,
                    "--prefix",
                    prefix,
                    "--output",
                    "json",
                )
            )
            for item in response.get("Contents", []):
                if fnmatch.fnmatchcase(item["Key"].removeprefix(prefix), pattern):
                    reports.append(
                        {key: item[key] for key in ("Key", "ETag", "Size", "LastModified")}
                    )
        return sorted(reports, key=lambda item: item["Key"])

    def download_cache(self, key: str, target: Path) -> bool:
        return (
            self.command(
                "s3api",
                "get-object",
                "--bucket",
                self.bucket,
                "--key",
                key,
                str(target),
                "--output",
                "json",
                missing_ok=True,
            )
            is not None
        )

    def upload_cache(self, source: Path, key: str) -> None:
        self.command(
            "s3",
            "cp",
            str(source),
            f"s3://{self.bucket}/{key}",
            "--only-show-errors",
            "--no-progress",
        )

    def download_reports(self, day: str, target: Path) -> None:
        for prefix, pattern in report_sources(day):
            self.command(
                "s3",
                "sync",
                f"s3://{self.bucket}/{prefix}",
                str(target / prefix.removeprefix(SOURCE_PREFIX)),
                "--exclude",
                "*",
                "--include",
                pattern,
                "--only-show-errors",
                "--no-progress",
            )


def source_fingerprint(reports: list[dict[str, Any]]) -> str:
    # ETags are change tokens, not necessarily content MD5s.
    data = json.dumps(sorted(reports, key=lambda item: item["Key"]), sort_keys=True)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def pipeline_fingerprint() -> str:
    digest = hashlib.sha256(dashboard.builder_fingerprint().encode("ascii"))
    digest.update(Path(__file__).read_bytes())
    return digest.hexdigest()


def downloaded_inputs(
    root: Path, reports: list[dict[str, Any]]
) -> tuple[list[tuple[Path, dashboard.Metadata]], bool]:
    expected = {item["Key"].removeprefix(SOURCE_PREFIX): item["Size"] for item in reports}
    actual = {
        path.relative_to(root).as_posix(): path.stat().st_size
        for path in root.rglob("*")
        if path.is_file()
    }
    xml_root = root / "junit"
    json_root = root / "dashboard"
    inputs = [
        (path, dashboard.metadata_for(xml_root, path)) for path in sorted(xml_root.rglob("*.xml"))
    ]
    inputs.extend(
        (path, dashboard.metadata_for_dashboard_json(json_root, path))
        for path in sorted(json_root.rglob("gtest-summary.json"))
    )
    return inputs, actual == expected


def s3_day_summaries(
    s3: S3,
    inventories: dict[str, list[dict[str, Any]]],
    cache_prefix: str,
    work_dir: Path,
    started: float,
) -> Iterator[tuple[str, dict[str, Any]]]:
    pipeline = pipeline_fingerprint()
    namespace = f"{cache_prefix}/v{dashboard.CACHE_VERSION}/{pipeline}"
    reused_days = rebuilt_days = reused_files = parsed_files = 0
    for index, (day, reports) in enumerate(sorted(inventories.items()), 1):
        dashboard.log_progress(
            f"Checking day {index}/{len(inventories)}: {day} ({len(reports)} reports)", started
        )
        fingerprint = source_fingerprint(reports)
        header = {
            "cache_version": dashboard.CACHE_VERSION,
            "pipeline_fingerprint": pipeline,
            "source_bucket": s3.bucket,
            "source_fingerprint": fingerprint,
            "date": day,
        }
        key = f"{namespace}/{day}.json.gz"
        # Each day gets a fresh directory: sync cannot reuse stale or removed files.
        with tempfile.TemporaryDirectory(prefix=f"{day}-", dir=work_dir) as temporary:
            cache_path = Path(temporary) / "summary.json.gz"
            summary = (
                dashboard.read_day_cache(cache_path, header)
                if s3.download_cache(key, cache_path)
                else None
            )
            if summary is not None:
                reused_days += 1
                reused_files += len(reports)
                dashboard.log_progress(f"Reused {day}: {len(reports)} reports", started)
            else:
                dashboard.log_progress(f"Downloading {day}: {len(reports)} reports", started)
                root = Path(temporary) / "input"
                s3.download_reports(day, root)
                inputs, matched_listing = downloaded_inputs(root, reports)
                if not inputs:
                    raise RuntimeError(f"No reports downloaded for nonempty day {day}")
                summary, cacheable = dashboard.parse_day(inputs, started, 0, len(inputs))
                if not cacheable:
                    raise RuntimeError(f"Could not read all downloaded reports for {day}")
                rebuilt_days += 1
                parsed_files += len(inputs)
                if source_fingerprint(s3.list_reports(day)) == fingerprint:
                    if not matched_listing:
                        raise RuntimeError(
                            f"Downloaded reports differ from the S3 listing for {day}"
                        )
                    if not dashboard.write_day_cache(cache_path, {**header, "summary": summary}):
                        raise RuntimeError(f"Could not save daily summary for {day}")
                    s3.upload_cache(cache_path, key)
                    dashboard.log_progress(f"Cached {day}: {len(inputs)} reports", started)
                else:
                    # Publish the downloaded snapshot, but never cache it under
                    # metadata from before new reports arrived or were replaced.
                    dashboard.log_progress(
                        f"S3 inputs changed while rebuilding {day}; cache not written", started
                    )
                del inputs
        yield day, summary
        del summary
    dashboard.log_progress(
        f"S3 daily cache: {reused_days} reused, {rebuilt_days} rebuilt; "
        f"reports: {reused_files} reused, {parsed_files} downloaded and parsed",
        started,
    )


def sync_dashboard(
    s3: S3,
    output: Path,
    work_dir: Path,
    lookback_days: int,
    cache_prefix: str = DEFAULT_CACHE_PREFIX,
    today: date | None = None,
) -> None:
    started = time.monotonic()
    today = today or datetime.now(timezone.utc).date()
    inventories = {}
    for offset in reversed(range(lookback_days)):
        day = (today - timedelta(days=offset)).isoformat()
        reports = s3.list_reports(day)
        if reports:
            inventories[day] = reports
        dashboard.log_progress(
            f"Listed day {lookback_days - offset}/{lookback_days}: {day} "
            f"({len(reports)} reports)",
            started,
        )
    work_dir.mkdir(parents=True, exist_ok=True)
    dashboard.build_from_summaries(
        s3_day_summaries(s3, inventories, cache_prefix, work_dir, started),
        set(inventories),
        f"s3://{s3.bucket}/{SOURCE_PREFIX}",
        output,
        started,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-bucket", required=True)
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--cache-prefix", default=DEFAULT_CACHE_PREFIX)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    if not args.source_bucket or "/" in args.source_bucket:
        parser.error("source-bucket must be a bucket name, without s3:// or a prefix")
    if args.lookback_days < 1:
        parser.error("lookback-days must be positive")
    cache_prefix = args.cache_prefix.strip("/")
    if not cache_prefix:
        parser.error("cache-prefix must not be empty")
    output = args.output.resolve()
    output_dir = output.parent if output.suffix else output
    work_dir = args.work_dir.resolve()
    if work_dir.is_relative_to(output_dir):
        parser.error("work-dir must be outside the dashboard output directory")
    try:
        sync_dashboard(S3(args.source_bucket), output, work_dir, args.lookback_days, cache_prefix)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"Dashboard sync failed: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
