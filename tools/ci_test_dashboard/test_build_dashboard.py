"""Run with python3 -m unittest discover -s tools/ci_test_dashboard."""

import gzip
import json
import tempfile
import time
import unittest
from pathlib import Path

import build_dashboard as dashboard


class BuildDashboardTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def report(self, day, run, content, suite="regression", filename="pytest.xml"):
        path = self.root / (
            f"{suite}/year=2026/month=09/day={day}/CI/{run}/2/build/debug/{filename}"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        metadata = (
            dashboard.metadata_for_dashboard_json
            if path.suffix == ".json"
            else dashboard.metadata_for
        )
        return path, metadata(self.root, path)

    def parse(self, inputs):
        summary, cacheable = dashboard.parse_day(inputs, time.monotonic(), 0, len(inputs))
        self.assertTrue(cacheable)
        return summary

    def test_xml_statuses_and_ctest_metadata(self):
        xml = (
            '<testsuite timestamp="2026-09-23T12:00:00Z">'
            + "".join(
                f'<testcase classname="Suite" name="{name}" time="0.5">{child}</testcase>'
                for name, child in (
                    ("pass", ""),
                    ("fail", '<failure message="assertion"/>'),
                    ("error", '<error message="exception"/>'),
                    ("skip", "<skipped/>"),
                )
            )
            + "</testsuite>"
        )
        for suite, filename, level in (
            ("regression", "pytest.xml", "pytest"),
            ("cpp", "ctest/report.xml", "ctest"),
        ):
            with self.subTest(level=level):
                summary = self.parse([self.report("23", "123", xml, suite, filename)])
                rows = {
                    item["summary"]["name"]: item["summary"] for item in summary["tests"].values()
                }
                self.assertEqual(summary["test_occurrences"], 4)
                self.assertEqual(summary["input_counts"]["reports_failed"], 1)
                for name, counter in (
                    ("pass", "passed"),
                    ("fail", "failed"),
                    ("error", "errored"),
                    ("skip", "skipped"),
                ):
                    self.assertEqual(rows[name][counter], 1)
                    self.assertEqual(rows[name]["level"], level)
                    self.assertEqual(rows[name]["avg_time"], 0.5)

    def test_gtest_json_and_embedded_errors(self):
        payload = {
            "tests": [
                {
                    "classname": "Suite",
                    "name": "case",
                    "status": "failed",
                    "time": 1.25,
                    "message": "failure",
                }
            ],
            "parse_errors": [{"file": "bad.xml", "error": "invalid"}],
        }
        summary = self.parse(
            [self.report("23", "123", json.dumps(payload), "cpp", "gtest-summary.json")]
        )
        compact = next(iter(summary["tests"].values()))
        self.assertEqual(compact["summary"]["level"], "gtest")
        self.assertEqual(compact["summary"]["failed"], 1)
        self.assertEqual(compact["summary"]["avg_time"], 1.25)
        self.assertEqual(summary["input_counts"]["dashboard_json_files"], 1)
        self.assertEqual(summary["input_counts"]["parse_errors"], 1)

    def test_ranges_counts_and_failure_links(self):
        summaries = []
        for day, child in (
            ("16", "<failure message='old'/>"),
            ("17", "<failure message='boundary'/>"),
            ("23", ""),
        ):
            xml = f'<testsuite timestamp="2026-09-{day}T12:00:00Z"><testcase classname="Suite" name="case" time="2">{child}</testcase></testsuite>'
            summaries.append((f"2026-09-{day}", self.parse([self.report(day, day, xml)])))
        output = self.root / "output"
        dashboard.build_from_summaries(
            summaries, {day for day, _ in summaries}, "fixture", output, time.monotonic()
        )
        all_row = json.loads((output / "ranges/all.json").read_text())["tests"][0]
        week_row = json.loads((output / "ranges/7.json").read_text())["tests"][0]
        self.assertEqual((all_row["total"], all_row["failures"]), (3, 2))
        self.assertEqual((week_row["total"], week_row["failures"]), (2, 1))
        self.assertEqual(week_row["failure_rate"], 0.5)
        self.assertTrue(week_row["is_flaky"])
        self.assertFalse(week_row["is_currently_failing"])
        self.assertEqual(week_row["last_failed_run_id"], "17")
        self.assertEqual(week_row["last_failed_run_attempt"], "2")
        detail = json.loads((output / week_row["detail_file"]).read_text())["ranges"]["7"]
        self.assertEqual([item["run_id"] for item in detail["failure_runs"]], ["17"])
        example = detail["failure_examples"][0]
        self.assertEqual(example["run_id"], "17")
        self.assertEqual(example["run_attempt"], "2")
        self.assertEqual(example["report"], week_row["last_failed_report"])
        self.assertEqual(example["message"], "boundary")
        manifest = json.loads((output / "manifest.json").read_text())
        self.assertEqual(manifest["totals"]["test_occurrences"], 3)
        self.assertEqual(manifest["totals"]["unique_tests"], 1)

    def test_cache_header_and_corruption(self):
        path = self.root / "cache.gz"
        header = {
            "date": "2026-09-23",
            "pipeline_fingerprint": "code",
            "source_fingerprint": "reports",
        }
        summary = {"tests": {}}
        dashboard.write_day_cache(path, {**header, "summary": summary})
        self.assertEqual(dashboard.read_day_cache(path, header), summary)
        self.assertIsNone(
            dashboard.read_day_cache(path, {**header, "source_fingerprint": "changed"})
        )
        valid = path.read_bytes()
        for data in (
            valid[:-5],
            b"not gzip",
            gzip.compress(b"invalid json"),
            gzip.compress(b"[]"),
            gzip.compress(json.dumps({**header, "summary": []}).encode()),
        ):
            with self.subTest(data=data):
                path.write_bytes(data)
                self.assertIsNone(dashboard.read_day_cache(path, header))


if __name__ == "__main__":
    unittest.main()
