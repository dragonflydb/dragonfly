"""Exercise incremental builds without AWS credentials or network access."""

import hashlib
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import sync_dashboard as sync


class FakeS3:
    bucket = "test-bucket"

    def __init__(self):
        self.key = "test-results/junit/regression/year=2026/month=09/day=23/CI/123/1/build/debug/pytest.xml"
        self.report = b'<testsuite><testcase classname="Suite" name="case"/></testsuite>'
        self.cache = {}
        self.downloads = 0
        self.change_during_download = False
        self.wrong_size = False

    def list_reports(self, day):
        if day != "2026-09-23":
            return []
        return [
            {
                "Key": self.key,
                "Size": len(self.report) + int(self.wrong_size),
                "ETag": hashlib.sha256(self.report).hexdigest(),
                "LastModified": day,
            }
        ]

    def download_cache(self, key, target):
        if key not in self.cache:
            return False
        target.write_bytes(self.cache[key])
        return True

    def upload_cache(self, source, key):
        self.cache[key] = source.read_bytes()

    def download_reports(self, day, target):
        self.downloads += 1
        path = target / self.key.removeprefix(sync.SOURCE_PREFIX)
        path.parent.mkdir(parents=True)
        path.write_bytes(self.report)
        if self.change_during_download:
            self.report += b"\n"


class SyncDashboardTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.s3 = FakeS3()

    def build(self):
        output = self.root / "output"
        sync.sync_dashboard(self.s3, output, self.root / "work", 2, today=date(2026, 9, 23))
        snapshot = {}
        for path in output.rglob("*.json"):
            payload = json.loads(path.read_text())
            payload.pop("generated_at", None)
            snapshot[path.relative_to(output).as_posix()] = payload
        return snapshot

    def test_warm_cache_matches_cold_build_without_parsing(self):
        cold = self.build()
        with patch.object(
            sync.dashboard, "parse_day", side_effect=AssertionError("warm cache parsed reports")
        ):
            self.assertEqual(self.build(), cold)
        self.assertEqual(self.s3.downloads, 1)

    def test_source_and_pipeline_changes_rebuild(self):
        self.build()
        self.s3.report += b"\n"
        self.build()
        self.assertEqual(self.s3.downloads, 2)
        with patch.object(sync, "pipeline_fingerprint", return_value="new-code"):
            self.build()
        self.assertEqual(self.s3.downloads, 3)

    def test_corrupt_cache_rebuilds(self):
        expected = self.build()
        key = next(iter(self.s3.cache))
        self.s3.cache[key] = b"corrupt"
        self.assertEqual(self.build(), expected)
        self.assertEqual(self.s3.downloads, 2)

    def test_changed_listing_is_not_cached(self):
        self.s3.change_during_download = True
        self.build()
        self.assertEqual(self.s3.cache, {})

    def test_download_size_mismatch_fails_without_caching(self):
        self.s3.wrong_size = True
        with self.assertRaisesRegex(RuntimeError, "differ from the S3 listing"):
            self.build()
        self.assertEqual(self.s3.cache, {})

    def test_fingerprint_is_order_independent_and_tracks_metadata(self):
        reports = self.s3.list_reports("2026-09-23")
        reports.append({**reports[0], "Key": "another-report"})
        expected = sync.source_fingerprint(reports)
        self.assertEqual(sync.source_fingerprint(list(reversed(reports))), expected)
        for field in ("Key", "ETag", "Size", "LastModified"):
            with self.subTest(field=field):
                changed = [dict(item) for item in reports]
                changed[0][field] = "changed"
                self.assertNotEqual(sync.source_fingerprint(changed), expected)


if __name__ == "__main__":
    unittest.main()
