"""Test publication contracts with all AWS subprocesses mocked."""

import gzip
import importlib.util
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


SCRIPT = Path(__file__).resolve().parents[2] / ".github/scripts/publish-ci-dashboard-site-to-s3.py"
SPEC = importlib.util.spec_from_file_location("publish_dashboard", SCRIPT)
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)


class PublishDashboardTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.site = Path(temporary.name) / "site"
        self.original = {
            "index.html": b"<html>Dashboard</html>",
            "data/manifest.json": b'{"ranges": []}',
            "data/ranges/all.json": b'{"tests": []}',
            "data/tests/example.json": b'{"id": "example"}',
        }
        for relative, data in self.original.items():
            path = self.site / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)

    def assert_source_unchanged(self):
        self.assertEqual(
            {
                path.relative_to(self.site).as_posix(): path.read_bytes()
                for path in self.site.rglob("*")
                if path.is_file()
            },
            self.original,
        )

    def test_publish_twice_uses_single_gzip_and_preserves_source(self):
        events = []
        staging_paths = []

        def upload(staging, destination, total):
            events.append("data")
            staging_paths.append(staging)
            self.assertNotEqual(staging, self.site)
            self.assertEqual(destination, "s3://bucket/prefix")
            self.assertEqual(total, 2)
            staged = {
                path.relative_to(staging).as_posix()
                for path in staging.rglob("*")
                if path.is_file()
            }
            self.assertEqual(staged, {name for name in self.original if name.endswith(".json")})
            for relative in staged:
                self.assertEqual(
                    gzip.decompress((staging / relative).read_bytes()), self.original[relative]
                )

        def aws(*args):
            events.append(args[0])
            if args[0] == "cp":
                self.assertEqual(
                    json.loads(gzip.decompress(Path(args[1]).read_bytes())), {"ranges": []}
                )
                self.assertEqual(args[2], "s3://bucket/prefix/data/manifest.json")
                self.assertEqual(args[args.index("--content-encoding") + 1], "gzip")
                self.assertEqual(args[args.index("--content-type") + 1], "application/json")
                self.assertEqual(args[-1], "public,max-age=300")
            else:
                self.assertEqual(args[0:3], ("sync", f"{self.site}/", "s3://bucket/prefix/"))
                self.assertEqual(args[args.index("--exclude") + 1], "*.json")

        with (
            patch.object(publisher, "upload_json", side_effect=upload),
            patch.object(publisher, "aws", side_effect=aws),
        ):
            publisher.publish(self.site, "s3://bucket/prefix")
            publisher.publish(self.site, "s3://bucket/prefix")
        self.assertEqual(events, ["data", "cp", "sync"] * 2)
        self.assertTrue(all(not path.exists() for path in staging_paths))
        self.assert_source_unchanged()

    def test_failed_upload_cleans_staging_and_can_retry(self):
        paths = []

        def fail(staging, *args):
            paths.append(staging)
            raise subprocess.CalledProcessError(1, "aws")

        with (
            patch.object(publisher, "upload_json", side_effect=fail),
            patch.object(publisher, "aws") as aws,
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                publisher.publish(self.site, "s3://bucket")
            aws.assert_not_called()
        self.assertFalse(paths[0].exists())
        self.assert_source_unchanged()

        def retry(staging, *args):
            self.assertEqual(
                gzip.decompress((staging / "data/tests/example.json").read_bytes()),
                self.original["data/tests/example.json"],
            )

        with (
            patch.object(publisher, "upload_json", side_effect=retry),
            patch.object(publisher, "aws"),
        ):
            publisher.publish(self.site, "s3://bucket")
        self.assert_source_unchanged()

    def test_missing_manifest_fails_before_compression(self):
        (self.site / "data/manifest.json").unlink()
        with patch.object(publisher, "compress_site") as compress:
            with self.assertRaises(FileNotFoundError):
                publisher.publish(self.site, "s3://bucket")
            compress.assert_not_called()

    def test_bulk_upload_headers_filters_and_failure_propagation(self):
        for returncode in (0, 1):
            with self.subTest(returncode=returncode):
                process = MagicMock()
                process.__enter__.return_value = process
                process.stdout = io.StringIO("upload: file to s3://bucket/file\n")
                process.wait.return_value = returncode
                with patch.object(publisher.subprocess, "Popen", return_value=process) as popen:
                    if returncode:
                        with self.assertRaises(subprocess.CalledProcessError):
                            publisher.upload_json(self.site, "s3://bucket", 1)
                    else:
                        publisher.upload_json(self.site, "s3://bucket", 1)
                command = popen.call_args.args[0]
                self.assertEqual(command[:5], ["aws", "s3", "cp", f"{self.site}/", "s3://bucket/"])
                self.assertEqual(command[command.index("--include") + 1], "data/*.json")
                self.assertIn(
                    ["--exclude", "data/manifest.json"],
                    [command[i : i + 2] for i in range(len(command) - 1)],
                )
                self.assertEqual(command[command.index("--content-encoding") + 1], "gzip")
                self.assertEqual(command[command.index("--content-type") + 1], "application/json")
                self.assertEqual(command[-1], "public,max-age=3600")


if __name__ == "__main__":
    unittest.main()
