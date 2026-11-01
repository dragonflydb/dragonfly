import gzip
import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[2] / ".github/scripts/publish-ci-dashboard-site-to-gcs.py"
SPEC = importlib.util.spec_from_file_location("publish_dashboard_gcs", SCRIPT)
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)


class PublishDashboardGcsTest(unittest.TestCase):
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

    def test_publish_twice_uses_single_gzip_and_preserves_source(self):
        events = []
        staging_paths = []

        def upload(staging, destination, total):
            events.append("data")
            staging_paths.append(staging)
            self.assertNotEqual(staging, self.site)
            self.assertEqual(destination, "gs://bucket/prefix")
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

        def gcloud(*args):
            events.append(args[0])
            if args[0] == "cp":
                self.assertEqual(
                    json.loads(gzip.decompress(Path(args[1]).read_bytes())), {"ranges": []}
                )
                self.assertEqual(args[2], "gs://bucket/prefix/data/manifest.json")
            else:
                self.assertEqual(args[0:3], ("rsync", f"{self.site}/", "gs://bucket/prefix/"))
                self.assertIn("--delete-unmatched-destination-objects", args)
                self.assertIn("--exclude=.*\\.json$", args)

        with (
            patch.object(publisher, "upload_json", side_effect=upload),
            patch.object(publisher, "gcloud", side_effect=gcloud),
        ):
            publisher.publish(self.site, "gs://bucket/prefix")
            publisher.publish(self.site, "gs://bucket/prefix")
        self.assertEqual(events, ["data", "cp", "rsync"] * 2)
        self.assertTrue(all(not path.exists() for path in staging_paths))
        self.assertEqual(
            {
                path.relative_to(self.site).as_posix(): path.read_bytes()
                for path in self.site.rglob("*")
                if path.is_file()
            },
            self.original,
        )

    def test_failed_upload_cleans_staging_and_can_retry(self):
        paths = []

        def fail(staging, *args):
            paths.append(staging)
            raise subprocess.CalledProcessError(1, "gcloud")

        with (
            patch.object(publisher, "upload_json", side_effect=fail),
            patch.object(publisher, "gcloud") as gcloud,
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                publisher.publish(self.site, "gs://bucket")
            gcloud.assert_not_called()
        self.assertFalse(paths[0].exists())
        self.assert_source_unchanged()

        def retry(staging, *args):
            self.assertEqual(
                gzip.decompress((staging / "data/tests/example.json").read_bytes()),
                self.original["data/tests/example.json"],
            )

        with (
            patch.object(publisher, "upload_json", side_effect=retry),
            patch.object(publisher, "gcloud"),
        ):
            publisher.publish(self.site, "gs://bucket")
        self.assert_source_unchanged()

    def test_missing_manifest_fails_before_staging(self):
        (self.site / "data/manifest.json").unlink()
        with patch.object(publisher, "compress_site") as compress:
            with self.assertRaises(FileNotFoundError):
                publisher.publish(self.site, "gs://bucket")
        compress.assert_not_called()

    def test_bulk_upload_and_failure_propagation(self):
        for returncode in (0, 1):
            with self.subTest(returncode=returncode):
                if returncode:
                    error = subprocess.CalledProcessError(1, "gcloud")
                    with patch.object(publisher, "gcloud", side_effect=error):
                        with self.assertRaises(subprocess.CalledProcessError):
                            publisher.upload_json(self.site, "gs://bucket", 1)
                else:
                    with patch.object(publisher, "gcloud") as gcloud:
                        publisher.upload_json(self.site, "gs://bucket", 1)
                    gcloud.assert_called_once_with(
                        "rsync",
                        f"{self.site}/",
                        "gs://bucket/",
                        "--recursive",
                        "--exclude=^data/manifest\\.json$",
                    )

    def assert_source_unchanged(self):
        self.assertEqual(
            {
                path.relative_to(self.site).as_posix(): path.read_bytes()
                for path in self.site.rglob("*")
                if path.is_file()
            },
            self.original,
        )


if __name__ == "__main__":
    unittest.main()
