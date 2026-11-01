#!/usr/bin/env python3
"""Stage compressed dashboard JSON in parallel, then publish using the GCS CLI."""

import gzip
import os
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def gcloud(*args: str) -> None:
    """Run a GCS command and fail publication immediately if it fails."""
    subprocess.run(["gcloud", "storage", *args], check=True)


def compress_json(json_file: Path, destination: Path) -> None:
    """A helper, creates a gzip copy from json_file without changing source JSON, which the browser decompresses."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(gzip.compress(json_file.read_bytes(), compresslevel=9))


def compress_site(json_files: list[Path], site_dir: Path, staging_dir: Path) -> None:
    """Compress dashboard JSON concurrently into staging to keep Cloud Run responses small."""
    total = len(json_files)
    print(f"Compressing {total} dashboard JSON files", flush=True)
    with ThreadPoolExecutor() as pool:
        destinations = (staging_dir / path.relative_to(site_dir) for path in json_files)
        for completed, _ in enumerate(pool.map(compress_json, json_files, destinations), 1):
            if completed % 1000 == 0 or completed == total:
                print(f"Compressed {completed}/{total} dashboard JSON files", flush=True)


def upload_json(site_dir: Path, destination: str, total: int) -> None:
    """Publish staged JSON before the manifest exposes the corresponding dashboard data."""
    print(f"Publishing {total} dashboard JSON files to {destination}/", flush=True)
    # First upload all JSON files except the manifest to ensure the manifest doesn't point to missing data.
    gcloud(
        "rsync",
        f"{site_dir}/",
        f"{destination}/",
        "--recursive",
        "--exclude=^data/manifest\\.json$",
    )
    print(f"Published {total} dashboard JSON files", flush=True)


def publish(site_dir: Path, destination: str) -> None:
    """Publish a complete site in JSON, manifest, and static-asset phases for consistency."""
    manifest = site_dir / "data" / "manifest.json"
    if not manifest.is_file():
        raise FileNotFoundError(f"Missing generated manifest: {manifest}")

    json_files = list(path for path in (site_dir / "data").rglob("*.json"))
    with tempfile.TemporaryDirectory(prefix="ci-dashboard-gzip-") as temporary:
        staging_dir = Path(temporary)
        compress_site(json_files, site_dir, staging_dir)
        upload_json(staging_dir, destination, len(json_files) - 1)

        print("Publishing dashboard manifest", flush=True)
        gcloud(
            "cp",
            str(staging_dir / "data" / "manifest.json"),
            f"{destination}/data/manifest.json",
        )

    print(f"Publishing dashboard assets to {destination}/", flush=True)
    gcloud(
        "rsync",
        f"{site_dir}/",
        f"{destination}/",
        "--recursive",
        "--delete-unmatched-destination-objects",
        "--exclude=.*\\.json$",
    )
    print("GCS dashboard publication finished", flush=True)


if __name__ == "__main__":
    site_dir = Path(os.environ["DASHBOARD_ROOT"]) / "site"
    publish(site_dir, os.environ["GCS_DASHBOARD_DESTINATION"])
