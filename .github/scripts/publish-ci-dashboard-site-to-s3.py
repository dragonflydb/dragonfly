#!/usr/bin/env python3
"""Compress dashboard JSON in place, in parallel, then publish the site using the AWS CLI."""

import gzip
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

GZIP_JSON = ["--content-type", "application/json", "--content-encoding", "gzip"]


def aws(*args: str) -> None:
    subprocess.run(["aws", "s3", *args], check=True)


def compress_json(json_file: Path) -> None:
    json_file.write_bytes(gzip.compress(json_file.read_bytes(), compresslevel=9))


def compress_site(json_files: list[Path]) -> None:
    total = len(json_files)
    print(f"Compressing {total} dashboard JSON files", flush=True)
    with ThreadPoolExecutor() as pool:
        for completed, _ in enumerate(pool.map(compress_json, json_files), 1):
            if completed % 1000 == 0 or completed == total:
                print(f"Compressed {completed}/{total} JSON files", flush=True)


def upload_json(site_dir: Path, destination: str, total: int) -> None:
    print(f"Publishing {total} dashboard JSON files to {destination}/", flush=True)
    command = [
        "aws",
        "s3",
        "cp",
        f"{site_dir}/",
        f"{destination}/",
        "--recursive",
        "--no-progress",
        "--exclude",
        "*",
        "--include",
        "data/*.json",
        "--exclude",
        "data/manifest.json",
        *GZIP_JSON,
        "--cache-control",
        "public,max-age=3600",
    ]
    completed = 0
    with subprocess.Popen(command, stdout=subprocess.PIPE, text=True) as process:
        for line in process.stdout:
            if line.startswith("upload: "):
                completed += 1
                if completed % 500 == 0:
                    print(f"Uploaded {completed}/{total} JSON files", flush=True)
            else:
                print(line, end="", flush=True)
        returncode = process.wait()
    if returncode:
        raise subprocess.CalledProcessError(returncode, command)
    print(f"Published {completed}/{total} dashboard JSON files", flush=True)


def publish(site_dir: Path, destination: str) -> None:
    manifest = site_dir / "data" / "manifest.json"
    if not manifest.is_file():
        raise FileNotFoundError(f"Missing generated manifest: {manifest}")
    json_files = list(path for path in (site_dir / "data").rglob("*.json"))
    compress_site(json_files)
    upload_json(site_dir, destination, len(json_files) - 1)

    print("Publishing dashboard manifest", flush=True)
    aws(
        "cp",
        str(manifest),
        f"{destination}/data/manifest.json",
        *GZIP_JSON,
        "--cache-control",
        "public,max-age=300",
    )

    print(f"Publishing dashboard assets to {destination}/", flush=True)
    aws(
        "sync",
        f"{site_dir}/",
        f"{destination}/",
        "--delete",
        "--exclude",
        "*.json",
        "--cache-control",
        "public,max-age=300",
    )
    print("Dashboard publication finished", flush=True)


if __name__ == "__main__":
    destination = "s3://" + os.environ["PUBLISH_BUCKET"]
    prefix = os.environ.get("PUBLISH_PREFIX", "").rstrip("/")
    if prefix:
        destination += f"/{prefix}"
    site_dir = Path(os.environ["DASHBOARD_ROOT"]) / "site"
    publish(site_dir, destination)
