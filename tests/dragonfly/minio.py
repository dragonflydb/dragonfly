"""Prepare the pinned MinIO-compatible server used by S3 snapshot tests."""

import argparse
import hashlib
import logging
import os
import platform
import shutil
import tarfile
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# MinIO archived its OSS server and dl.min.io returns HTTP 410 for every build, so the
# binary comes from silo, a maintained fork that is CLI- and API-compatible with it.
MINIO_RELEASE = "RELEASE.2026-09-03T13-18-01Z"
MINIO_VERSION = "20260903131801.0.0"
MINIO_SHA256 = {
    "linux-amd64": "cbe5c01eac0a97ccb22fa252eafa432e8608bfde7e3ea27a324cb5ed625a1e96",
    "linux-arm64": "311846ca9387de36f8e34daa8bf1a130684cc7b8c61aa39581264249eb8df0cf",
    "darwin-amd64": "35b4121f6ca2ba79514c3536e5f7f79ae8f7fe6fb88288efbdfa2ae8d5283131",
    "darwin-arm64": "bcfd90d51ab6dffc34193aac260464097bb1d2decdd2598538e618bc02ada668",
}
GITHUB_API = "https://api.github.com/repos/pgsty/silo"
REQUEST_TIMEOUT = (10, 30)


def _platform_key() -> str:
    arch = platform.machine()
    arch_map = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}
    return f"{platform.system().lower()}-{arch_map.get(arch, arch)}"


def _download(session, url, dest, headers=None):
    with session.get(url, headers=headers, stream=True, timeout=REQUEST_TIMEOUT) as response:
        response.raise_for_status()
        with open(dest, "wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                output.write(chunk)


def _download_archive(asset_name: str, dest: Path, github_token: str):
    url = f"https://github.com/pgsty/silo/releases/download/{MINIO_RELEASE}/{asset_name}"
    with requests.Session() as session:
        session.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=2,
                    backoff_factor=1,
                    status_forcelist=[500, 502, 503, 504],
                )
            ),
        )
        logging.info("Downloading MinIO binary from %s", url)
        try:
            _download(session, url, dest)
            return
        except requests.RequestException as error:
            logging.warning(
                "MinIO release download failed (%s); trying the GitHub API", type(error).__name__
            )

        headers = {"Accept": "application/vnd.github+json"}
        if github_token:
            headers["Authorization"] = f"Bearer {github_token}"
        try:
            with session.get(
                f"{GITHUB_API}/releases/tags/{MINIO_RELEASE}",
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            ) as response:
                response.raise_for_status()
                assets = [
                    asset for asset in response.json()["assets"] if asset["name"] == asset_name
                ]
            if len(assets) != 1:
                raise RuntimeError(f"Expected one GitHub release asset named {asset_name}")

            # Keep credentials on the API request only. Requests strips Authorization
            # when GitHub redirects the download to a different host.
            asset_id = int(assets[0]["id"])
            headers["Accept"] = "application/octet-stream"
            _download(session, f"{GITHUB_API}/releases/assets/{asset_id}", dest, headers)
        except requests.RequestException as error:
            status = error.response.status_code if error.response is not None else "unavailable"

            # Do not include exception URLs: an asset redirect can contain signed query parameters.
            raise RuntimeError(
                f"MinIO download failed via the release URL and GitHub API "
                f"({type(error).__name__}, HTTP status {status})"
            ) from None


def _download_minio_binary(dest: Path, platform_key: str, github_token: str):
    expected_sha256 = MINIO_SHA256.get(platform_key)
    if expected_sha256 is None:
        raise RuntimeError(f"No pinned MinIO build for {platform_key}")

    asset_name = f"silo_{MINIO_VERSION}_{platform_key.replace('-', '_')}.tar.gz"
    tmp_archive = dest.with_name(dest.name + ".tar.gz")
    tmp_dest = dest.with_name(dest.name + ".tmp")
    try:
        _download_archive(asset_name, tmp_archive, github_token)
        actual_sha256 = hashlib.sha256(tmp_archive.read_bytes()).hexdigest()
        if actual_sha256 != expected_sha256:
            raise RuntimeError(
                f"MinIO checksum mismatch for {asset_name}: got {actual_sha256}, "
                f"expected {expected_sha256}"
            )
        with tarfile.open(tmp_archive) as tar:
            with tar.extractfile("silo") as src, open(tmp_dest, "wb") as dst:
                shutil.copyfileobj(src, dst)
        tmp_dest.chmod(0o755)
        tmp_dest.rename(dest)
    finally:
        tmp_archive.unlink(missing_ok=True)
        tmp_dest.unlink(missing_ok=True)


def prepare_minio_binary(cache_dir: Path | None = None, github_token: str = "") -> Path:
    """Return the cached server, verifying new downloads before installing them atomically."""
    if cache_dir is None:
        cache_dir = Path.home() / ".cache" / "dragonfly-tests"
    cache_dir.mkdir(parents=True, exist_ok=True)
    platform_key = _platform_key()
    minio_bin = cache_dir / f"minio-{MINIO_RELEASE}-{platform_key}"
    if not minio_bin.exists():
        _download_minio_binary(minio_bin, platform_key, github_token)
    return minio_bin


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    print(prepare_minio_binary(args.cache_dir, os.environ.get("GITHUB_TOKEN", "")))
