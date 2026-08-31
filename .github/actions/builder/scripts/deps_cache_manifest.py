#!/usr/bin/env python3

"""Generate and validate dependency-cache manifests.

Usage:
    python3 .github/actions/builder/scripts/deps_cache_manifest.py generate --root BUILD_DIR --manifest FILE PATH [...]
    python3 .github/actions/builder/scripts/deps_cache_manifest.py validate --root BUILD_DIR --manifest FILE PATH [...]

Requires Python 3.8 or newer.

The CI builder saves this manifest with its dependency cache. On an exact cache hit it
regenerates the manifest before CMake/Ninja; a mismatch means the restored tree is
discarded locally and that job cold-builds dependencies instead of failing repeatedly.
Run .github/actions/builder/scripts/test_deps_cache_manifest.sh after changing this tool or cache-validation behavior;
it is the regression harness for manifest generation, validation, and archive restoration.
Timestamps are intentionally excluded because cache archive tools do not consistently
preserve nanosecond precision.
Hardlink topology is intentionally not recorded; cache correctness depends on each path's
contents rather than its inode identity.

Exit status:
    0: manifest generated or restored cache matches.
    1: restored cache or manifest differs from the generated manifest.
    2: invalid invocation or configuration.
    3: unexpected filesystem or runtime error.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import stat
import sys
from pathlib import Path
from typing import NoReturn


# Manifest body schema. Changing it causes incompatible restored caches to fail validation and rebuild.
FORMAT = "deps-cache-manifest-v2"


def fail(message: str) -> NoReturn:
    print(f"deps-cache-manifest: {message}", file=sys.stderr)
    raise SystemExit(2)


def mismatch(message: str) -> NoReturn:
    print(f"deps-cache-manifest: {message}", file=sys.stderr)
    raise SystemExit(1)


def requested_path(root: Path, requested: str) -> Path | None:
    path = Path(requested)
    if path.is_absolute() or ".." in path.parts:
        fail(f"cached path must be a relative path below the root: {requested}")
    full_path = root / path
    if not os.path.lexists(full_path):
        return None
    try:
        full_path.resolve().relative_to(root)
    except ValueError:
        fail(f"cached path resolves outside the root: {requested}")
    return full_path


def walk(path: Path):
    stack = [(path, path.lstat())]
    while stack:
        current, metadata = stack.pop()
        yield current, metadata
        if stat.S_ISDIR(metadata.st_mode):
            with os.scandir(current) as entries:
                stack.extend(
                    (Path(entry.path), entry.stat(follow_symlinks=False)) for entry in entries
                )


def relative_path(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def record_path(
    root: Path, path: Path, metadata: os.stat_result, checksums: dict[Path, str]
) -> dict[str, object]:
    record: dict[str, object] = {
        "path": relative_path(root, path),
        "mode": stat.S_IMODE(metadata.st_mode),
    }

    if stat.S_ISREG(metadata.st_mode):
        record.update(type="file", size=metadata.st_size, sha256=checksums[path])
    elif stat.S_ISDIR(metadata.st_mode):
        record["type"] = "directory"
    elif stat.S_ISLNK(metadata.st_mode):
        record.update(type="symlink", target=os.readlink(path))
    else:
        mismatch(f"unsupported filesystem type at {record['path']}")
    return record


def build_manifest(root: Path, requested: list[str], workers: int) -> bytes:
    entries: list[tuple[Path, os.stat_result]] = []
    for item in requested:
        path = requested_path(root, item)
        if path is not None:
            entries.extend(walk(path))

    entries.sort(key=lambda entry: os.fsencode(relative_path(root, entry[0])))
    metadata_by_path = dict(entries)
    paths = list(dict.fromkeys(path for path, _ in entries))
    regular_files = [path for path in paths if stat.S_ISREG(metadata_by_path[path].st_mode)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        checksums = dict(zip(regular_files, executor.map(hash_file, regular_files)))

    records = [record_path(root, path, metadata_by_path[path], checksums) for path in paths]
    header = {"format": FORMAT, "paths": requested}
    lines = [json.dumps(header, separators=(",", ":"), sort_keys=True)]
    lines.extend(json.dumps(record, separators=(",", ":"), sort_keys=True) for record in records)
    return ("\n".join(lines) + "\n").encode()


def validate_header(contents: bytes, requested: list[str]) -> None:
    try:
        header = json.loads(contents.split(b"\n", 1)[0])
    except json.JSONDecodeError:
        mismatch("manifest header is not valid JSON")
    if not isinstance(header, dict) or header.get("format") != FORMAT:
        actual_format = header.get("format") if isinstance(header, dict) else None
        mismatch(f"manifest format {actual_format!r} does not match expected {FORMAT!r}")
    if header.get("paths") != requested:
        mismatch("manifest requested paths do not match the validation invocation")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("generate", "validate"))
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--workers", type=int, default=min(os.cpu_count() or 1, 8))
    parser.add_argument("paths", nargs="+")
    arguments = parser.parse_args()
    if arguments.workers < 1:
        parser.error("--workers must be positive")
    return arguments


def main() -> None:
    if sys.version_info < (3, 8):
        fail("Python 3.8 or newer is required")
    arguments = parse_args()
    root = arguments.root.resolve()
    if not root.is_dir():
        fail(f"root is not a directory: {root}")

    if arguments.command == "generate":
        arguments.manifest.write_bytes(build_manifest(root, arguments.paths, arguments.workers))
        return

    if not arguments.manifest.is_file():
        mismatch(f"manifest is missing: {arguments.manifest}")
    saved_contents = arguments.manifest.read_bytes()
    validate_header(saved_contents, arguments.paths)
    if saved_contents != build_manifest(root, arguments.paths, arguments.workers):
        mismatch(f"restored cache does not match {arguments.manifest}")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as error:
        print(f"deps-cache-manifest: unexpected error: {error}", file=sys.stderr)
        raise SystemExit(3)
