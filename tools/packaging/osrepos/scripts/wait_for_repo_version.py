import json
import os
import sys
import time
import urllib.request

RELEASES_URL = "https://api.github.com/repos/dragonflydb/dragonfly/releases"
DEB_PACKAGES_URL = "https://packages.dragonflydb.io/deb/dists/noble/main/binary-amd64/Packages"

MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 60


def get_latest_release_version():
    headers = {"Accept": "application/vnd.github+json"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(RELEASES_URL, headers=headers)
    with urllib.request.urlopen(req) as resp:
        releases = json.loads(resp.read())
    for release in releases:
        if not release.get("draft"):
            return release["tag_name"].lstrip("v")
    return None


def get_repo_version():
    req = urllib.request.Request(DEB_PACKAGES_URL)
    with urllib.request.urlopen(req) as resp:
        text = resp.read().decode()
    for line in text.splitlines():
        if line.startswith("Version:"):
            return line.split(":", 1)[1].strip()
    return None


def main():
    latest = get_latest_release_version()
    if not latest:
        print("Could not determine latest release version")
        return 1

    print(f"Latest GitHub release: {latest}")

    for attempt in range(1, MAX_RETRIES + 1):
        repo_version = get_repo_version()
        print(f"Attempt {attempt}/{MAX_RETRIES}: repo version = {repo_version}")
        if repo_version == latest:
            print("Versions match.")
            return 0
        if attempt < MAX_RETRIES:
            print(f"Mismatch (expected {latest}). Retrying in {RETRY_DELAY_SECONDS}s...")
            time.sleep(RETRY_DELAY_SECONDS)

    print(f"FAILED: expected {latest}, repo has {repo_version} after {MAX_RETRIES} attempts")
    return 1


if __name__ == "__main__":
    sys.exit(main())
