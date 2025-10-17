import dataclasses
import enum
import os.path
import time

import requests

RELEASE_URL = "https://api.github.com/repos/dragonflydb/dragonfly/releases"


class AssetKind(enum.Enum):
    RPM = 1
    DEB = 2


@dataclasses.dataclass
class Package:
    kind: AssetKind
    download_url: str
    version: str
    filename: str
    arch: str

    @staticmethod
    def from_url(url: str) -> "Package":
        tokens = url.split("/")
        filename = tokens[-1]
        kind = AssetKind.RPM if filename.endswith(".rpm") else AssetKind.DEB
        if kind == AssetKind.DEB:
            arch = filename.split(".")[0].split("_")[1]
        else:
            arch = filename.split(".")[1]
        return Package(
            kind=kind, download_url=url, version=tokens[-2], filename=filename, arch=arch
        )

    def storage_path(self, root: str) -> str:
        match self.kind:
            case AssetKind.RPM:
                return os.path.join(root, "rpm", self.version)
            case AssetKind.DEB:
                return os.path.join("deb_tmp", self.arch, self.version)


def collect_download_urls() -> list[Package]:
    packages = []
    # TODO retry logic
    response = requests.get(RELEASE_URL)
    releases = response.json()
    for release in releases[:5]:
        for asset in release["assets"]:
            if asset["name"].endswith(".rpm") or asset["name"].endswith(".deb"):
                packages.append(Package.from_url(asset["browser_download_url"]))
    return packages


def download_packages(root: str, packages: list[Package]):
    # The debian repository building tool, reprepo, only supports a single package per version by default.
    # The ability to support multiple versions has been added but is not present in ubuntu-latest on
    # github action runners yet. So we only download one package, the latest, for ubuntu.
    # The rest of the scripts work on a set of packages, so that when the Limit parameter is supported,
    # we can remove this flag and start hosting more than the latest versions.
    # Another alternative would be to use the components feature of reprepo, but it would involve updating
    # the repository definition itself for each release, which is a bad experience for end users.
    deb_done = False
    for package in packages:
        if package.kind == AssetKind.DEB and deb_done:
            continue

        print(f"Downloading {package.download_url}")
        path = package.storage_path(root)
        if not os.path.exists(path):
            os.makedirs(path)

        target = os.path.join(path, package.filename)
        # TODO retry logic
        response = requests.get(package.download_url)
        with open(target, "wb") as f:
            f.write(response.content)
        print(f"Downloaded {package.download_url}")
        time.sleep(0.5)
        if package.kind == AssetKind.DEB:
            deb_done = True


def main():
    packages = collect_download_urls()
    download_packages("_site", packages)


if __name__ == "__main__":
    main()
