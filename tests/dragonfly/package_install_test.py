import platform

import pytest
from testcontainers.core.container import DockerContainer


def check(container: DockerContainer, cmd: str):
    result = container.exec(cmd)
    assert result.exit_code == 0, f"command {cmd} failed with result {result}"


@pytest.mark.skipif(platform.processor() != "x86_64", reason="rpm is only built for x86_64")
async def test_install_package_on_fedora():
    with DockerContainer(image="ghcr.io/romange/fedora:30", tty=True) as fedora:
        check(
            fedora,
            "curl -Lo /etc/yum.repos.d/dragonfly.repo https://packages.dragonflydb.io/dragonfly.repo",
        )
        check(fedora, "dnf clean all")
        check(fedora, "dnf makecache")
        check(fedora, "dnf -y install dragonfly")
        check(fedora, "dragonfly --version")


async def test_install_package_on_ubuntu():
    with DockerContainer(image="ghcr.io/romange/ubuntu:noble", tty=True) as ubuntu:
        check(ubuntu, "apt update")
        check(ubuntu, "apt install -y curl")
        check(
            ubuntu,
            "curl -Lo /usr/share/keyrings/dragonfly-keyring.public https://packages.dragonflydb.io/pgp-key.public",
        )
        check(
            ubuntu,
            "curl -Lo /etc/apt/sources.list.d/dragonfly.sources https://packages.dragonflydb.io/dragonfly.sources",
        )
        check(ubuntu, "apt update")
        check(ubuntu, "apt install -y dragonfly")
        check(ubuntu, "dragonfly --version")
