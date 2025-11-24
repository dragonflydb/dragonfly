# Package repositories for rpm and debian packages

This directory contains scripts and definitions for setting up YUM and apt repositories for Linux users to install
dragonfly packages.

The repositories are served as static websites. The generate-site workflow is used to set up and deploy the sites using
scripts and definitions included here.

The workflow does the following tasks:

* Download the latest 5 releases from dragonfly releases page, specifically deb and rpm assets
    * for deb files, only the latest package is downloaded and present (see note below)
* Set up a directory structure separating deb and rpm files into version specific paths
* Sign the packages (see note on GPG)
* Deploy the assets prepared, along with the public GPG key and repo definitions for apt and rpm tooling

## Using the YUM repository

Add the repository using:

```shell
sudo dnf config-manager addrepo --from-repofile=https://packages.dragonflydb.io/dragonfly.repo
```

Then install dragonfly as usual, or a specific version:

```shell
sudo dnf -y install dragonfly-0:v1.33.1-1.fc30.x86_64
```

## Using the APT repository

First download the public GPG key to an appropriate location:

```shell
sudo curl -Lo /usr/share/keyrings/dragonfly-keyring.public https://packages.dragonflydb.io/pgp-key.public
```

Then add the sources file:

```shell
sudo curl -Lo /etc/apt/sources.list.d/dragonfly.sources https://packages.dragonflydb.io/dragonfly.sources
```

Finally install dragonfly using apt

```shell
sudo apt update && sudo apt install dragonfly
```

#### Versions in APT repository

Unlike the yum repo, the apt repo only has the latest version. The reason for this is the tool, `reprepro` supplied by
debian to build repositories only supports multiple
versions in version 5.4 onwards, and the github runner using ubuntu-latest does not have this version.

Another option would be to use the components feature of apt repositories in the sources file we ask users to install,
but then the versions would need
to be hardcoded in the sources file and the user would have
to update the file with each new release which makes for a bad user experience. As of now users wanting older packages
should download them directly.

### Signing packages

The packages are signed using the GPG key imported from the secret GPG_PRIVATE_KEY in this repository.

The corresponding public key is served with site assets, so the apt/yum/dnf based tooling can consume the public key to
verify package integrity.

### TODO

- [X] debian packages signing (not required? release file is signed)
- [X] debian repo metadata setup
- [ ] tests asserting that packages are installable?
