# Installation Packages

## Overview
This directory includes a set of files and scripts to build installation package for various Linux distributions.

## Debian
The file to build the Debian package all located under "debian" directory.
The resulting package will install the binary of Dragonfly as well as generate a new service entry for dragonfly,
that can be controlled with "systemctl" command, to start, stop and check status of.
### Building
To build the package, you have a script called "generate_debian_package.sh". This script accepts the following parameters:
* Optional binary path - the location from which to take the binary for the installation. The default for this is "repo path/build-opt".
The location to which the resulting package is writing is at the location from which the script is executed.
This script is depends on the following packages:
* git
* moreutils
* debhelper
* dpkg-dev

To build:
```
/path/to/dragonfly/tools/packaging/generate_debian_package.sh [/path/to/dragonfly-binary-file]
```

This can only be run on Debian based hosts.
You can use the flowing docker file to generate this package:
```
FROM ubuntu:20.04
ARG DEBIAN_FRONTEND=noninteractive
RUN apt update -y && apt-get install -y gcc dpkg-dev gpg vim wget git moreutils debhelper
```
Build the above docker and then run it with your dragonfly source code path mount as volume for the build:
```
docker build -t ubuntu-package .
docker run --rm -ti -v /path/to/dragonfly-repo:/mydocker-path ubuntu-package bash
```
Again note that you need to be at "main" branch to successfully build this package.
Note: If at the end of the installation you see a message "/usr/bin/deb-systemd-helper: error: systemctl preset failed on dragonfly.service: No such file or directory",
you can ignore it, this seem to be related to [the following issue](https://groups.google.com/g/linux.debian.bugs.dist/c/m6xGZ82TdvM).
