#!/bin/sh

set -e

PLATFORM=$1

PSHORT=${PLATFORM#"linux/"}
echo "PSHORT ${PSHORT}"


if [ "${PSHORT}" = "amd64" ]; then
  SUFFIX='x86_64'
else
  SUFFIX='aarch64'
fi

mv /tmp/dragonfly-${SUFFIX} /build/dragonfly
ls -l /build/