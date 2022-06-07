#!/bin/sh

# This is important in order to provide enough locked memory to dragonfly
# when running on kernels < 5.12.
# This line should reside before `set -e` so it could fail silently
# in case the container runs in non-privileged mode.
ulimit -l 65000 2> /dev/null

set -e

# first arg is `-some-option`
if [ "${1#-}" != "$1" ]; then
    # override arguments by prepending "dragonfly --logtostderr" to them.
	set -- dragonfly --logtostderr "$@"
fi

# allow the docker container to be started with `--user`
if [ "$1" = 'dragonfly' -a "$(id -u)" = '0' ]; then
	exec su-exec dfly "$0" "$@"   # runs this script under user dfly
fi

exec "$@"
