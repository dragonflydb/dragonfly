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
    # find all the files in the WORKDIR including the dir itself that do not
    # have dfly user on them and chmod them to dfly.
    find . \! -user dfly -exec chown dfly '{}' +
    # runs this script under user dfly
    exec setpriv --reuid=dfly --regid=dfly --clear-groups -- "$0" "$@"
fi

um="$(umask)"
if [ "$um" = '0022' ]; then
    umask 0077  # restrict access permissions only to the owner
fi

exec "$@"
