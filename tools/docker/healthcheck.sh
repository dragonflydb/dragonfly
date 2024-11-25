#!/bin/sh

HOST="localhost"
PORT=$HEALTHCHECK_PORT


if [ -z "$HEALTHCHECK_PORT" ]; then
  # try unpriveleged version first. This should cover cases when the container is running
  # without root, for example:
  # docker run  --group-add 999  --cap-drop=ALL --user 999 docker.dragonflydb.io/dragonflydb/dragonfly
  DF_NET=$(netstat -tlnp | grep "1/dragonfly")
  if [ -z "$DF_NET" ]; then
    # if we failed, then lets try the priveleged version. is triggerred by the regular command:
    # docker run docker.dragonflydb.io/dragonflydb/dragonfly
    DF_NET=$(su dfly -c "netstat -tlnp" | grep "1/dragonfly")
  fi

  # check all the TCP ports, and fetch the port.
  # For cases when dragonfly opens multiple ports, we filter with tail to choose one of them.
  PORT=$(echo $DF_NET | grep -oE ':[0-9]+' | cut -c2- | tail -n 1)
fi

_healthcheck="nc -q1 $HOST $PORT"

echo PING | ${_healthcheck}

exit $?
