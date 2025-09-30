#!/bin/sh

# Cleanup function to prevent zombie processes (issue #5844)
# This is critical when dragonfly runs as PID 1 without an init system
cleanup() {
  # Wait for all background/child processes to finish
  wait 2>/dev/null || true
}

# Set trap to ensure cleanup runs on exit, regardless of how the script exits
trap cleanup EXIT

HOST="localhost"
PORT=$HEALTHCHECK_PORT

if [ -z "$HEALTHCHECK_PORT" ]; then
  # try unpriveleged version first. This should cover cases when the container is running
  # without root, for example:
  # docker run  --group-add 999  --cap-drop=ALL --user 999 docker.dragonflydb.io/dragonflydb/dragonfly
  DF_NET=$(netstat -tlnp | grep "/dragonfly")
  if [ -z "$DF_NET" ]; then
    # if we failed, then lets try the priveleged version. is triggerred by the regular command:
    # docker run docker.dragonflydb.io/dragonflydb/dragonfly
    DF_NET=$(su dfly -c "netstat -tlnp" | grep "/dragonfly")
  fi

  # check all the TCP ports, and fetch the port.
  # For cases when dragonfly opens multiple ports, we filter with tail to choose one of them.
  PORT=$(echo $DF_NET | grep -oE ':[0-9]+' | cut -c2- | tail -n 1)
fi

_healthcheck="nc -q1 $HOST $PORT"

# Send PING and check response
# During snapshot loading, server returns "LOADING" error instead of "PONG"
# This handles both issues #5863 (loading detection) and normal healthcheck
RESPONSE=$(echo PING | timeout 3 ${_healthcheck} 2>/dev/null)

# Check if response contains PONG (ready) or LOADING (not ready)
if echo "$RESPONSE" | grep -qi "LOADING"; then
  # Server is loading dataset, not ready for traffic (issue #5863)
  exit 1
elif echo "$RESPONSE" | grep -q "PONG"; then
  # Server is ready
  exit 0
else
  # Unknown response or connection failed
  exit 1
fi
