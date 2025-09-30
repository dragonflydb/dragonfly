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

# Use redis-cli instead of nc for better reliability
# and to properly check loading state (issue #5863)
REDIS_CLI="redis-cli -h $HOST -p $PORT"

# Step 1: Basic PING check
if ! timeout 3 $REDIS_CLI PING 2>/dev/null | grep -q "PONG"; then
  exit 1
fi

# Step 2: Check if server is in LOADING state
# During snapshot loading, the server responds to PING but is not ready for traffic
# Note: 'loading' field is in PERSISTENCE section
INFO_OUTPUT=$(timeout 3 $REDIS_CLI INFO PERSISTENCE 2>/dev/null)

# Server is ready when loading:0
if echo "$INFO_OUTPUT" | grep -q "^loading:0"; then
  exit 0
fi

# If loading:1 or can't determine state, fail to be safe
exit 1
