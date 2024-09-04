#!/bin/sh

HOST="localhost"
PORT=$HEALTHCHECK_PORT

if [ -z "$HEALTHCHECK_PORT" ]; then
    # check all the TCP listening sockets, filter the dragonfly process, and fetch the port.
    # For cases when dragonfly opens multiple ports, we filter with tail to choose one of them.
    PORT=$(su dfly -c "netstat -tlnp" | grep "1/dragonfly" | grep -oE ':[0-9]+' | cut -c2- | tail -n 1)
fi

# If we're running with TLS enabled, utilise OpenSSL for the check
if [ -f "/etc/dragonfly/tls/ca.crt" ]
then
    _healthcheck="openssl s_client -connect ${HOST}:${PORT} -CAfile /etc/dragonfly/tls/ca.crt -quiet -no_ign_eof"
else
    _healthcheck="nc -q1 $HOST $PORT"
fi

echo PING | ${_healthcheck}

exit $?
