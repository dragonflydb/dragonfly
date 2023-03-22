#!/bin/sh

HOST="localhost"
PORT=6379

# If we're running with TLS enabled, utilise OpenSSL for the check
if [ -f "/etc/dragonfly/tls/ca.crt" ]
then
    _healthcheck="openssl s_client -connect ${HOST}:${PORT} -CAfile /etc/dragonfly/tls/ca.crt -quiet -no_ign_eof"
else
    _healthcheck="nc -q1 $HOST $PORT"
fi

echo PING | ${_healthcheck}

exit $?
