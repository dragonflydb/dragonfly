#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "$0")
ROOT_DIR=$(readlink -f "$SCRIPT_DIR/../..")
GEN_DIR=$ROOT_DIR/genfiles/tls


#   genfiles/tls/ca.{crt,key}          Self signed CA certificate.
#   genfiles/tls/dragonfly.{crt,key}   A certificate with no key usage/policy restrictions.
#   genfiles/tls/client.{crt,key}      A certificate restricted for SSL client usage.
#   genfiles/tls/server.{crt,key}      A certificate restricted for SSL server usage.

: '
To run dragonfly use:
dragonfly --tls --tls_key_file ../genfiles/tls/server.key  --tls_cert_file ../genfiles/tls/server.crt -requirepass pass

Or with CA (does not require password):
dragonfly --tls --tls_key_file ../genfiles/tls/server.key  --tls_cert_file ../genfiles/tls/server.crt \
--tls_ca_cert_file ../genfiles/tls/ca.crt

To connect with client (without ca):
openssl s_client   -state -crlf  -connect 127.0.0.1:6379

With CA:
openssl s_client   -state -crlf -CAfile ../genfiles/tls/ca.crt  -cert ../genfiles/tls/client.crt -key ../genfiles/tls/client.key  -connect 127.0.0.1:6379

Similarly, to connect with redis-cli (no CA):
redis-cli --tls --insecure -a pass

With CA:
redis-cli --tls  --cacert ../genfiles/tls/ca.crt  --cert ../genfiles/tls/client.crt --key ../genfiles/tls/client.key

memtier (without CA):
memtier_benchmark --tls --key ../genfiles/tls/client.key  --cert ../genfiles/tls/client.crt -a pass

memtier (with CA):
memtier_benchmark --tls --key ../genfiles/tls/client.key  --cert ../genfiles/tls/client.crt --cacert ../genfiles/tls/ca.crt
'

generate_cert() {
    local name=$1
    local cn="$2"
    local opts="$3"

    local keyfile=$GEN_DIR/${name}.key
    local certfile=$GEN_DIR/${name}.crt

    [ -f $keyfile ] || openssl genpkey -algorithm ED25519 -out $keyfile
    openssl req -new -sha256 \
        -subj "/O=Dragonfly Test/CN=$cn" \
        -key $keyfile | \
        openssl x509 \
            -req -sha256 \
            -CA $GEN_DIR/ca.crt \
            -CAkey $GEN_DIR/ca.key \
            -CAserial $GEN_DIR/ca.txt \
            -CAcreateserial \
            -days 365 \
            $opts \
            -out $certfile
}

mkdir -p $GEN_DIR
[ -f $GEN_DIR/ca.key ] || openssl genpkey -algorithm ED25519 -out $GEN_DIR/ca.key

# -x509: self-signed certificate, -nodes: no password
openssl req \
    -x509 -new -nodes -sha256 \
    -key $GEN_DIR/ca.key \
    -days 3650 \
    -subj '/O=Dragonfly Test/CN=Certificate Authority' \
    -out $GEN_DIR/ca.crt

cat > $GEN_DIR/openssl.cnf <<_END_
[ server_cert ]
keyUsage = digitalSignature, keyEncipherment
nsCertType = server

[ client_cert ]
keyUsage = digitalSignature, keyEncipherment
nsCertType = client
_END_

generate_cert server "Server-only" "-extfile $GEN_DIR/openssl.cnf -extensions server_cert"
generate_cert client "Client-only" "-extfile $GEN_DIR/openssl.cnf -extensions client_cert"
generate_cert dragonfly "Generic-cert"
