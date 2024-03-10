#!/bin/bash

# This script generates locally-signed TLS files for development usage.
# It's probably a good idea to run in an empty, temporary directory.
#
# Example usage:
#
# mkdir /tmp/dfly-tls
# cd /tmp/dfly-tls
# ~/dragonfly/tools/generate-tls-files.sh
# ~/dragonfly/build-dbg/dragonfly \
#      --dbfilename= \
#      --logtostdout \
#      --tls=true \
#      --tls_key_file=/tmp/dfly-tls/df-key.pem \
#      --tls_cert_file=/tmp/dfly-tls/df-cert.pem \
#      --requirepass=XXX
# redis-cli --tls --cacert /tmp/dfly-tls/ca-cert.pem -a XXX

CA_KEY_PATH=ca-key.pem
CA_CERTIFICATE_PATH=ca-cert.pem
CERTIFICATE_REQUEST_PATH=df-req.pem
PRIVATE_KEY_PATH=df-key.pem
CERTIFICATE_PATH=df-cert.pem

echo "Generating files in local directory (rm *.pem to cleanup)"

openssl req -x509 -newkey rsa:4096 -days 1 -nodes \
  -keyout ${CA_KEY_PATH} \
  -out ${CA_CERTIFICATE_PATH} \
  -subj "/C=GR/ST=SKG/L=Thessaloniki/O=KK/OU=AcmeStudios/CN=Gr/emailAddress=acme@gmail.com"

openssl req -newkey rsa:4096 -nodes \
  -keyout ${PRIVATE_KEY_PATH} \
  -out ${CERTIFICATE_REQUEST_PATH} \
  -subj "/C=GR/ST=SKG/L=Thessaloniki/O=KK/OU=Comp/CN=Gr/emailAddress=does_not_exist@gmail.com"

openssl x509 -req \
  -in ${CERTIFICATE_REQUEST_PATH} \
  -days 1 \
  -CA ${CA_CERTIFICATE_PATH} \
  -CAkey ${CA_KEY_PATH} \
  -CAcreateserial -out ${CERTIFICATE_PATH}

echo "You can now run:"
echo "dragonfly --tls=true --tls_key_file=${PRIVATE_KEY_PATH} --tls_cert_file=${CERTIFICATE_PATH} --requirepass=XXX"
echo "redis-cli --tls --cacert ${CA_CERTIFICATE_PATH} -a XXX"
