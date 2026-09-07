#!/usr/bin/env bash
set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Usage
usage() {
  echo -e "${CYAN}Usage: $0 [output_path]${NC}"
  exit 1
}

# Check openssl
if ! command -v openssl &>/dev/null; then
  echo -e "${RED}Error: openssl is not installed.${NC}"
  exit 1
fi

# Output directory
OUTDIR="${1:-.}"
mkdir -p "$OUTDIR"
cd "$OUTDIR"

echo -e "${YELLOW}Generating CA key and certificate...${NC}"
openssl req -x509 -newkey rsa:4096 -days 10000 -nodes -keyout ca-key.pem -out ca-cert.pem --subj "/C=GR/ST=SKG/L=Thessaloniki/O=KK/OU=AcmeStudios/CN=Gr/emailAddress=acme@gmail.com"

echo -e "${YELLOW}Generating Dragonfly key and CSR...${NC}"
openssl req -newkey rsa:4096 -nodes -keyout df-key.pem -out df-req.pem --subj "/C=GR/ST=SKG/L=Thessaloniki/O=KK/OU=Comp/CN=Gr/emailAddress=foo@gmai.com"

echo -e "${YELLOW}Signing Dragonfly certificate...${NC}"
openssl x509 -req -in df-req.pem -days 10000 -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out df-cert.pem

echo -e "${YELLOW}Generating client key and CSR...${NC}"
openssl req -newkey rsa:4096 -nodes -keyout cl-key.pem -out cl-req.pem --subj "/C=GR/ST=SKG/L=Thessaloniki/O=KK/OU=Comp/CN=Gr/emailAddress=foo@bar.com"

echo -e "${YELLOW}Signing client certificate...${NC}"
openssl x509 -req -in cl-req.pem -days 10000 -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out cl-cert.pem

echo -e "${GREEN}SUCCESS: TLS certificates generated in ${OUTDIR}${NC}"

echo -e "${CYAN}\n### How to run dfly with TLS:${NC}"
echo -e "${YELLOW}First:${NC}"
echo -e "${CYAN}./dragonfly --tls --tls_key_file=${OUTDIR}/df-key.pem --tls_cert_file=${OUTDIR}/df-cert.pem${NC} --tls_ca_cert_file=${OUTDIR}/ca-cert.pem"

echo -e "${YELLOW}\nThen connect via redis-cli with:${NC}"
echo -e "${CYAN}redis-cli --tls --key ${OUTDIR}/cl-key.pem --cert ${OUTDIR}/cl-cert.pem --insecure${NC}"

echo -e "${YELLOW}\nTo run memtier-benchmark:${NC}"
echo -e "${CYAN}memtier_benchmark --tls --key ${OUTDIR}/cl-key.pem --cert ${OUTDIR}/cl-cert.pem --cacert ${OUTDIR}/ca-cert.pem --tls-skip-verify${NC}"

echo -e "${YELLOW}\nTo run dfly with TLS and CA validation:${NC}"
echo -e "${CYAN}./dragonfly --tls --tls_key_file=${OUTDIR}/df-key.pem --tls_cert_file=${OUTDIR}/df-cert.pem --tls_ca_cert_file=${OUTDIR}/ca-cert.pem${NC}"

echo -e "${YELLOW}\nThen connect via redis-cli without --insecure:${NC}"
echo -e "${CYAN}redis-cli --tls --key ${OUTDIR}/cl-key.pem --cert ${OUTDIR}/cl-cert.pem --cacert ${OUTDIR}/ca-cert.pem${NC}"

echo -e "${YELLOW}\nExport this variable to use the certs path:${NC}"
echo -e "${CYAN}export TLS_CRTS_PATH=${OUTDIR}${NC}"
