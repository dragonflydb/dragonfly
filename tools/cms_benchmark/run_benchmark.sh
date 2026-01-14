#!/bin/bash
#
# CMS Benchmark Runner
# Compares Dragonfly CMS implementation vs Redis Stack (RedisBloom)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║       CMS Benchmark: Dragonfly vs Redis Stack              ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"

# Parse arguments
ITERATIONS=${1:-1000}
DRAGONFLY_IMAGE=${DRAGONFLY_IMAGE:-"docker.dragonflydb.io/dragonflydb/dragonfly:latest"}

echo -e "\n${YELLOW}Configuration:${NC}"
echo "  Iterations: $ITERATIONS"
echo "  Dragonfly Image: $DRAGONFLY_IMAGE"

# Check if docker compose is available
if command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
elif docker compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
else
    echo -e "${RED}Error: docker-compose or docker compose not found${NC}"
    exit 1
fi

echo -e "\n${YELLOW}Starting containers...${NC}"
$COMPOSE_CMD up -d

echo -e "\n${YELLOW}Waiting for services to be ready...${NC}"
sleep 3

# Get the host IP for the remote docker context
DOCKER_HOST_IP=$(docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null | sed 's|tcp://\([^:]*\):.*|\1|' || echo "localhost")

if [ "$DOCKER_HOST_IP" = "" ] || [ "$DOCKER_HOST_IP" = "localhost" ]; then
    # Try to get from docker info
    DOCKER_HOST_IP="localhost"
fi

echo "  Docker Host: $DOCKER_HOST_IP"

echo -e "\n${YELLOW}Running benchmark...${NC}"
python3 benchmark.py \
    --dragonfly-host "$DOCKER_HOST_IP" \
    --dragonfly-port 6380 \
    --redis-host "$DOCKER_HOST_IP" \
    --redis-port 6381 \
    --iterations "$ITERATIONS"

echo -e "\n${YELLOW}Cleaning up...${NC}"
$COMPOSE_CMD down

echo -e "\n${GREEN}Benchmark complete!${NC}"
