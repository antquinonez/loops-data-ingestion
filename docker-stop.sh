#!/bin/bash
# Convenience script to stop the Loops demo Docker container

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Get current user's UID and GID (same as docker-run.sh)
UID=${UID:-$(id -u)}
GID=${GID:-$(id -g)}
export UID
export GID

echo "========================================"
echo "  Stopping Loops Demo Docker Container"
echo "========================================"

# Check if container is running
if docker-compose ps | grep -q "loops-demo"; then
    echo ""
    echo "Stopping container..."
    docker-compose down
    echo ""
    echo "Container stopped successfully."
else
    echo ""
    echo "No running container found."
fi

echo ""
echo "========================================"
