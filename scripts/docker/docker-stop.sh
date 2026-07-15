#!/bin/bash
# Convenience script to stop the Loops demo Docker container

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Get current user's UID and GID (same as docker-run.sh)
# Use DOCKER_* vars to avoid conflict with shell readonly UID/GID
DOCKER_USER_ID=${UID:-$(id -u)}
DOCKER_GROUP_ID=${GID:-$(id -g)}
export DOCKER_USER_ID
export DOCKER_GROUP_ID

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
