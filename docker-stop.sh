#!/bin/bash
# Convenience script to stop the Loops demo Docker container

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

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
