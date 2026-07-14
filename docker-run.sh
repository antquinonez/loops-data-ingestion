#!/bin/bash
# Convenience script to start and run the Loops demo in Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Get current user's UID and GID to avoid permission issues
# This ensures files created in mounted volumes are owned by the current user
# Use DOCKER_* vars to avoid conflict with shell readonly UID/GID
DOCKER_USER_ID=${UID:-$(id -u)}
DOCKER_GROUP_ID=${GID:-$(id -g)}

export DOCKER_USER_ID
export DOCKER_GROUP_ID

echo "========================================"
echo "  Starting Loops Demo in Docker"
echo "  Running as UID=$DOCKER_USER_ID, GID=$DOCKER_GROUP_ID"
echo "========================================"

# Check if .env file exists with OPENAI_API_KEY
if [ ! -f .env ]; then
    echo "ERROR: .env file not found in project root."
    echo "Please create a .env file with your OPENAI_API_KEY."
    echo "Example:"
    echo "  OPENAI_API_KEY=your-api-key-here"
    echo "  OPENAI_MODEL=gpt-4.1-mini-2025-04-14"
    exit 1
fi

if ! grep -q "OPENAI_API_KEY" .env; then
    echo "ERROR: .env file does not contain OPENAI_API_KEY."
    exit 1
fi

# Check if Docker is running
docker info > /dev/null 2>&1 || {
    echo "ERROR: Docker is not running. Please start Docker."
    exit 1
}

echo ""
echo "Building Docker image..."
docker-compose build

echo ""
echo "Starting container..."
docker-compose up -d

echo ""
echo "Container started. Running demo..."
echo ""
echo "Press Ctrl+C to stop the demo at any time."
echo "Use './docker-stop.sh' to stop the container after the demo completes."
echo ""

# Run the demo in the container and attach to its output
docker-compose exec loops python run_demo.py

echo ""
echo "========================================"
echo "  Demo complete!"
echo "  Container is still running (use docker-stop.sh to stop)"
echo "========================================"
