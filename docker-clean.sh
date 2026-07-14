#!/bin/bash
# Cleanup script to remove all generated artifacts and start fresh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "  Cleaning Loops Project"
echo "========================================"

# Get current user's UID and GID
DOCKER_USER_ID=${UID:-$(id -u)}
DOCKER_GROUP_ID=${GID:-$(id -g)}
export DOCKER_USER_ID
export DOCKER_GROUP_ID

# Stop any running container
if docker-compose ps | grep -q "loops-demo"; then
    echo ""
    echo "Stopping running container..."
    docker-compose down
fi

echo ""
echo "Removing generated files..."

# Remove database files
rm -f data/ingestion.db data/*.wal data/*.shm 2>/dev/null || true

# Remove log files
rm -f logs/*.log logs/*.md 2>/dev/null || true
rm -rf logs/archive 2>/dev/null || true

# Remove generated pipelines
rm -f pipelines/generated/*.py 2>/dev/null || true

# Remove memory files
rm -rf memory/* 2>/dev/null || true
rm -rf memory/sessions 2>/dev/null || true

# Remove pipeline attempt tracking
rm -f logs/pipeline_attempts.jsonl 2>/dev/null || true

echo ""
echo "✓ Cleanup complete!"
echo ""
echo "All generated files removed. Next run will start fresh."
echo "Use './docker-run.sh' to start the demo."
