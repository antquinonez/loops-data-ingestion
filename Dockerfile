# Dockerfile for Loops Data Ingestion Demo
# Python 3.11+ required for nanobot-ai compatibility

FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Arguments for non-root user setup
# Allows running as host user to avoid permission issues on mounted volumes
ARG USER_ID=1000
ARG GROUP_ID=1000

# Install system dependencies for duckdb
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Set working directory first
WORKDIR /app

# Create non-root user with configurable UID/GID
# This ensures files created in mounted volumes are owned by the host user
RUN groupadd -g ${GROUP_ID} appgroup && \
    useradd -m -u ${USER_ID} -g ${GROUP_ID} -s /bin/bash appuser

# Install Python dependencies (as root for cache, then fix permissions)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    chown -R appuser:appgroup /app

# Copy project files as the non-root user
COPY --chown=appuser:appgroup . .

# Set PYTHONPATH to project root
ENV PYTHONPATH=/app

# Switch to non-root user
USER appuser

# Default command (can be overridden)
CMD ["python", "run_demo.py"]
