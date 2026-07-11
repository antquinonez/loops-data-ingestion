#!/bin/bash
# Wrapper script to run duckdb CLI with venv PATH

# Add venv/bin to PATH
VENV_BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/venv/bin"
export PATH="$VENV_BIN:$PATH"

# Run duckdb CLI with all arguments
exec duckdb "$@"
