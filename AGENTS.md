# AI Agent Instructions for the Loops Data Ingestion Project

This document provides instructions, constraints, and best practices for AI agents (including Nanobot, Mistral Vibe, and other AI assistants) working with this codebase.

---

## 🔴 Critical Implementation Patterns

### Hybrid Prefect/sync Pipelines
⚠️ **ALL GENERATED PIPELINES MUST USE THIS PATTERN:**

- Use Prefect 3.7+ decorators (`@flow`, `@task`)
- **Always include sync fallback** for when no Prefect server is available
- Check server availability: `os.environ.get("PREFECT_API_KEY")` and `PREFECT_EPHEMERAL_START`
- Define dummy decorators when server is unavailable
- See template: `agents/pipeline_builder/flow_template_prefect_v3.txt`

### Pipeline-Aware Validation (Lazy Generation)
- ⚠️ **DO NOT pre-generate validation checks** during pipeline creation
- Use `validate_pipeline_output()` with pipeline metadata
- Pass `source_path` and `source_row_count` (don't query raw_ tables)
- Let the function generate checks on-demand from schema
- Cache generated checks to `pipelines/validation/{output_table}_validation_checks.json`

### Single Agent Architecture
- Use **one Nanobot instance** with sequential phases
- Phase 1: Investigation (uses `flows/nanobot_tools.py`)
- Phase 2: Pipeline Generation (uses `agents/pipeline_builder/nanobot_tools.py`)
- Maintains context continuity between phases

---

## Overview

This project is an **autonomous data ingestion troubleshooting system** that uses Nanobot AI agents to detect, investigate, and fix data quality issues in ETL pipelines. When working with this codebase, you are expected to:

1. **Understand the architecture** before making changes
2. **Follow the existing patterns** for tool registration and usage
3. **Respect the file structure** and naming conventions
4. **Use the provided tools** rather than implementing duplicate functionality
5. **Generate clean, maintainable code** that integrates with the existing system

---

## Project Architecture

### Agent Architecture: Single Instance, Multiple Roles

This project uses **a single Nanobot agent instance** that performs multiple roles sequentially:

```
┌─────────────────────────────────────────────────────────────┐
│                    run_demo.py (Orchestrator)                    │
└─────────────────────────────────────────────────────────────┘
                                    │
              ┌─────────────────────┬─────────────────────┐
              ▼                     ▼                     ▼
     ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
     │  flows/          │   │  agents/         │   │  schemas/        │
     │  - ingestion_    │   │  - pipeline_     │   │  - *.yaml        │
     │    flow.py       │   │    builder/      │   │                  │
     │  - nanobot_     │   │    - tools.py    │   │                  │
     │    tools.py      │   │    - nanobot_   │   │                  │
     │  - mcp_server.py │   │      tools.py   │   │                  │
     └─────────────────┘   └─────────────────┘   └─────────────────┘
              │                     │                     │
              ▼                     ▼                     ▼
     ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
     │  Phase 1:        │   │  Phase 2:       │   │  Defines         │
     │  Investigation   │   │  Pipeline        │   │  expected        │
     │  (Single Agent)  │   │  Generation      │   │  schemas         │
     │                 │   │  (Same Agent)    │   │                  │
     └─────────────────┘   └─────────────────┘   └─────────────────┘
```

**Key Insight**: Both investigation and pipeline generation are performed by the **same Nanobot instance** in sequential phases, maintaining context continuity.

### Entry Point

- **`run_demo.py`** - Main entry point that orchestrates the complete workflow
  - Step 1: Runs ingestion flow (fails intentionally)
  - Step 2: Tests investigation tools
  - Step 3: Triggers Nanobot investigation (Phase 1)
  - Step 4: Generates and executes cleaning pipelines (Phase 2)

---

## Agent Roles and Responsibilities

**IMPORTANT**: This project uses **a single Nanobot agent instance** that sequentially performs both investigation and pipeline generation. The roles below represent **phases** of the same agent, not separate agents.

### Phase 1: Investigation Role

**Purpose**: Diagnose data ingestion failures

**Tools Available**:
- `read_logs(path, tail_n)` - Read log files for error details
- `query_duckdb(query)` - Query DuckDB database
- `inspect_file(path, sample_size)` - Inspect CSV files
- `check_schema(path, schema)` - Validate data against schema
- `get_ingestion_status()` - Get pipeline status
- `send_slack_alert(message, severity)` - Send alerts

**Expected Workflow**:
1. Start with `logs/ingestion.log` to find the error
2. Use `inspect_file` on `data/source_data.csv`
3. Query `raw_users` table with `query_duckdb`
4. Use `check_schema` to validate against ideal schema
5. Identify root cause and recommend fixes
6. **Transition to Phase 2**: Register pipeline builder tools and proceed to generation

**Configuration**: `config/nanobot_config.yaml`

### Phase 2: Pipeline Builder Role

**Purpose**: Generate **hybrid Prefect/sync** data cleaning pipelines automatically

**Tools Available**:
- `load_ideal_schema()` - Load schema from YAML
- `infer_source_schema(file_path, sample_size)` - Infer schema from CSV
- `compare_schemas(source_path, ideal_path)` - Compare schemas
- `generate_cleaning_pipeline(source_path, ideal_path, output_table)` - Generate complete pipeline with Prefect decorators + sync fallback

**Expected Workflow**:
1. Load ideal schema with `load_ideal_schema()`
2. Infer source schema with `infer_source_schema()`
3. Compare with `compare_schemas()`
4. Generate **hybrid Prefect/sync** cleaning code with `generate_cleaning_pipeline()`
5. Save to `pipelines/generated/`
6. Validate output using pipeline-aware validation (lazy check generation)

**Configuration**: `agents/pipeline_builder/config.json`

**Key Difference**: The generated pipelines use **hybrid Prefect/sync architecture** - they include Prefect 3.x decorators but fall back to synchronous execution when no Prefect server is available.

---

## Environment Setup

### Required Environment Variables

```bash
# Mandatory
OPENAI_API_KEY="your-api-key-here"

# Optional
OPENAI_MODEL="gpt-4o-mini"  # Default model
PYTHONPATH=$(pwd)
```

### Virtual Environment

**Required Dependencies**: Prefect 3.7+ is required for hybrid pipeline generation.

```bash
# Activate the existing venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# If venv doesn't exist
python -m venv venv
source venv/bin/activate
pip install -q nanobot duckdb>=1.5.0 prefect>=3.7.0 mcp>=1.28.0 pandas python-dotenv
```

### Path Configuration

The project root is: `$(pwd)`

All Python files should have:
```python
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.environ["PYTHONPATH"] = str(PROJECT_ROOT)
```

---

## Working with Tools

### Tool Registration Pattern

When adding new tools to Nanobot, follow this pattern:

```python
from nanobot import Nanobot
from nanobot.agent.tools.registry import ToolRegistry

# Create bot
bot = Nanobot.from_config(
    config_path="config/nanobot_config_minimal.json",
    model="gpt-4o-mini"
)

# Register investigation tools
from flows.nanobot_tools import NANOBOT_TOOLS
for tool_name, tool_config in NANOBOT_TOOLS.items():
    bot.register_tool(
        name=tool_name,
        description=tool_config["description"],
        func=tool_config["function"]
    )

# Register pipeline builder tools (as Tool classes)
from agents.pipeline_builder.nanobot_tools import PIPELINE_TOOL_CLASSES
for tool_class in PIPELINE_TOOL_CLASSES:
    tool_instance = tool_class()
    bot._loop.tools.register(tool_instance)
```

### Tool Development Guidelines

When creating new tools:

1. **Investigation Tools** (simple functions):
   - Place in `flows/nanobot_tools.py`
   - Export in `NANOBOT_TOOLS` dictionary
   - Each tool should have: `description`, `function`, `parameters`

2. **Pipeline Builder Tools** (complex classes):
   - Place in `agents/pipeline_builder/tools.py`
   - Create corresponding Tool class in `agents/pipeline_builder/nanobot_tools.py`
   - Export in `PIPELINE_TOOL_CLASSES` list

### Tool Example (Function-based)

```python
# In flows/nanobot_tools.py

def inspect_file(path: str, sample_size: int = 10) -> dict:
    """Inspect a file and return metadata and sample data."""
    import csv
    from pathlib import Path
    
    file_path = Path(path)
    if not file_path.exists():
        return {"error": f"File not found: {path}"}
    
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)[:sample_size]
    
    return {
        "path": str(file_path),
        "size": file_path.stat().st_size,
        "rows": len(rows),
        "columns": list(rows[0].keys()) if rows else [],
        "sample": rows
    }

# Export in NANOBOT_TOOLS
NANOBOT_TOOLS = {
    "inspect_file": {
        "description": "Inspect source data files for metadata and sample data",
        "function": inspect_file,
        "parameters": {
            "path": {"type": "str", "required": True, "description": "Path to the file"},
            "sample_size": {"type": "int", "default": 10, "description": "Number of sample rows"}
        }
    },
    # ... other tools
}
```

### Tool Example (Class-based for Nanobot)

```python
# In agents/pipeline_builder/nanobot_tools.py

from nanobot.agent.tools import Tool
from typing import Optional
import yaml
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

class LoadIdealSchemaTool(Tool):
    """Tool to load ideal schema definition."""
    
    name = "load_ideal_schema"
    description = "Load the ideal schema definition from YAML file"
    
    def __init__(self):
        super().__init__()
    
    def __call__(self, schema_path: Optional[str] = None) -> dict:
        """
        Load ideal schema from YAML.
        
        Args:
            schema_path: Optional path to schema file. Defaults to schemas/users_schema.yaml
        
        Returns:
            Dictionary with schema definition or error
        """
        if schema_path is None:
            schema_path = str(PROJECT_ROOT / "schemas" / "users_schema.yaml")
        
        try:
            with open(schema_path, 'r') as f:
                schema = yaml.safe_load(f)
            return {"status": "success", "schema": schema}
        except Exception as e:
            return {"status": "error", "error": str(e)}

# Export in PIPELINE_TOOL_CLASSES
PIPELINE_TOOL_CLASSES = [
    LoadIdealSchemaTool,
    # ... other tool classes
]
```

---

## File Structure and Naming Conventions

### Directory Structure Rules

```
loops/
├── agents/                  # AI agent components
│   └── pipeline_builder/    # Pipeline generation agent
│       ├── tools.py         # Core pipeline generation logic
│       ├── nanobot_tools.py  # Nanobot-compatible tool classes
│       ├── config.json      # Agent configuration
│       └── skills.md        # Agent-specific skills
├── config/                  # Configuration files
│   ├── nanobot_config.yaml   # Full Nanobot configuration
│   ├── nanobot_config_minimal.json
│   └── nanobot_logging.yaml
├── data/                    # Data files
│   ├── *.csv               # Source data files
│   └── ingestion.db         # DuckDB database
├── flows/                   # Prefect flows and tools
│   ├── ingestion_flow.py    # Main ingestion flow
│   ├── nanobot_tools.py     # Investigation tools
│   └── mcp_server.py        # MCP server
├── pipelines/               # Generated pipelines
│   └── generated/           # Auto-generated hybrid Prefect/sync pipelines
├── schemas/                 # Schema definitions
│   └── *.yaml              # YAML schema files
├── skills/                  # Skill loading utilities
│   ├── __init__.py         # SkillLoader class for loading skills
│   └── utils.py            # Utility functions for agent context
├── logs/                    # Log files
└── *.py                     # Entry point scripts
```

### File Naming Conventions

| Type | Pattern | Example |
|------|---------|---------|
| Python modules | snake_case.py | `ingestion_flow.py` |
| Classes | PascalCase | `DataIngestionFlow` |
| Functions | snake_case | `load_ideal_schema()` |
| Variables | snake_case | `project_root` |
| Constants | UPPER_SNAKE_CASE | `PROJECT_ROOT` |
| YAML/JSON files | snake_case | `users_schema.yaml` |
| Configuration files | snake_case | `nanobot_config.yaml` |

### Path Handling

Always use absolute paths from `PROJECT_ROOT`:

```python
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Good
schema_path = PROJECT_ROOT / "schemas" / "users_schema.yaml"
data_path = PROJECT_ROOT / "data" / "source_data.csv"

# Bad (relative paths)
schema_path = "schemas/users_schema.yaml"
data_path = "../data/source_data.csv"
```

---

## Code Generation Guidelines

### When Generating Pipeline Code

1. **Use the existing templates** in `agents/pipeline_builder/flow_template_prefect_v3.txt`
2. **Follow the pattern** from existing generated pipelines
3. **Include proper error handling** with try/except blocks
4. **Add logging** for debugging using Prefect-compatible logger
5. **Use pandas with Prefect tasks** for data transformations
6. **Respect default values** from schema definitions
7. **Include hybrid Prefect/sync fallback** - always check for server availability
8. **Load data directly from CSV** - pipelines read from CSV files, not from raw_* tables
9. **Handle all three data types** - generate pipelines for users, orders, and transactions when applicable

### Pipeline Generation Checklist (Hybrid Prefect/sync)

- [ ] Use Prefect 3.7+ decorators (`@flow`, `@task`)
- [ ] Include sync fallback with dummy decorators
- [ ] Check `PREFECT_API_KEY` and `PREFECT_EPHEMERAL_START` environment variables
- [ ] Load source data correctly with pandas
- [ ] Handle NULL values with defaults from schema using `.fillna()`
- [ ] Convert types with `pd.to_numeric(..., errors='coerce').fillna(default).astype(type)`
- [ ] Respect constraints (min, max, enum) with validation
- [ ] Validate output data using pipeline-aware validation
- [ ] Include proper error handling and logging
- [ ] Follow existing code style from generated pipelines

### Example Generated Pipeline Structure

```python
"""
Auto-generated data cleaning pipeline.
Created by pipeline_builder agent.
"""

from prefect import flow, task
import duckdb
import pandas as pd
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = str(PROJECT_ROOT / "data" / "ingestion.db")

@task
def load_source_data():
    """Load source data from CSV file."""
    source_path = str(PROJECT_ROOT / "data" / "source_data.csv")
    return pd.read_csv(source_path)

@task
def clean_data(df):
    """Clean source data and return cleaned DataFrame."""
    # Apply transformations
    # Type conversions
    df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(0).astype(int)
    
    # NULL handling
    df['email'] = df['email'].fillna('unknown@example.com')
    
    # Date conversion
    df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce').fillna('1970-01-01')
    
    return df

@task
def save_to_duckdb(df):
    """Save cleaned data to DuckDB table."""
    conn = duckdb.connect(database=DB_PATH, read_only=False)
    try:
        conn.register('df', df)
        conn.execute("""
            CREATE OR REPLACE TABLE users_clean AS
            SELECT * FROM df
        """)
        logging.info("Cleaned data saved to users_clean table")
    finally:
        conn.close()

@flow
def clean_and_load_pipeline():
    """Main cleaning pipeline flow."""
    df = load_source_data()
    df = clean_data(df)
    save_to_duckdb(df)

if __name__ == "__main__":
    clean_and_load_pipeline()
```

---

## Integration Patterns

### Registering Tools with Nanobot

```python
# Pattern 1: Function-based tools from flows/nanobot_tools.py
from flows.nanobot_tools import NANOBOT_TOOLS

for tool_name, tool_config in NANOBOT_TOOLS.items():
    bot.register_tool(
        name=tool_name,
        description=tool_config["description"],
        func=tool_config["function"]
    )

# Pattern 2: Class-based tools from agents/pipeline_builder/
from agents.pipeline_builder.nanobot_tools import PIPELINE_TOOL_CLASSES

for tool_class in PIPELINE_TOOL_CLASSES:
    tool_instance = tool_class()
    bot._loop.tools.register(tool_instance)
```

### Using Environment Variables

```python
import os
from dotenv import load_dotenv

# Load from project .env
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# Verify required variables
if not os.environ.get("OPENAI_API_KEY"):
    load_dotenv()  # Try current directory
    
if not os.environ.get("OPENAI_API_KEY"):
    raise ValueError("OPENAI_API_KEY not found in environment")
```

### Running Subprocesses

```python
import subprocess
import sys

result = subprocess.run(
    [sys.executable, "flows/ingestion_flow.py"],
    capture_output=True,
    text=True,
    cwd=PROJECT_ROOT,
    env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
)

# Check result
if result.returncode != 0:
    print(f"Failed: {result.stderr}")
else:
    print(f"Success: {result.stdout}")
```

---

## Schema Handling

### Schema File Format

Schema files in `schemas/` use this format:

```yaml
tables:
  users:
    description: "User data table"
    columns:
      - name: id
        type: integer
        nullable: false
        description: "Unique identifier"
      - name: name
        type: string
        nullable: false
        description: "User name"
      - name: email
        type: string
        nullable: false
        default: "unknown@example.com"
        description: "Email address"
      - name: age
        type: integer
        nullable: false
        default: 0
        constraints:
          min: 0
          max: 150
      - name: join_date
        type: date
        nullable: false
        default: "1970-01-01"
      - name: status
        type: string
        nullable: false
        default: "inactive"
        enum: ["active", "inactive", "pending"]
      - name: score
        type: float
        nullable: false
        default: 0.0
```

### Type Mappings

| Schema Type | DuckDB Type | Python Type | Pandas Type |
|-------------|-------------|-------------|-------------|
| integer | INTEGER | int | Int64 |
| float | FLOAT | float | float |
| string | VARCHAR | str | string |
| date | DATE | datetime.date | datetime64 |
| boolean | BOOLEAN | bool | bool |

---

## Testing Guidelines

### Test Structure

Tests should follow this pattern:

```python
# test_*.py files
def test_tool_functionality():
    """Test that a tool works correctly."""
    from flows.nanobot_tools import inspect_file
    
    result = inspect_file("data/source_data.csv", sample_size=5)
    
    assert "path" in result
    assert "rows" in result
    assert "columns" in result
    assert len(result["rows"]) <= 5
```

### Test Execution

**Total: 106 tests across all test files**

```bash
# Run specific test
python -m pytest tests/test_pipeline_tools_with_nanobot.py -v

# Run all tests (106 total)
python -m pytest -v

# Run specific test suites
python -m pytest tests/test_pipeline_builder.py -v    # Pipeline builder tools (27 tests)
python -m pytest tests/test_limits.py -v              # Pipeline attempt tracking (32 tests)
python -m pytest tests/test_mcp_server.py -v           # MCP server functionality (20 tests)
python -m pytest tests/test_validation.py -v            # Validation agent (24 tests)
python -m pytest tests/test_pipeline_tools_with_nanobot.py -v  # Nanobot integration (3 tests)

# Run with coverage
python -m pytest --cov=flows --cov=agents -v

# Run in Docker (after ./scripts/docker/docker-run.sh)
docker-compose exec loops python -m pytest -v
```

---

## Debugging Tips

### Common Issues and Solutions

| Issue | Debug Steps | Solution |
|-------|-------------|----------|
| Tool not found | Check `bot._loop.tools.get_definitions()` | Register tool properly |
| Module not found | Check `sys.path` and `PYTHONPATH` | Add project root to path |
| Database errors | Check DuckDB connection | Verify DB path exists |
| API key errors | Check `os.environ.get("OPENAI_API_KEY")` | Set in .env or export |
| Type conversion errors | Check source data types | Use COALESCE with CAST |

### Debug Commands

```python
# List all registered tools
print("Registered tools:", [t.name for t in bot._loop.tools.get_definitions()])

# Check environment
import os
print("PYTHONPATH:", os.environ.get("PYTHONPATH"))
print("OPENAI_API_KEY set:", bool(os.environ.get("OPENAI_API_KEY")))

# Test DuckDB connection
import duckdb
conn = duckdb.connect(database="data/ingestion.db")
print("Tables:", conn.execute("SHOW TABLES").fetchall())
conn.close()

# Test tool directly
from flows.nanobot_tools import inspect_file
result = inspect_file("data/source_data.csv", sample_size=3)
print(result)
```

### Docker-Specific Debug Commands

```bash
# Check if container is running
docker-compose ps

# View container logs
docker-compose logs -f

# Execute Python in running container
docker-compose exec loops python3 -c "import duckdb; print(duckdb.__version__)"

# Check mounted volumes (from host)
ls -la data/ingestion.db
ls -la logs/
ls -la pipelines/generated/

# Open shell in container for interactive debugging
docker-compose exec loops bash

# Check UID/GID being used
echo "Container UID: $(docker-compose exec loops id -u)"
echo "Container GID: $(docker-compose exec loops id -g)"
```

---

## Enhanced Error Handling

### PipelineAttemptTracker (utils/limits.py)

The system now includes circuit breaker pattern for pipeline execution:

```python
from utils.limits import PipelineAttemptTracker

tracker = PipelineAttemptTracker()

if tracker.should_attempt(pipeline_name):
    try:
        # execute pipeline
        result = run_pipeline()
        tracker.record_success(pipeline_name)
        return result
    except Exception as e:
        tracker.record_failure(pipeline_name, str(e))
        raise
else:
    raise CircuitBreakerError(f"Too many failures for {pipeline_name}")
```

**Features**:
- Tracks attempts per pipeline
- Implements exponential backoff
- Prevents infinite loops
- Detects repeated errors
- Circuit breaker pattern for stability

---

## Constraints and Limitations

### Do NOT

- [ ] Modify existing tool signatures without updating all callers
- [ ] Change file paths without updating `PROJECT_ROOT` references
- [ ] Remove existing functionality without replacement
- [ ] Use relative paths in generated code
- [ ] Hardcode API keys in files (use environment variables)
- [ ] Create files outside the project structure
- [ ] Modify generated files in `pipelines/generated/` manually (they are auto-generated)
- [ ] Generate pipelines without hybrid Prefect/sync fallback
- [ ] Pre-generate validation checks (use lazy generation instead)
- [ ] Query non-existent `raw_*` tables for source data (use pipeline metadata)

### DO

- [ ] Follow existing code patterns and style
- [ ] Use absolute paths from `PROJECT_ROOT`
- [ ] Add proper error handling with try/except blocks
- [ ] Include logging for debugging using Prefect-compatible logger
- [ ] Test new functionality before committing
- [ ] Update documentation when adding new features
- [ ] Respect the existing architecture
- [ ] Use hybrid Prefect/sync patterns for all generated pipelines
- [ ] Use pipeline-aware validation with lazy check generation
- [ ] Register both investigation and pipeline builder tools to the same agent
- [ ] Maintain context continuity between Phase 1 and Phase 2

---

## Workflow for New Features

### Adding a New Tool

1. **Decide tool type**: Function-based (simple) or Class-based (complex)
2. **Implement the tool**:
   - Function-based: Add to `flows/nanobot_tools.py`
   - Class-based: Add to `agents/pipeline_builder/nanobot_tools.py`
3. **Export the tool**: Add to appropriate dictionary/list
4. **Register the tool**: Update `run_demo.py` or create registration code
5. **Test the tool**: Create a test in `test_*.py`
6. **Document the tool**: Update this file and README.md

**Note**: If the tool is for validation, ensure it uses **lazy validation check generation** pattern.

### Adding a New Data Type

1. **Create schema**: Add `schemas/{type}_schema.yaml`
2. **Add source data**: Create `data/{type}.csv`
3. **Update pipeline builder**: Extend tools to handle new schema
4. **Test**: Verify pipeline generation works for new type

### Modifying Existing Code

1. **Read the file first**: Understand current implementation
2. **Identify dependencies**: Check where the code is called from
3. **Make minimal changes**: Only change what's necessary
4. **Test**: Verify existing functionality still works
5. **Document**: Update any affected documentation

---

## Example Agent Session

Here's how a typical agent session should work with the **single agent, multiple phases** approach:

```
USER: "Investigate the data ingestion failure and fix it."

AGENT (Single Nanobot Instance):
Phase 1 - Investigation:
1. Reading logs/ingestion.log to understand the error...
2. Inspecting data/source_data.csv...
3. Querying raw_users table...
4. Comparing source schema with ideal schema...
5. Found 3 issues: NULL email, invalid age, malformed email

Phase 2 - Pipeline Generation:
6. Loading ideal schema from schemas/users_schema.yaml...
7. Inferring source schema from data/source_data.csv...
8. Comparing schemas to identify mismatches...
9. Generating hybrid Prefect/sync cleaning pipeline...
10. Saving to pipelines/generated/clean_users_pipeline.py

Phase 3 - Validation:
11. Executing generated hybrid pipeline (Prefect decorators + sync fallback)...
12. Using pipeline-aware validation with lazy check generation...
13. Verification: Pipeline succeeded, users_clean table created

Result: Ingestion failure fixed by single agent performing investigation → generation → validation with context continuity.
```

**Key Point**: All phases are performed by the **same Nanobot instance**, maintaining context throughout the workflow.

---

## Performance Considerations

- **Token limits**: Be mindful of LLM token usage in conversations
- **Database queries**: Use LIMIT clauses to avoid large result sets
- **File reads**: Read only necessary portions of files
- **Subprocess timeout**: Always set timeout for subprocess calls
- **Caching**: Consider caching schema loads and file inspections

---

## Security Considerations

- **API keys**: Never log or expose `OPENAI_API_KEY`
- **Data privacy**: Be careful with sensitive data in source files
- **File permissions**: Respect file permissions when reading/writing
- **Database access**: Use read-only connections where possible
- **Tool validation**: Validate tool inputs to prevent injection attacks

---

## Useful Commands

```bash
# Run the complete demo
python run_demo.py

# Run just the pipeline builder
python scripts/demo_pipeline_builder.py

# Run the failing ingestion
python flows/ingestion_flow.py

# Start Nanobot server
python -m nanobot.server --config config/nanobot_config.yaml --log-level DEBUG

# Start MCP server
python flows/mcp_server.py --host 127.0.0.1 --port 8081

# Check generated pipelines
ls -la pipelines/generated/

# View logs
tail -f logs/ingestion.log

# Query the database
python -c "import duckdb; conn = duckdb.connect('data/ingestion.db'); print(conn.execute('SHOW TABLES').fetchall()); conn.close()"

# Test a specific tool
python -c "from flows.nanobot_tools import inspect_file; import pprint; pprint.pprint(inspect_file('data/source_data.csv', 3))"
```

### Docker Commands

```bash
# Full cleanup and restart
./scripts/docker/docker-clean.sh && ./scripts/docker/docker-run.sh

# Just stop the container
./scripts/docker/docker-stop.sh

# Rebuild image after code changes
docker-compose build

# Start container manually
docker-compose up -d

# Execute command in running container
docker-compose exec loops python run_demo.py
```

---

## Docker-Specific Guidelines

When working with the Docker containerized version of this project:

### Docker Architecture
- The container uses `tail -f /dev/null` as CMD to stay running
- `scripts/docker/docker-run.sh` starts the container and executes `run_demo.py` via `docker-compose exec`
- This ensures only **one instance** of `run_demo.py` runs (no duplicate execution)
- All generated files (data/, logs/, pipelines/) are **volume-mounted** and persist outside the container

### Working with Docker as an Agent

**To test your changes in Docker:**
```bash
# Rebuild the image (required after code changes)
docker-compose build

# Run cleanup and start fresh
./scripts/docker/docker-clean.sh

# Start the demo
./scripts/docker/docker-run.sh

# The container stays running - you can exec into it
docker-compose exec loops bash
```

**Important Docker considerations:**
- Files you create in mounted volumes (`data/`, `logs/`, `pipelines/`, `memory/`) will be owned by your host user (no sudo needed)
- The `.env` file is **NOT baked into the image** - it's loaded at runtime by docker-compose
- To test database queries: `docker-compose exec loops python3 -c "import duckdb; conn = duckdb.connect('data/ingestion.db'); print(conn.execute('SHOW TABLES').fetchall())"`
- Database files persist in `data/ingestion.db` on the host

### Common Docker Issues for Agents

| Issue | Cause | Solution |
|-------|-------|----------|
| Files owned by root | Container not using UID/GID | Use `DOCKER_USER_ID` and `DOCKER_GROUP_ID` env vars |
| Database not found | First run not completed | Run `./scripts/docker/docker-run.sh` first |
| Generated pipelines missing | Cleanup ran at start | Check `pipelines/generated/` after demo completes |
| Container exits immediately | CMD issue | Container should run `tail -f /dev/null` |
| Duplicate execution | Both CMD and exec running | Fixed: CMD is `tail -f /dev/null`, demo runs via exec |

---

## Data Files

The project includes multiple data sources that the pipeline builder processes:

- **`data/source_data.csv`** - Primary users data (13 rows with intentional errors)
- **`data/orders.csv`** - Orders data (13 rows) - processed by pipeline builder
- **`data/transactions.csv`** - Transactions data (13 rows) - processed by pipeline builder

**Expected output tables after successful run:**
- `raw_users` - Raw staging data from source_data.csv
- `users` - Strict target table (0 rows - transform intentionally fails)
- `users_clean` - Cleaned user data (13 rows)
- `orders_clean` - Cleaned orders data (13 rows)
- `transactions_clean` - Cleaned transactions data (13 rows)

---

## Summary

When working with this codebase:

1. **Understand the architecture** - Read this file and README.md first
2. **Follow existing patterns** - Match the code style and structure
3. **Use the tools** - Leverage existing tools rather than reimplementing
4. **Test your changes** - Verify new functionality works (including in Docker)
5. **Document** - Update documentation for new features
6. **Respect constraints** - Follow the do's and don'ts above
7. **Use single agent pattern** - One Nanobot instance with sequential phases (Investigation → Pipeline Generation → Validation)

The system is designed to be **autonomous** - your goal as an agent is to help it work better, not to replace its functionality.

**Remember**: All generated pipelines must use the **hybrid Prefect/sync pattern** with Prefect 3.7+ decorators and graceful fallback to synchronous execution.

**Docker Note**: When working in the Docker environment, remember that the demo cleans up at the start of every `run_demo.py` execution. Use `./scripts/docker/docker-clean.sh` for full cleanup, or exec into the running container to inspect state.

---

## Version Information

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.11+ | Runtime (required by nanobot-ai and pandas 3.x) |
| Nanobot | Latest | AI Agent Framework |
| DuckDB | >=1.5.0 | Database |
| Prefect | >=3.7.0 | Workflow Orchestration (required for hybrid sync fallback) |
| MCP | >=1.28.0 | Model Context Protocol |

---

*For questions about this codebase, first check README.md, SKILLS.md, and this file (AGENTS.md). If you still have questions, ask for clarification from the user.*
