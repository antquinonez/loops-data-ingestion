# AI Agent Instructions for the Loops Data Ingestion Project

This document provides instructions, constraints, and best practices for AI agents (including Nanobot, Mistral Vibe, and other AI assistants) working with this codebase.

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

### Entry Point

- **`run_demo.py`** - Main entry point that orchestrates the complete workflow
  - Step 1: Runs ingestion flow (fails intentionally)
  - Step 2: Tests investigation tools
  - Step 3: Triggers Nanobot investigation
  - Step 4: Generates and executes cleaning pipelines

### Core Components

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
     │  Investigates    │   │  Generates       │   │  Defines         │
     │  failures        │   │  cleaning code   │   │  expected        │
     │                 │   │                 │   │  schemas         │
     └─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Agent Roles and Responsibilities

### 1. Investigation Agent

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

**Configuration**: `config/nanobot_config.yaml`

### 2. Pipeline Builder Agent

**Purpose**: Generate data cleaning pipelines automatically

**Tools Available**:
- `load_ideal_schema()` - Load schema from YAML
- `infer_source_schema(file_path, sample_size)` - Infer schema from CSV
- `compare_schemas(source_path, ideal_path)` - Compare schemas
- `generate_cleaning_pipeline(source_path, ideal_path, output_table)` - Generate complete pipeline

**Expected Workflow**:
1. Load ideal schema with `load_ideal_schema()`
2. Infer source schema with `infer_source_schema()`
3. Compare with `compare_schemas()`
4. Generate cleaning code with `generate_cleaning_pipeline()`
5. Save to `pipelines/generated/`

**Configuration**: `agents/pipeline_builder/config.json`

---

## Environment Setup

### Required Environment Variables

```bash
# Mandatory
OPENAI_API_KEY="your-api-key-here"

# Optional
OPENAI_MODEL="gpt-4o-mini"  # Default model
PYTHONPATH=/home/aq/Documents/Source/loops
```

### Virtual Environment

```bash
# Activate the existing venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# If venv doesn't exist
python -m venv venv
source venv/bin/activate
pip install -q nanobot duckdb prefect python-dateutil mcp python-dotenv
```

### Path Configuration

The project root is: `/home/aq/Documents/Source/loops`

All Python files should have:
```python
PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
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

PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")

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
│   └── generated/           # Auto-generated by agents
├── schemas/                 # Schema definitions
│   └── *.yaml              # YAML schema files
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

PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")

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

1. **Use the existing templates** in `agents/pipeline_builder/flow_template.txt`
2. **Follow the pattern** from existing generated pipelines
3. **Include proper error handling**
4. **Add logging** for debugging
5. **Use COALESCE and CAST** for type conversions in SQL, or pandas with Prefect tasks
6. **Respect default values** from schema definitions

### Pipeline Generation Checklist

- [ ] Load source data correctly
- [ ] Handle NULL values with defaults from schema
- [ ] Convert types with CAST and COALESCE
- [ ] Respect constraints (min, max, enum)
- [ ] Validate output data
- [ ] Include proper error handling
- [ ] Add logging statements
- [ ] Follow existing code style

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

PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
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
PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
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

```bash
# Run specific test
python -m pytest test_pipeline_tools_with_nanobot.py -v

# Run all tests
python -m pytest -v

# Run with coverage
python -m pytest --cov=flows --cov=agents -v
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

### DO

- [ ] Follow existing code patterns and style
- [ ] Use absolute paths from `PROJECT_ROOT`
- [ ] Add proper error handling
- [ ] Include logging for debugging
- [ ] Test new functionality before committing
- [ ] Update documentation when adding new features
- [ ] Respect the existing architecture

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

Here's how a typical agent session should work:

```
USER: "Investigate the data ingestion failure and fix it."

AGENT:
1. Reading logs/ingestion.log to understand the error...
2. Inspecting data/source_data.csv...
3. Querying raw_users table...
4. Comparing source schema with ideal schema...
5. Found 3 issues: NULL email, invalid age, malformed email
6. Generating cleaning pipeline...
7. Saving to pipelines/generated/clean_users_pipeline.py
8. Executing generated pipeline...
9. Verification: Pipeline succeeded, users_clean table created

Result: Ingestion failure fixed by generating and executing cleaning pipeline.
```

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
python demo_pipeline_builder.py

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

---

## Summary

When working with this codebase:

1. **Understand the architecture** - Read this file and README.md first
2. **Follow existing patterns** - Match the code style and structure
3. **Use the tools** - Leverage existing tools rather than reimplementing
4. **Test your changes** - Verify new functionality works
5. **Document** - Update documentation for new features
6. **Respect constraints** - Follow the do's and don'ts above

The system is designed to be **autonomous** - your goal as an agent is to help it work better, not to replace its functionality.

---

## Version Information

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.10+ | Runtime |
| Nanobot | Latest | AI Agent Framework |
| DuckDB | Latest | Database |
| Prefect | Latest | Workflow Orchestration |

---

*For questions about this codebase, first check README.md, SKILLS.md, and this file (AGENTS.md). If you still have questions, ask for clarification from the user.*
