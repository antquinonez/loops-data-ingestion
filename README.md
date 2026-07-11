# Data Ingestion Troubleshooting with Nanobot

An **autonomous AI agent system** for diagnosing and automatically fixing data ingestion failures. This proof-of-concept demonstrates a complete workflow where an AI agent (Nanobot) detects, investigates, and resolves data quality issues in a Prefect-based ETL pipeline.

## Overview

This project showcases:

1. **Intentional Data Errors**: A Prefect ingestion flow that fails due to data quality issues (NULL values, type mismatches, malformed data)
2. **Autonomous Investigation**: A Nanobot agent that automatically troubleshoots failures using custom tools
3. **Automatic Pipeline Generation**: AI-powered pipeline builder that generates cleaning code to fix data issues
4. **Schema Validation**: Automatic comparison of source data against ideal schemas
5. **Self-Healing**: The system can generate and execute cleaning pipelines to resolve ingestion failures

### Key Technologies

- **Nanobot**: Autonomous AI agent framework with tool integration
- **DuckDB**: Embedded analytical database for data processing
- **Prefect**: Workflow orchestration (intentionally configured to fail for demo purposes)
- **OpenAI API**: LLM-powered investigation and code generation
- **MCP Server**: Model Context Protocol for enhanced agent capabilities (optional)

---

## Project Structure

```
loops/
├── agents/
│   └── pipeline_builder/
│       ├── tools.py           # Schema analysis & pipeline generation tools
│       ├── nanobot_tools.py   # Nanobot-compatible tool classes
│       ├── config.json        # Pipeline builder configuration
│       └── skills.md          # Pipeline builder specific skills
├── config/
│   ├── nanobot_config.yaml    # Full Nanobot server configuration
│   ├── nanobot_config_minimal.json  # Minimal config for demo
│   └── nanobot_logging.yaml   # Logging configuration
├── data/
│   ├── source_data.csv       # Source CSV with intentional errors
│   ├── orders.csv            # Orders data (optional)
│   ├── transactions.csv      # Transactions data (optional)
│   └── ingestion.db          # DuckDB database (created on first run)
├── flows/
│   ├── ingestion_flow.py     # Prefect flow that fails on bad data
│   ├── nanobot_tools.py      # Investigation tools for Nanobot
│   └── mcp_server.py         # MCP server for enhanced capabilities
├── pipelines/
│   └── generated/            # Auto-generated cleaning pipelines
│       ├── clean_users_pipeline.py
│       ├── clean_transactions_pipeline.py
│       └── clean_orders_pipeline.py
├── schemas/
│   ├── ideal_schema.yaml     # Ideal schema for users data
│   ├── orders_schema.yaml    # Schema for orders data
│   └── transactions_schema.yaml  # Schema for transactions data
├── logs/
│   ├── ingestion.log         # Ingestion pipeline logs
│   ├── prefect.log           # Prefect flow logs
│   └── nanobot.log           # Nanobot investigation logs
├── agents/
│   └── pipeline_builder/
│       ├── tools.py          # Pipeline generation logic
│       ├── nanobot_tools.py  # Nanobot tool registrations
│       └── skills.md         # Pipeline builder skills
├── run_demo.py               # Main demo entry point
├── demo_pipeline_builder.py # Standalone pipeline builder demo
├── SKILLS.md                # General troubleshooting guide for agents
├── AGENTS.md               # Instructions for AI agents working with this repo
├── README.md                # This file
└── .env                    # Environment variables (OPENAI_API_KEY)
```

---

## Intentional Errors in Source Data

The `data/source_data.csv` file contains several intentional data quality issues that cause the ingestion to fail:

| Row | Column | Issue | Error Type |
|-----|--------|-------|------------|
| 6 | email | Empty/NULL value | NOT NULL constraint violation |
| 7 | age | "N/A" | Type conversion error (STRING to INTEGER) |
| 11 | email | "karen@example" | Invalid format (missing TLD) |

These issues cause:
- `ConversionException: Could not convert string 'N/A' to INT32`
- `NOT NULL constraint failed: users.email`

---

## Quick Start

### Prerequisites

- Python 3.10+
- Virtual environment (recommended)
- OpenAI API key (required for Nanobot LLM access)

### Setup

```bash
# Clone or navigate to the project
cd /home/aq/Documents/Source/loops

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# OR: venv\Scripts\activate  # Windows

# Install dependencies
pip install -q nanobot duckdb prefect python-dateutil mcp python-dotenv

# Set your OpenAI API key (required)
export OPENAI_API_KEY="your-api-key-here"

# Optional: Set model (default: gpt-4o-mini)
export OPENAI_MODEL="gpt-4o-mini"
```

### Run the Complete Demo

```bash
# Run the full demo - this will:
# 1. Run the failing ingestion flow
# 2. Test investigation tools
# 3. Trigger Nanobot to analyze and fix the issues
# 4. Generate cleaning pipelines
# 5. Execute the generated pipelines to verify the fix
python run_demo.py
```

The demo will:
1. First run fails (expected) due to data errors
2. Nanobot investigates using the tools in `flows/nanobot_tools.py`
3. Pipeline builder generates cleaning code in `pipelines/generated/`
4. Generated pipelines are executed and succeed

### Run Individual Components

```bash
# Step 1: Run the failing ingestion (creates errors)
python flows/ingestion_flow.py

# Step 2: Run the pipeline builder to generate fixes
python demo_pipeline_builder.py

# Step 3: Run the generated cleaning pipeline
python pipelines/generated/clean_users_pipeline.py

# Step 4: Start Nanobot server for manual investigation
python -m nanobot.server --config config/nanobot_config.yaml --log-level DEBUG

# Step 5: Start MCP server (optional)
python flows/mcp_server.py --host 127.0.0.1 --port 8081
```

---

## Architecture

### Workflow Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      run_demo.py (Entry Point)                  │
└─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────┐
│                 flows/ingestion_flow.py                         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐  │
│  │ validate     │───▶│ create       │───▶│ transform_and_load   │  │
│  │ source_file  │    │ target_table │    │ (INTENTIONALLY FAILS)│  │
│  └─────────────┘    └─────────────┘    └─────────────────────┘  │
│                              │                                  │
│                              ▼                                  ▼
│                    raw_users table         users table (fails)
└─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────┐
│              Investigation Phase (run_demo.py)                  │
│  ┌─────────────────┐    ┌─────────────────────────────────┐  │
│  │ Test Tools:      │    │ Trigger Nanobot Investigation     │  │
│  │ - inspect_file   │    │ - Uses tools from nanobot_tools  │  │
│  │ - check_schema   │    │ - Analyzes logs, DB, source data  │  │
│  │ - get_ingestion  │    │ - Generates cleaning pipeline    │  │
│  │   _status        │    │                                   │  │
│  └─────────────────┘    └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────┐
│           Pipeline Builder (agents/pipeline_builder/)          │
│  ┌─────────────────┐    ┌─────────────────────────────────┐  │
│  │ Tools:           │    │ generate_cleaning_pipeline()      │  │
│  │ - load_ideal_    │───▶│ - Compares source vs ideal       │  │
│  │   schema        │    │ - Generates SQL + Python code    │  │
│  │ - infer_source_  │    │ - Handles type casting, NULLs   │  │
│  │   schema        │    │ - Saves to pipelines/generated  │  │
│  │ - compare_      │    │                                   │  │
│  │   schemas       │    │                                   │  │
│  └─────────────────┘    └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────┐
│           Generated Cleaning Pipeline                           │
│  pipelines/generated/clean_users_pipeline.py                   │
│  - Automatically loads source data                           │
│  - Applies type conversions with COALESCE fallback             │
│  - Fills NULL values with defaults                           │
│  - Inserts cleaned data into target table                     │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Source CSV → [Prefect Flow] → raw_users (staging) → [Transformation] → users (target)
                         ↓
                  [FAILS due to data errors]
                         ↓
                  [Nanobot investigates]
                         ↓
                  [Pipeline Builder generates fix]
                         ↓
                  Generated Pipeline → users_clean (fixed)
```

---

## Using Nanobot Programmatically

### Basic Investigation

```python
from nanobot import Nanobot
import os

# Set API key
os.environ["OPENAI_API_KEY"] = "your-api-key"

# Create bot with config
bot = Nanobot.from_config(
    config_path="config/nanobot_config_minimal.json",
    model="gpt-4o-mini"
)

# Register custom tools
from flows.nanobot_tools import NANOBOT_TOOLS
for tool_name, tool_config in NANOBOT_TOOLS.items():
    bot.register_tool(
        name=tool_name,
        description=tool_config["description"],
        func=tool_config["function"]
    )

# Trigger investigation
result = bot.run("Investigate the failed data ingestion and identify all issues.")
print(result)
```

### With Pipeline Builder Tools

```python
from nanobot import Nanobot
from agents.pipeline_builder.nanobot_tools import PIPELINE_TOOL_CLASSES
import os

os.environ["OPENAI_API_KEY"] = "your-api-key"

# Create bot
bot = Nanobot.from_config(
    config_path="config/nanobot_config_minimal.json",
    model="gpt-4o-mini"
)

# Register pipeline builder tools
for tool_class in PIPELINE_TOOL_CLASSES:
    tool_instance = tool_class()
    bot.register_tool(tool_instance)

# Trigger pipeline generation
result = bot.run("""
    Investigate the failed data ingestion. 
    Use: infer_source_schema, load_ideal_schema, compare_schemas, 
    generate_cleaning_pipeline. 
    Save pipeline to pipelines/generated/clean_users_pipeline.py using write_file.
""")
```

---

## Available Tools

### Investigation Tools (`flows/nanobot_tools.py`)

| Tool | Description | Parameters |
|------|-------------|------------|
| `read_logs` | Read application logs to find error details | `path`, `tail_n=100` |
| `query_duckdb` | Execute SQL queries against DuckDB | `query` |
| `inspect_file` | Inspect source data files for metadata and samples | `path`, `sample_size=10` |
| `check_schema` | Validate data against expected schema | `path`, `schema` |
| `send_slack_alert` | Send investigation results to Slack (mock) | `message`, `severity` |
| `get_ingestion_status` | Get current status of ingestion pipeline | None |

### Pipeline Builder Tools (`agents/pipeline_builder/tools.py`)

| Tool | Description | Parameters |
|------|-------------|------------|
| `load_ideal_schema` | Load ideal schema definition from YAML | None |
| `infer_source_schema` | Infer schema from source CSV file | `file_path`, `sample_size=100` |
| `compare_schemas` | Compare source and ideal schemas | `source_path`, `ideal_path` |
| `generate_cleaning_pipeline` | Generate complete cleaning pipeline | `source_path`, `ideal_path`, `output_table` |

---

## Schema Files

### `schemas/ideal_schema.yaml`

Defines the expected schema for the users table with type constraints, nullable flags, and default values:

```yaml
tables:
  users:
    description: "Clean user data table"
    columns:
      - name: id
        type: integer
        nullable: false
        description: "User ID"
      - name: name
        type: string
        nullable: false
        description: "User name"
      - name: email
        type: string
        nullable: false
        default: "unknown@example.com"
        description: "User email"
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

---

## MCP Server (Optional)

The MCP (Model Context Protocol) server provides additional resources and tools for enhanced agent capabilities.

### Running the MCP Server

```bash
python flows/mcp_server.py --host 127.0.0.1 --port 8081
```

### Exposed Resources

- `logs/ingestion.log` - Main ingestion pipeline log
- `data/source_data.csv` - Source CSV with intentional errors
- `data/ingestion.db` - DuckDB database
- `schemas/ideal_schema.yaml` - Ideal schema definition
- `SKILLS.md` - Troubleshooting guide

### Additional MCP Tools

- `query_database` - Execute SQL queries
- `get_data_quality_report` - Generate data quality reports
- `get_recent_errors` - Extract recent errors from logs
- `get_file_metadata` - Get file metadata and statistics

---

## Expected Investigation Flow

When the agent is triggered, it follows this protocol:

1. **Check Logs**: Use `read_logs` to get error messages from `logs/ingestion.log`
2. **Inspect Source**: Use `inspect_file` to examine `data/source_data.csv`
3. **Query Database**: Use `query_duckdb` to check `raw_users` table state
4. **Validate Schema**: Use `check_schema` to identify data quality issues
5. **Compare Schemas**: Use `compare_schemas` to find type/nullability mismatches
6. **Generate Pipeline**: Use `generate_cleaning_pipeline` to create fix
7. **Analyze Findings**: Identify root causes and impact
8. **Send Alert**: Use `send_slack_alert` to notify team

### Expected Findings

The agent should identify:

1. **Primary Failure**: `ConversionException: Could not convert string 'N/A' to INT32` for the `age` column
2. **Additional Issues**:
   - Row 6: Empty email value (NULL constraint violation)
   - Row 7: Age is 'N/A' instead of a number (type mismatch)
   - Row 11: Malformed email ('karen@example' missing TLD)
3. **Root Cause**: Source CSV contains data that doesn't match target schema constraints
4. **Recommended Actions**:
   - Use COALESCE with CAST to handle type conversions
   - Fill NULL values with defaults
   - Add data validation before transformation

---

## Configuration

### Nanobot Configuration

Edit `config/nanobot_config.yaml` to customize:

```yaml
agents:
  - name: "data_ingestion_investigator"
    model: "gpt-4o-mini"  # Change to your preferred model
    max_iterations: 20    # Prevent infinite loops
    temperature: 0.3       # Lower = more deterministic
    system_prompt: "..."  # Custom instructions
```

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `OPENAI_API_KEY` | OpenAI API key for LLM access | Yes |
| `OPENAI_MODEL` | Model to use (default: gpt-4o-mini) | No |
| `PYTHONPATH` | Should include project root | Set automatically |

Create a `.env` file:

```bash
OPENAI_API_KEY=your-api-key-here
OPENAI_MODEL=gpt-4o-mini
PYTHONPATH=/home/aq/Documents/Source/loops
```

---

## Database Schema

### `raw_users` (Staging Table - Created Automatically)

```sql
CREATE TABLE raw_users (
    id VARCHAR,
    name VARCHAR,
    email VARCHAR,
    age VARCHAR,
    join_date VARCHAR,
    status VARCHAR,
    score VARCHAR
)
```

### `users` (Target Table - Strict Constraints)

```sql
CREATE TABLE users (
    id INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    age INTEGER NOT NULL,
    join_date DATE NOT NULL,
    status VARCHAR NOT NULL,
    score FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
)
```

### `users_clean` (Generated Clean Table)

Created by the generated cleaning pipeline with properly typed and validated data.

---

## Testing

### Test Files

- `test_nanobot_with_pipeline_tools.py` - Tests Nanobot integration with pipeline tools
- `test_pipeline_tools_with_nanobot.py` - Tests pipeline tools with Nanobot

### Run Tests

```bash
# Test pipeline tools
python -m pytest test_pipeline_tools_with_nanobot.py -v

# Test Nanobot integration
python test_nanobot_with_pipeline_tools.py
```

---

## Production Deployment Considerations

For production use, consider:

1. **Replace mock Slack alert** with real Slack webhook integration
2. **Add authentication** to the MCP server
3. **Configure proper logging** rotation and retention
4. **Add monitoring** for agent health and performance
5. **Implement circuit breakers** to prevent infinite loops
6. **Add rate limiting** for database queries
7. **Set up automated triggers** for investigation on failure
8. **Add data lineage tracking** for audit purposes

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Nanobot can't connect to LLM | Verify `OPENAI_API_KEY` is set and valid |
| Database connection errors | Check `data/ingestion.db` exists and permissions |
| Prefect authentication errors | Set `PREFECT_API_KEY` or use local mode |
| Tools not found | Ensure `PYTHONPATH` includes project root |
| Module not found errors | Activate virtual environment |
| DuckDB CLI not accessible | Install duckdb package or add venv/bin to PATH |

### Debug Mode

For verbose output:

```bash
# Run demo with debug logging
export LOG_LEVEL=DEBUG
python run_demo.py

# Or edit config/nanobot_logging.yaml
log_level: DEBUG
```

---

## Files Summary

| File | Purpose |
|------|---------|
| `run_demo.py` | Main entry point - runs complete demo workflow |
| `demo_pipeline_builder.py` | Standalone pipeline builder demonstration |
| `flows/ingestion_flow.py` | Prefect flow that fails on bad data |
| `flows/nanobot_tools.py` | Investigation tools for Nanobot |
| `flows/mcp_server.py` | MCP server for enhanced capabilities |
| `agents/pipeline_builder/tools.py` | Pipeline generation logic |
| `agents/pipeline_builder/nanobot_tools.py` | Nanobot tool classes for pipeline builder |
| `agents/pipeline_builder/config.json` | Pipeline builder configuration |
| `agents/pipeline_builder/skills.md` | Pipeline builder specific skills |
| `schemas/*.yaml` | Schema definitions for each data type |
| `config/nanobot_config.yaml` | Full Nanobot server configuration |
| `config/nanobot_config_minimal.json` | Minimal config for demo |
| `config/nanobot_logging.yaml` | Logging configuration |
| `data/source_data.csv` | Source data with intentional errors |
| `data/ingestion.db` | DuckDB database (auto-created) |
| `SKILLS.md` | General troubleshooting guide for agents |
| `AGENTS.md` | Instructions for AI agents working with this repo |
| `.env` | Environment variables |

---

## Related Documentation

- **SKILLS.md** - General troubleshooting methodology for AI agents
- **AGENTS.md** - Instructions and constraints for AI agents working with this codebase
- **schemas/*.yaml** - Data schema definitions
- **config/*.yaml** - Configuration files

---

## License

MIT License - Feel free to use and adapt for your own projects.
