# Loops Data Ingestion - Master Skills Index

**Master index for all agent skills in the autonomous data ingestion troubleshooting system**

This document serves as the **entry point** and **index** for all specialized skill files. Each stage of the workflow has its own dedicated skills document with detailed guidance for that specific phase.

## Overview

The Loops project implements a **3-stage autonomous workflow** for troubleshooting and fixing data ingestion failures using a **consistent Prefect pipeline architecture**:

```
┌─────────────────────────────────────────────────────────────┐
│ AUTONOMOUS DATA INGESTION TROUBLESHOOTING WORKFLOW              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────┐  │
│  │  STAGE 1:        │    │  STAGE 2:        │    │  STAGE 3:    │  │
│  │  INVESTIGATION  │───▶│  PIPELINE       │───▶│  VALIDATION  │  │
│  │                 │    │  BUILDER        │    │              │  │
│  │  (Prefect      │    │  (Generates     │    │  (Prefect   │  │
│  │   Analysis)   │    │   Prefect)      │    │   Flows)   │  │
│  └─────────────────┘    └─────────────────┘    └─────────────┘  │
│         │                  │                   │            │
│         ▼                  ▼                   ▼            │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                    AGENT HANDOFF                           │  │
│  │  Investigation → Pipeline Builder → Validation            │  │
│  │         (with clear findings and instructions)              │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                             │
│  FINAL OUTPUT: Cleaned data in users_clean table              │
│                + Complete validation report                     │
└─────────────────────────────────────────────────────────────┘
```

### Pipeline Architecture Clarification

This project uses a **consistent Prefect model** for all pipeline types:

| Pipeline Type | Location | Purpose | Technology |
|---------------|----------|---------|------------|
| **Prefect Flow** | `flows/ingestion_flow.py` | Demonstrate failure | Prefect + DuckDB |
| **Prefect Flow** | `pipelines/generated/*.py` | Fix data issues | Prefect + Pandas + DuckDB |

**Why use Prefect for all pipelines?**
- **Prefect Flow**: Used for both the **demo** to show failures and for **generated cleaning pipelines** to fix data issues.
- **Consistent orchestration**: All pipelines benefit from Prefect's built-in retry, logging, and task dependency management.
- **Unified deployment**: Single deployment strategy for all pipeline types.

The **Pipeline Builder** (Stage 2) generates **Prefect flows**, not plain Python scripts.

---

## Stage-Specific Skills

### Stage 1: Investigation Agent

**File**: `flows/investigation_skills.md`

**Agent Identity**: Senior Data Engineer / Data Detective

**Mission**: Thoroughly investigate **Prefect flow** failures to identify root causes, impacted data, and recommend solutions.

**Key Responsibilities**:
- Read and analyze error logs (from Prefect flow execution)
- Inspect source data files
- Query DuckDB database state (raw_users table)
- Validate against schemas
- Identify all data quality issues
- Formulate solution recommendations
- Handoff to Pipeline Builder with clear instructions

**Tools Used**:
- `read_logs(path, tail_n)` - Read application logs
- `query_duckdb(query)` - Query DuckDB database
- `inspect_file(path, sample_size)` - Inspect CSV files
- `check_schema(path, schema)` - Validate data against schema
- `get_ingestion_status()` - Get Prefect pipeline status
- `send_slack_alert(message, severity)` - Send alerts

**Trigger**: Prefect flow fails or user requests investigation

**Handoff**: To Stage 2 with findings: "Generate **Prefect** cleaning pipeline"

**When to use**: Always the **first stage** after a failure is detected

---

### Stage 2: Pipeline Builder Agent

**File**: `agents/pipeline_builder/skills.md`

**Agent Identity**: Data Pipeline Engineer

**Mission**: Automatically generate **Prefect cleaning flows** based on schema comparisons and identified issues.

**Key Responsibilities**:
- Load and compare schemas (source vs ideal)
- Identify type mismatches, NULL issues, constraint violations
- Generate **Prefect flows** with @task and @flow decorators
- Create executable Python scripts using pandas + DuckDB with Prefect orchestration
- Generate validation queries
- Save **Prefect flow** pipeline to `pipelines/generated/`

**Tools Used** (generate Prefect flow code):
- `load_ideal_schema()` - Load schema from YAML
- `infer_source_schema(file_path, sample_size)` - Infer schema from CSV
- `compare_schemas(source_path, ideal_path)` - Compare schemas
- `generate_cleaning_pipeline(source_path, ideal_path, output_table)` - Generate **Prefect flow** cleaning pipeline
- `write_file(path, content)` - Save generated code

**Output**: Prefect flow scripts (e.g., `pipelines/generated/clean_users_pipeline.py`) that:
```python
from prefect import flow, task
import pandas as pd

@task
def clean_data(df):
    # Uses pandas within Prefect tasks
    df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(0).astype(int)
    df['email'] = df['email'].fillna('unknown@example.com')
    return df

@flow
def clean_and_load_pipeline():
    df = load_source_data()
    df = clean_data(df)
    save_to_duckdb(df, "users_clean")
```

**Trigger**: Investigation complete with identified data quality issues

**Handoff**: To Stage 3 with generated **Prefect** pipeline path

**When to use**: After Stage 1 identifies data quality issues that require transformation

---

### Stage 3: Validation Agent

**File**: `flows/validation_skills.md`

**Agent Identity**: Data Quality Assurance Engineer

**Mission**: Execute generated **Prefect** pipelines and validate they produce correct, high-quality data.

**Key Responsibilities**:
- Pre-execution checks (file exists, syntax valid, etc.)
- Execute **Prefect flow** pipeline using subprocess
- Validate row counts match expectations
- Check for NULL values in NOT NULL columns
- Verify all types match target schema
- Validate constraints (ranges, enums)
- Check data formats (emails, dates)
- Verify data consistency
- Test performance and idempotency
- Generate complete validation report

**Tools Used**:
- `query_duckdb(query)` - Query DuckDB for validation
- `subprocess.run()` - Execute Prefect flow pipeline
- Custom validation queries (NULL checks, type checks, etc.)

**Trigger**: Pipeline Builder generates a **Prefect** cleaning pipeline

**Handooff**: 
- To deployment if PASS
- To Pipeline Builder if FAIL (with specific issues)

**When to use**: Always the **final stage** before deployment

---

## Shared Skills and Knowledge

The following skills and knowledge apply across **all stages**:

### Common Tools

All agents have access to:

| Tool | Purpose | Available in Stages |
|------|---------|-------------------|
| `read_logs` | Read log files | 1, 2, 3 |
| `query_duckdb` | Query database | 1, 2, 3 |
| `inspect_file` | Inspect files | 1, 2, 3 |
| `check_schema` | Schema validation | 1, 2, 3 |

### Project Knowledge

**Project Root**: `/home/aq/Documents/Source/loops`

**Key Paths**:
- `data/source_data.csv` - Source CSV with intentional errors
- `data/ingestion.db` - DuckDB database
- `schemas/users_schema.yaml` - Target schema
- `logs/ingestion.log` - Main ingestion log (from Prefect flow)
- `pipelines/generated/` - **Prefect** cleaning flows (auto-generated)
- `config/nanobot_config.yaml` - Agent configuration

**Key Tables**:
- `raw_users` - Staging table (all source data, created by Prefect flow)
- `users` - Target table (strict constraints, Prefect flow fails here)
- `users_clean` - Cleaned output table (**created by Prefect cleaning flow**)

### Communication Patterns

**Handoff Format** (between stages):
```
HANDOFF TO [Stage Name]
=======================

From: [Previous Stage]
Findings: [Summary of what was discovered]
Action Required: [What the next stage should do]
Instructions: [Specific instructions]
Tools to Use: [List of relevant tools]
Expected Output: [What should be produced]

Note: Generated pipelines will be PREFECT flows, not plain Python.
```

**Final Output Format** (Stage 3):
```json
{
  "status": "PASS|FAIL|WARN",
  "pipeline": "path/to/prefect_pipeline.py",
  "findings": [...],
  "validation": {...},
  "recommendations": [...]
}
```

### Error Handling Patterns

**Common Error Categories**:

1. **Data Quality Errors** (Most common)
   - NULL values in required fields
   - Type mismatches
   - Format violations
   - Constraint violations
   - → **Fix**: Generate **Prefect** cleaning pipeline

2. **Configuration Errors**
   - Missing files
   - Incorrect paths
   - Permission issues
   - → **Fix**: Check configuration, paths, permissions

3. **Connection Errors**
   - Database not accessible
   - API not available
   - → **Fix**: Check connections, credentials, network

4. **Code Errors**
   - Syntax errors in generated Prefect flows
   - Import errors
   - Logic errors
   - → **Fix**: Debug and correct the generated Prefect flow code

---

## Workflow Coordination

### Complete End-to-End Flow

```
USER REQUEST / DETECTED FAILURE
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ STAGE 1: INVESTIGATION                                       │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ 1. Read logs/ingestion.log (from Prefect flow)          │ │ │
│ │ 2. Inspect data/source_data.csv                         │ │ │
│ │ 3. Query raw_users table (created by Prefect flow)      │ │ │
│ │ 4. Validate against ideal schema                        │ │ │
│ │ 5. Identify all issues                                  │ │ │
│ │ 6. Formulate root cause analysis                       │ │ │
│ └─────────────────────────────────────────────────────────┘ │
│ Output: "Generate Prefect cleaning pipeline"          │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ STAGE 2: PIPELINE BUILDER                                  │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ 1. Load ideal schema                                    │ │ │
│ │ 2. Infer source schema                                 │ │ │
│ │ 3. Compare schemas to identify mismatches                │ │ │
│ │ 4. Generate TRANSFORMATIONS (for Prefect tasks)           │ │ │
│ │ 5. Generate Prefect flow cleaning pipeline                │ │ │
│ │ 6. Generate validation queries                         │ │ │
│ │ 7. Save to pipelines/generated/clean_*.py              │ │ │
│ └─────────────────────────────────────────────────────────┘ │
│ Output: PREFECT FLOW (with @flow and @task decorators)      │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ STAGE 3: VALIDATION                                         │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ 1. Pre-execution checks                                 │ │ │
│ │ 2. Execute generated PREFECT pipeline                 │ │ │
│ │ 3. Validate row counts                                   │ │ │
│ │ 4. Check NULL constraints                               │ │ │
│ │ 5. Verify types match                                   │ │ │
│ │ 6. Validate constraints (ranges, enums)                 │ │ │
│ │ 7. Check formats (emails, dates)                         │ │ │
│ │ 8. Verify data consistency                               │ │ │
│ │ 9. Test performance                                     │ │ │
│ └─────────────────────────────────────────────────────────┘ │
│ Output: Validation report for PREFECT pipeline          │
└─────────────────────────────────────────────────────────────┘
    │
    ├── FAIL ──► Return to Stage 2 with specific issues
    │
    └── PASS ──► Deploy Prefect pipeline
```

### Agent-to-Agent Communication

**Stage 1 → Stage 2 (Investigation → Pipeline Builder)**:
```
INVESTIGATION COMPLETE - HANDOFF TO PIPELINE BUILDER
===================================================

Root Cause: ConversionException - Cannot cast 'N/A' to INTEGER for age column

Identified Issues:
  1. Column: email, Issue: NULL values, Rows: 1
     - Constraint: NOT NULL
     - Fix: COALESCE(email, 'unknown@example.com')
  
  2. Column: age, Issue: Type mismatch (string → integer), Rows: 1
     - Problem values: ['N/A']
     - Fix: COALESCE(CAST(age AS INTEGER), 0)

Action: Generate PREFECT cleaning pipeline for data/source_data.csv
Target: users_clean table
Schema: schemas/users_schema.yaml
Tools: load_ideal_schema, infer_source_schema, compare_schemas, generate_cleaning_pipeline
Save to: pipelines/generated/clean_users_pipeline.py

Expected Output: A PREFECT flow (with @flow and @task decorators) that:
  1. Loads data/source_data.csv with pandas in @task functions
  2. Applies type conversions with fillna fallback in @task functions
  3. Fills NULL values with schema defaults in @task functions
  4. Inserts cleaned data into users_clean table using DuckDB in @task function
```

**Stage 2 → Stage 3 (Pipeline Builder → Validation)**:
```
PIPELINE GENERATION COMPLETE - HANDOFF TO VALIDATION
====================================================

Pipeline Generated: pipelines/generated/clean_users_pipeline.py
Pipeline Type: PREFECT FLOW (with @flow and @task decorators)

Source: data/source_data.csv
Target: users_clean
Schema: schemas/users_schema.yaml

Issues Addressed:
  ✓ NULL emails → df['email'].fillna('unknown@example.com')
  ✓ Type mismatch (age) → pd.to_numeric(df['age'], errors='coerce').fillna(0)
  ✓ Format invalid (email) → Email validation with regex

Transformations Applied (PREFECT tasks with pandas):
  1. email: df['email'].fillna('unknown@example.com')
  2. age: pd.to_numeric(df['age'], errors='coerce').fillna(0).astype(int)
  3. join_date: pd.to_datetime(df['join_date'], errors='coerce').fillna('1970-01-01')
  4. status: df['status'].fillna('inactive')
  5. score: pd.to_numeric(df['score'], errors='coerce').fillna(0.0)

Validation Queries Generated:
  - Row count check
  - NULL checks for all columns
  - Type checks for all columns
  - Constraint checks (age range, status enum)

Action: Execute and validate the PREFECT pipeline
Expected: All 13 rows processed, no NULLs, all types correct
```

**Stage 3 → Deployment or Stage 2 (Validation → Next)**:

**PASS case**:
```
VALIDATION PASSED - READY FOR DEPLOYMENT
========================================

Pipeline: pipelines/generated/clean_users_pipeline.py
Pipeline Type: PREFECT FLOW
Status: ALL CHECKS PASSED

Validation Results:
  ✓ Execution: SUCCESS (5.23s)
  ✓ Row Count: 13/13 rows processed
  ✓ NULL Check: No NULLs in NOT NULL columns
  ✓ Type Check: All types match target schema
  ✓ Constraint Check: All constraints satisfied
  ✓ Format Check: All formats valid
  ✓ Performance: 2485.6 rows/second

Cleaned data available in:
  - Table: users_clean
  - Database: data/ingestion.db
  - Created by: PREFECT pipeline

Action: Deploy pipeline to production workflow
```

**FAIL case**:
```
VALIDATION FAILED - RETURNING TO PIPELINE BUILDER
=================================================

Pipeline: pipelines/generated/clean_users_pipeline.py
Pipeline Type: PREFECT FLOW
Status: VALIDATION FAILED

Failed Checks:
  1. Type Check - age column
     - Expected: INTEGER
     - Actual: VARCHAR
     - Issue: Type conversion in Prefect task not working correctly
  
  2. NULL Check - email column
     - Expected: 0 NULLs
     - Actual: 2 NULLs
     - Issue: fillna not applied properly in Prefect task

Root Cause: Prefect pipeline generated with incorrect logic

Action: Pipeline Builder should:
  - Fix pandas type conversion: pd.to_numeric(..., errors='coerce')
  - Ensure all fillna defaults are applied in Prefect tasks
  - Add explicit validation in the Prefect flow

Trigger Pipeline Builder with:
  "Fix type conversion for age and NULL handling for email in the PREFECT pipeline. Use pandas properly."
```

---

## Skill Selection Guide

Use this decision tree to determine which skills file to reference:

```
Is this about...?
│
├── Diagnosing a Prefect flow failure?
│   ├── Need to read logs? → flows/investigation_skills.md
│   ├── Need to inspect data? → flows/investigation_skills.md
│   ├── Need to query database? → flows/investigation_skills.md
│   └── Need to validate schema? → flows/investigation_skills.md
│
├── Generating a Prefect fix?
│   ├── Need to compare schemas? → agents/pipeline_builder/skills.md
│   ├── Need to generate transformations? → agents/pipeline_builder/skills.md
│   ├── Need to create cleaning flow? → agents/pipeline_builder/skills.md
│   └── Need to save generated code? → agents/pipeline_builder/skills.md
│
└── Testing a Prefect pipeline?
    ├── Need to execute pipeline? → flows/validation_skills.md
    ├── Need to validate output? → flows/validation_skills.md
    ├── Need to check row counts? → flows/validation_skills.md
    └── Need to verify constraints? → flows/validation_skills.md
```

---

## Quick Start for New Agents

If you're new to this project, follow this learning path:

1. **Read this file (SKILLS.md)** - Understand the overall workflow and consistent Prefect architecture
2. **Read flows/investigation_skills.md** - Learn how to diagnose Prefect flow failures
3. **Read agents/pipeline_builder/skills.md** - Learn how to generate Prefect fixes
4. **Read flows/validation_skills.md** - Learn how to verify Prefect pipelines
5. **Read AGENTS.md** - Learn project conventions and constraints
6. **Read README.md** - Learn project structure and setup

**Key Concept**: This project uses **one consistent pipeline type** - Prefect for both orchestration/demo and cleaning/fixes.

---

## Common Multi-Stage Workflows

### Workflow 1: Complete Troubleshooting (Most Common)
```
User: "The ingestion failed, fix it"

Stage 1 (Investigation):
  → Read logs from Prefect flow
  → Inspect data, query DB
  → Output: "Found NULL emails and invalid age values"
  
Stage 2 (Pipeline Builder):
  → Compare schemas
  → Generate Prefect cleaning flow
  → Output: "Generated pipelines/generated/clean_users_pipeline.py"
  
Stage 3 (Validation):
  → Execute Prefect pipeline
  → Validate output
  → Output: "Pipeline works, data is clean"

Result: Self-healing complete with Prefect fix
```

### Workflow 2: Investigation Only
```
User: "What caused the Prefect flow failure?"

Stage 1 (Investigation):
  → Full investigation of Prefect flow failure
  → Output: Complete root cause analysis

Result: Analysis report (no fix generated)
```

### Workflow 3: Pipeline Generation Only
```
User: "Generate a Prefect cleaning pipeline for this data"

Stage 2 (Pipeline Builder):
  → Load schemas, compare, generate
  → Output: Generated Prefect cleaning flow

Result: Prefect pipeline code (no investigation or validation)
```

### Workflow 4: Validation Only
```
User: "Validate this Prefect pipeline"

Stage 3 (Validation):
  → Execute Prefect pipeline
  → Validate output
  → Output: Validation report

Result: Pass/Fail report for Prefect pipeline
```

---

## Skill File Locations

| Stage | Agent Type | Skills File | Purpose | Pipeline Type |
|-------|------------|-------------|---------|----------------|
| 1 | Investigation Agent | `flows/investigation_skills.md` | Diagnose Prefect flow failures | N/A |
| 2 | Pipeline Builder Agent | `agents/pipeline_builder/skills.md` | Generate fixes | Prefect |
| 3 | Validation Agent | `flows/validation_skills.md` | Verify fixes | Prefect |
| All | Any Agent | `SKILLS.md` (this file) | Master index + shared skills | Prefect |

---

## Related Documentation

- **README.md** - Project setup, structure, consistent Prefect architecture, and usage
- **AGENTS.md** - Instructions for AI agents working with this codebase
- **config/nanobot_config.yaml** - Agent configuration
- **run_demo.py** - Main entry point that orchestrates all stages

---

## Safety Guidelines (All Stages)

1. **Never assume** - Always verify with tools
2. **Be thorough** - Check all aspects, not just the obvious ones
3. **Document everything** - Your work may be reviewed or audited
4. **Respect constraints** - Follow the do's and don'ts in AGENTS.md
5. **Test your work** - Validate before declaring success
6. **Handle errors gracefully** - Don't crash, provide useful error messages

---

## Summary

This project implements a **multi-stage autonomous workflow** with a **consistent Prefect pipeline architecture**:

1. **Investigation** → Find what's wrong with the Prefect flow
2. **Pipeline Building** → Generate a **Prefect** fix  
3. **Validation** → Verify the **Prefect** fix works

**Important Distinction**:
- **Prefect flows** are used for **both** the failing demo (`flows/ingestion_flow.py`) **and** the generated solutions (`pipelines/generated/*.py`)

Each stage has specialized skills documented in its own file. This master SKILLS.md file serves as the index to help you find the right guidance for your current task.

**Remember**: The workflow is **sequential** - each stage builds on the previous one. Start with investigation, then build a Prefect fix, then validate it.

---

*For detailed guidance on a specific stage, refer to the specialized skills file for that stage.*
