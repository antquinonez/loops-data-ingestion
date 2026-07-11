# Loops Data Ingestion - Master Skills Index

**Master index for all agent skills in the autonomous data ingestion troubleshooting system**

This document serves as the **entry point** and **index** for all specialized skill files. Each stage of the workflow has its own dedicated skills document with detailed guidance for that specific phase.

## Overview

The Loops project implements a **3-stage autonomous workflow** for troubleshooting and fixing data ingestion failures:

```
┌─────────────────────────────────────────────────────────────┐
│ AUTONOMOUS DATA INGESTION TROUBLESHOOTING WORKFLOW              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────┐  │
│  │  STAGE 1:        │    │  STAGE 2:        │    │  STAGE 3:    │  │
│  │  INVESTIGATION  │───▶│  PIPELINE       │───▶│  VALIDATION  │  │
│  │                 │    │  BUILDER        │    │              │  │
│  │  "What went     │    │  "How do I fix  │    │  "Did it     │  │
│  │   wrong?"       │    │   it?"          │    │   work?"     │  │
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

---

## Stage-Specific Skills

### Stage 1: Investigation Agent

**File**: `flows/investigation_skills.md`

**Agent Identity**: Senior Data Engineer / Data Detective

**Mission**: Thoroughly investigate data ingestion failures to identify root causes, impacted data, and recommend solutions.

**Key Responsibilities**:
- Read and analyze error logs
- Inspect source data files
- Query database state
- Validate against schemas
- Identify all data quality issues
- Formulate solution recommendations
- Handoff to Pipeline Builder with clear instructions

**Tools Used**:
- `read_logs(path, tail_n)` - Read application logs
- `query_duckdb(query)` - Query DuckDB database
- `inspect_file(path, sample_size)` - Inspect CSV files
- `check_schema(path, schema)` - Validate data against schema
- `get_ingestion_status()` - Get pipeline status
- `send_slack_alert(message, severity)` - Send alerts

**Trigger**: Ingestion flow fails or user requests investigation

**Handoff**: To Stage 2 with findings and clear instructions

**When to use**: Always the **first stage** after a failure is detected

---

### Stage 2: Pipeline Builder Agent

**File**: `agents/pipeline_builder/skills.md`

**Agent Identity**: Data Pipeline Engineer

**Mission**: Automatically generate data cleaning pipelines based on schema comparisons and identified issues.

**Key Responsibilities**:
- Load and compare schemas (source vs ideal)
- Identify type mismatches, NULL issues, constraint violations
- Generate SQL and Python cleaning transformations
- Create executable Prefect flows
- Generate validation queries
- Save pipeline to `pipelines/generated/`

**Tools Used**:
- `load_ideal_schema()` - Load schema from YAML
- `infer_source_schema(file_path, sample_size)` - Infer schema from CSV
- `compare_schemas(source_path, ideal_path)` - Compare schemas
- `generate_cleaning_pipeline(source_path, ideal_path, output_table)` - Generate complete pipeline
- `write_file(path, content)` - Save generated code

**Trigger**: Investigation complete with identified issues

**Handoff**: To Stage 3 with generated pipeline path

**When to use**: After Stage 1 identifies data quality issues that require transformation

---

### Stage 3: Validation Agent

**File**: `flows/validation_skills.md`

**Agent Identity**: Data Quality Assurance Engineer

**Mission**: Execute generated pipelines and validate they produce correct, high-quality data.

**Key Responsibilities**:
- Pre-execution checks (file exists, syntax valid, etc.)
- Execute pipeline and capture results
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
- `subprocess.run()` - Execute pipeline (via Python)
- Custom validation queries (NULL checks, type checks, etc.)

**Trigger**: Pipeline Builder generates a cleaning pipeline

**Handoff**: 
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
- `schemas/ideal_schema.yaml` - Target schema
- `logs/ingestion.log` - Main ingestion log
- `pipelines/generated/` - Generated cleaning pipelines
- `config/nanobot_config.yaml` - Agent configuration

**Key Tables**:
- `raw_users` - Staging table (all source data)
- `users` - Target table (strict constraints, will fail)
- `users_clean` - Cleaned output table (generated by pipeline)

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
```

**Final Output Format** (Stage 3):
```json
{
  "status": "PASS|FAIL|WARN",
  "pipeline": "path/to/pipeline.py",
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
   - → **Fix**: Generate cleaning pipeline

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
   - Syntax errors
   - Import errors
   - Logic errors
   - → **Fix**: Debug and correct code

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
│ │ 1. Read logs/ingestion.log                              │ │ │
│ │ 2. Inspect data/source_data.csv                         │ │ │
│ │ 3. Query raw_users table                               │ │ │
│ │ 4. Validate against ideal schema                        │ │ │
│ │ 5. Identify all issues                                  │ │ │
│ │ 6. Formulate root cause analysis                       │ │ │
│ └─────────────────────────────────────────────────────────┘ │
│ Output: Complete investigation report                     │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ STAGE 2: PIPELINE BUILDER                                  │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ 1. Load ideal schema                                    │ │ │
│ │ 2. Infer source schema                                 │ │ │
│ │ 3. Compare schemas to identify mismatches                │ │ │
│ │ 4. Generate SQL transformations (CAST + COALESCE)      │ │ │
│ │ 5. Generate Python/Prefect pipeline                     │ │ │
│ │ 6. Generate validation queries                         │ │ │
│ │ 7. Save to pipelines/generated/                        │ │ │
│ └─────────────────────────────────────────────────────────┘ │
│ Output: Generated cleaning pipeline + validation queries      │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ STAGE 3: VALIDATION                                         │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ 1. Pre-execution checks                                 │ │ │
│ │ 2. Execute generated pipeline                            │ │ │
│ │ 3. Validate row counts                                   │ │ │
│ │ 4. Check NULL constraints                               │ │ │
│ │ 5. Verify types match                                   │ │ │
│ │ 6. Validate constraints (ranges, enums)                 │ │ │
│ │ 7. Check formats (emails, dates)                         │ │ │
│ │ 8. Verify data consistency                               │ │ │
│ │ 9. Test performance                                     │ │ │
│ └─────────────────────────────────────────────────────────┘ │
│ Output: Complete validation report with PASS/FAIL status      │
└─────────────────────────────────────────────────────────────┘
    │
    ├── FAIL ──► Return to Stage 2 with specific issues
    │
    └── PASS ──► Deploy to production
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
  
  3. Column: email, Issue: Format invalid, Rows: 1
     - Problem values: ['karen@example']
     - Fix: Email validation and correction

Action: Generate cleaning pipeline for data/source_data.csv
Target: users_clean table
Schema: schemas/ideal_schema.yaml
Tools: load_ideal_schema, infer_source_schema, compare_schemas, generate_cleaning_pipeline
Save to: pipelines/generated/clean_users_pipeline.py

Expected Pipeline Features:
  - Handle NULL emails with default 'unknown@example.com'
  - Cast age to INTEGER with fallback 0
  - Validate and fix email formats
  - Include all validation queries
```

**Stage 2 → Stage 3 (Pipeline Builder → Validation)**:
```
PIPELINE GENERATION COMPLETE - HANDOFF TO VALIDATION
====================================================

Pipeline Generated: pipelines/generated/clean_users_pipeline.py

Source: data/source_data.csv
Target: users_clean
Schema: schemas/ideal_schema.yaml

Issues Addressed:
  ✓ NULL emails → COALESCE(email, 'unknown@example.com')
  ✓ Type mismatch (age) → COALESCE(CAST(age AS INTEGER), 0)
  ✓ Format invalid (email) → Email validation

Transformations Applied:
  1. email: COALESCE(email, 'unknown@example.com')
  2. age: COALESCE(CAST(age AS INTEGER), 0)
  3. join_date: COALESCE(CAST(join_date AS DATE), '1970-01-01')
  4. status: COALESCE(status, 'inactive')
  5. score: COALESCE(CAST(score AS FLOAT), 0.0)

Validation Queries Generated:
  - Row count check
  - NULL checks for all columns
  - Type checks for all columns
  - Constraint checks (age range, status enum)

Action: Execute and validate the pipeline
Expected: All 13 rows processed, no NULLs, all types correct
```

**Stage 3 → Deployment or Stage 2 (Validation → Next)**:

**PASS case**:
```
VALIDATION PASSED - READY FOR DEPLOYMENT
========================================

Pipeline: pipelines/generated/clean_users_pipeline.py
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
  - Row count: 13

Action: Deploy pipeline to production workflow
```

**FAIL case**:
```
VALIDATION FAILED - RETURNING TO PIPELINE BUILDER
=================================================

Pipeline: pipelines/generated/clean_users_pipeline.py
Status: VALIDATION FAILED

Failed Checks:
  1. Type Check - age column
     - Expected: BIGINT
     - Actual: VARCHAR
     - Issue: CAST statement not working correctly
  
  2. NULL Check - email column
     - Expected: 0 NULLs
     - Actual: 2 NULLs
     - Issue: COALESCE default not applied

Root Cause: Pipeline generated with incorrect type handling

Action: Pipeline Builder should:
  - Fix CAST statements to use COALESCE properly
  - Ensure all COALESCE defaults are applied
  - Add explicit type validation

Trigger Pipeline Builder with:
  "Fix type conversion for age and NULL handling for email. Use COALESCE(CAST(...)) pattern for all numeric conversions."
```

---

## Skill Selection Guide

Use this decision tree to determine which skills file to reference:

```
Is this about...?
│
├── Diagnosing a failure?
│   ├── Need to read logs? → flows/investigation_skills.md
│   ├── Need to inspect data? → flows/investigation_skills.md
│   ├── Need to query database? → flows/investigation_skills.md
│   └── Need to validate schema? → flows/investigation_skills.md
│
├── Generating a fix?
│   ├── Need to compare schemas? → agents/pipeline_builder/skills.md
│   ├── Need to generate transformations? → agents/pipeline_builder/skills.md
│   ├── Need to create pipeline code? → agents/pipeline_builder/skills.md
│   └── Need to save generated code? → agents/pipeline_builder/skills.md
│
└── Testing a fix?
    ├── Need to execute pipeline? → flows/validation_skills.md
    ├── Need to validate output? → flows/validation_skills.md
    ├── Need to check row counts? → flows/validation_skills.md
    └── Need to verify constraints? → flows/validation_skills.md
```

---

## Quick Start for New Agents

If you're new to this project, follow this learning path:

1. **Read this file (SKILLS.md)** - Understand the overall workflow
2. **Read flows/investigation_skills.md** - Learn how to diagnose failures
3. **Read agents/pipeline_builder/skills.md** - Learn how to generate fixes
4. **Read flows/validation_skills.md** - Learn how to verify fixes
5. **Read AGENTS.md** - Learn project conventions and constraints
6. **Read README.md** - Learn project structure and setup

---

## Common Multi-Stage Workflows

### Workflow 1: Complete Troubleshooting (Most Common)
```
User: "The ingestion failed, fix it"

Stage 1 (Investigation):
  → Read logs, inspect data, query DB, validate schema
  → Output: "Found NULL emails and invalid age values"
  
Stage 2 (Pipeline Builder):
  → Compare schemas, generate transformations
  → Output: "Generated cleaning pipeline"
  
Stage 3 (Validation):
  → Execute pipeline, validate output
  → Output: "Pipeline works, data is clean"

Result: Self-healing complete
```

### Workflow 2: Investigation Only
```
User: "What caused the ingestion failure?"

Stage 1 (Investigation):
  → Full investigation
  → Output: Complete root cause analysis

Result: Analysis report (no fix generated)
```

### Workflow 3: Pipeline Generation Only
```
User: "Generate a cleaning pipeline for this data"

Stage 2 (Pipeline Builder):
  → Load schemas, compare, generate
  → Output: Generated pipeline

Result: Pipeline code (no investigation or validation)
```

### Workflow 4: Validation Only
```
User: "Validate this pipeline"

Stage 3 (Validation):
  → Execute, validate
  → Output: Validation report

Result: Pass/Fail report
```

---

## Skill File Locations

| Stage | Agent Type | Skills File | Purpose |
|-------|------------|-------------|---------|
| 1 | Investigation Agent | `flows/investigation_skills.md` | Diagnose failures |
| 2 | Pipeline Builder Agent | `agents/pipeline_builder/skills.md` | Generate fixes |
| 3 | Validation Agent | `flows/validation_skills.md` | Verify fixes |
| All | Any Agent | `SKILLS.md` (this file) | Master index + shared skills |

---

## Related Documentation

- **README.md** - Project setup, structure, and usage
- **AGENTS.md** - Instructions for AI agents working with this codebase
- **config/nanobot_config.yaml** - Agent configuration
- **runs_demo.py** - Main entry point that orchestrates all stages

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

This project implements a **multi-stage autonomous workflow** for data ingestion troubleshooting:

1. **Investigation** → Find what's wrong
2. **Pipeline Building** → Generate a fix  
3. **Validation** → Verify the fix works

Each stage has specialized skills documented in its own file. This master SKILLS.md file serves as the index to help you find the right guidance for your current task.

**Remember**: The workflow is **sequential** - each stage builds on the previous one. Start with investigation, then build, then validate.

---

*For detailed guidance on a specific stage, refer to the specialized skills file for that stage.*
