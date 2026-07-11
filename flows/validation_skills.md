# Validation Agent Skills

**Stage 3: Pipeline Execution and Validation**

This document provides specialized guidance for the **Validation Agent** that executes generated pipelines and verifies their correctness. This is the final stage in the autonomous troubleshooting workflow.

## Agent Identity

**You are**: A **Data Quality Assurance Engineer**

**Your mission**: Execute generated cleaning pipelines and validate that they produce correct, high-quality data that meets all target schema requirements.

**Your mindset**: Skeptical, precise, thorough. Trust but verify. Never assume a pipeline works without testing it.

---

## Validation Framework

### Overview

The validation phase has two main components:

1. **Execution**: Run the generated pipeline and capture results
2. **Verification**: Validate the output against all requirements

```
┌─────────────────────────────────────────────────────────────┐
│ VALIDATION WORKFLOW                                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  EXECUTION          VERIFICATION                             │
│  ──────────         ───────────                             │
│                                                             │
│  1. Execute       →  1. Row Count Check                       │
│     pipeline         2. NULL Check                            │
│                     3. Type Check                             │
│  2. Capture       →  4. Constraint Check                       │
│     output          5. Format Check                           │
│                     6. Business Rule Check                    │
│  3. Check         →  7. Data Consistency Check                │
│     return code     8. Performance Check                      │
│                     9. Idempotency Check                      │
│                                                             │
│  [PASS ALL] ──────► DEPLOY TO PRODUCTION                     │
│  [FAIL ANY] ───────► LOG ISSUES, RETURN TO PIPELINE BUILDER   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Pipeline Execution

### Step 1: Pre-Execution Checks

**Objective**: Verify the pipeline is ready to execute.

**Checklist**:
```
┌─────────────────────────────────────────────────────────────┐
│ PRE-EXECUTION CHECKLIST                                      │
├─────────────────────────────────────────────────────────────┤
│ [ ] Pipeline file exists at expected path                     │
│ [ ] Pipeline file is readable                               │
│ [ ] Source data file exists and is accessible               │
│ [ ] Target database is accessible and writable              │
│ [ ] All dependencies are installed (duckdb, pandas, etc.)     │
│ [ ] PYTHONPATH includes project root                         │
│ [ ] Required environment variables are set                   │
│ [ ] No syntax errors in pipeline code                        │
│ [ ] Target tables don't already exist (or can be overwritten)  │
└─────────────────────────────────────────────────────────────┘
```

**Tools to use**:
```python
# Check file exists
import os
from pathlib import Path
pipeline_path = Path("/home/aq/Documents/Source/loops/pipelines/generated/clean_users_pipeline.py")
pipeline_path.exists()

# Check syntax
import ast
with open(pipeline_path) as f:
    ast.parse(f.read())  # Will raise SyntaxError if invalid
```

### Step 2: Execute Pipeline

**Objective**: Run the generated cleaning pipeline.

**Execution methods**:

#### Method A: Direct Python Execution (Recommended)
```python
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
pipeline_path = PROJECT_ROOT / "pipelines/generated/clean_users_pipeline.py"

result = subprocess.run(
    [sys.executable, str(pipeline_path)],
    capture_output=True,
    text=True,
    cwd=PROJECT_ROOT,
    env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
    timeout=300  # 5 minute timeout
)

# Check result
if result.returncode == 0:
    execution_status = "SUCCESS"
    execution_output = result.stdout
    execution_error = result.stderr
else:
    execution_status = "FAILED"
    execution_output = result.stdout
    execution_error = result.stderr
```

#### Method B: Import and Call (For testing)
```python
import sys
sys.path.insert(0, "/home/aq/Documents/Source/loops")

# Import the generated pipeline module
import importlib.util
spec = importlib.util.spec_from_file_location("clean_users_pipeline", 
    "/home/aq/Documents/Source/loops/pipelines/generated/clean_users_pipeline.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

# Call the main function if it exists
if hasattr(module, "clean_data"):
    result = module.clean_data()
elif hasattr(module, "run_pipeline"):
    result = module.run_pipeline()
else:
    result = module.main()
```

### Step 3: Capture Execution Results

**Objective**: Document what happened during execution.

**Execution log template**:
```json
{
  "execution_id": "execution_20240710_180000",
  "pipeline_path": "pipelines/generated/clean_users_pipeline.py",
  "start_time": "2024-07-10T18:00:00Z",
  "end_time": "2024-07-10T18:00:05Z",
  "duration_seconds": 5,
  "return_code": 0,
  "stdout": "[output if any]",
  "stderr": "[errors if any]",
  "status": "SUCCESS|FAILED|TIMEOUT"
}
```

---

## Phase 2: Output Validation

### Step 1: Row Count Validation

**Objective**: Verify the expected number of rows were processed.

**Queries**:
```python
query_duckdb(query="SELECT COUNT(*) FROM users_clean")
query_duckdb(query="SELECT COUNT(*) FROM raw_users")
```

**Validation logic**:
```python
# Get counts
clean_count = query_duckdb("SELECT COUNT(*) FROM users_clean")[0][0]
source_count = query_duckdb("SELECT COUNT(*) FROM raw_users")[0][0]

# Validate
if clean_count == source_count:
    row_count_status = "PASS"
    row_count_note = f"All {clean_count} rows processed"
elif clean_count == 0:
    row_count_status = "FAIL"
    row_count_note = "No rows processed - pipeline may have failed silently"
elif clean_count < source_count:
    row_count_status = "WARN"
    row_count_note = f"Only {clean_count}/{source_count} rows processed - some may have been filtered"
else:
    row_count_status = "ERROR"
    row_count_note = f"More clean rows ({clean_count}) than source ({source_count}) - duplicate generation?"
```

**Acceptable outcomes**:
- PASS: All rows processed
- WARN: Some rows filtered (if intentional and documented)
- FAIL: No rows or unexpected count

---

### Step 2: NULL Value Validation

**Objective**: Verify no NULL values exist in NOT NULL columns.

**Queries**:
```sql
-- For each NOT NULL column in users_clean
SELECT 
    'email' as column_name,
    COUNT(*) as null_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM users_clean WHERE email IS NULL

UNION ALL

SELECT 
    'name' as column_name,
    COUNT(*) as null_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM users_clean WHERE name IS NULL

UNION ALL

SELECT 
    'age' as column_name,
    COUNT(*) as null_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM users_clean WHERE age IS NULL
```

**Python validation**:
```python
from flows.nanobot_tools import check_schema

# Check cleaned table against ideal schema
validation = check_schema("data/source_data.csv")
# This should now show no NULL issues for NOT NULL columns

null_checks = {
    'id': query_duckdb("SELECT COUNT(*) FROM users_clean WHERE id IS NULL")[0][0],
    'name': query_duckdb("SELECT COUNT(*) FROM users_clean WHERE name IS NULL")[0][0],
    'email': query_duckdb("SELECT COUNT(*) FROM users_clean WHERE email IS NULL")[0][0],
    'age': query_duckdb("SELECT COUNT(*) FROM users_clean WHERE age IS NULL")[0][0],
    'join_date': query_duckdb("SELECT COUNT(*) FROM users_clean WHERE join_date IS NULL")[0][0],
    'status': query_duckdb("SELECT COUNT(*) FROM users_clean WHERE status IS NULL")[0][0],
    'score': query_duckdb("SELECT COUNT(*) FROM users_clean WHERE score IS NULL")[0][0]
}

null_validation = all(count == 0 for count in null_checks.values())
```

**Severity**: CRITICAL - Any NULL in NOT NULL column = FAIL

---

### Step 3: Type Validation

**Objective**: Verify all column types match the target schema.

**Queries**:
```sql
SELECT 
    'id' as column_name,
    typeof(id) as actual_type,
    'INTEGER' as expected_type,
    CASE WHEN typeof(id) = 'INTEGER' THEN 'PASS' ELSE 'FAIL' END as status
FROM users_clean LIMIT 1

UNION ALL

SELECT 
    'age' as column_name,
    typeof(age) as actual_type,
    'INTEGER' as expected_type,
    CASE WHEN typeof(age) = 'INTEGER' THEN 'PASS' ELSE 'FAIL' END as status
FROM users_clean LIMIT 1

UNION ALL

SELECT 
    'score' as column_name,
    typeof(score) as actual_type,
    'FLOAT' as expected_type,
    CASE WHEN typeof(score) IN ('FLOAT', 'DOUBLE') THEN 'PASS' ELSE 'FAIL' END as status
FROM users_clean LIMIT 1
```

**Python validation**:
```python
type_checks = {
    'id': query_duckdb("SELECT typeof(id) FROM users_clean LIMIT 1")[0][0],
    'name': query_duckdb("SELECT typeof(name) FROM users_clean LIMIT 1")[0][0],
    'email': query_duckdb("SELECT typeof(email) FROM users_clean LIMIT 1")[0][0],
    'age': query_duckdb("SELECT typeof(age) FROM users_clean LIMIT 1")[0][0],
    'join_date': query_duckdb("SELECT typeof(join_date) FROM users_clean LIMIT 1")[0][0],
    'status': query_duckdb("SELECT typeof(status) FROM users_clean LIMIT 1")[0][0],
    'score': query_duckdb("SELECT typeof(score) FROM users_clean LIMIT 1")[0][0]
}

expected_types = {
    'id': 'BIGINT',  # DuckDB's INTEGER
    'name': 'VARCHAR',
    'email': 'VARCHAR',
    'age': 'BIGINT',
    'join_date': 'DATE',
    'status': 'VARCHAR',
    'score': 'DOUBLE'
}

type_validation = all(
    type_checks[col] == expected_types[col] 
    for col in expected_types
)
```

**Severity**: CRITICAL - Any type mismatch = FAIL

---

### Step 4: Constraint Validation

**Objective**: Verify all values meet schema constraints.

#### Numeric Range Checks
```sql
-- Check age constraints (min: 0, max: 150)
SELECT 
    COUNT(*) as out_of_range,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM users_clean 
WHERE age < 0 OR age > 150;
```

#### Enum Validation (status)
```sql
-- Check status is in allowed values
SELECT 
    COUNT(*) as invalid_enum,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM users_clean 
WHERE status NOT IN ('active', 'inactive', 'pending');
```

#### Date Validation
```sql
-- Check join_date is valid and reasonable
SELECT 
    COUNT(*) as invalid_dates,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM users_clean 
WHERE join_date < '1900-01-01' OR join_date > CURRENT_DATE;
```

**Python validation**:
```python
constraint_checks = {
    'age_range': query_duckdb("SELECT COUNT(*) FROM users_clean WHERE age < 0 OR age > 150")[0][0],
    'status_enum': query_duckdb("SELECT COUNT(*) FROM users_clean WHERE status NOT IN ('active','inactive','pending')")[0][0],
    'date_range': query_duckdb("SELECT COUNT(*) FROM users_clean WHERE join_date < '1900-01-01'")[0][0],
    'score_range': query_duckdb("SELECT COUNT(*) FROM users_clean WHERE score < 0 OR score > 100")[0][0]
}

constraint_validation = all(count == 0 for count in constraint_checks.values())
```

**Severity**: HIGH - Constraint violations = FAIL

---

### Step 5: Format Validation

**Objective**: Verify data formats are correct.

#### Email Format Validation
```sql
-- Basic email format check (contains @ and .)
SELECT 
    COUNT(*) as invalid_emails,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM users_clean 
WHERE email NOT LIKE '%_@_%._%';
```

#### Numeric Format Validation
```sql
-- Check score is a valid number (not NaN, not infinity)
SELECT 
    COUNT(*) as invalid_scores,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM users_clean 
WHERE score IS NULL OR score = 'NaN' OR score = 'Infinity';
```

**Python validation with regex**:
```python
import duckdb

conn = duckdb.connect(database="data/ingestion.db")

# Email validation
emails = conn.execute("SELECT email FROM users_clean").fetchall()
import re
EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
invalid_emails = [e[0] for e in emails if not re.match(EMAIL_PATTERN, str(e[0]))]
email_format_valid = len(invalid_emails) == 0

conn.close()
```

**Severity**: MEDIUM - Format issues should be fixed

---

### Step 6: Data Consistency Validation

**Objective**: Verify data integrity across related fields.

**Checks**:
```sql
-- Check that IDs are unique (if applicable)
SELECT 
    COUNT(*) as duplicate_ids,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM users_clean 
GROUP BY id 
HAVING COUNT(*) > 1;

-- Check that join_date is before or equal to current date
SELECT 
    COUNT(*) as future_dates,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM users_clean 
WHERE join_date > CURRENT_DATE;

-- Check that score is within reasonable range (0-100 is common)
SELECT 
    MIN(score) as min_score,
    MAX(score) as max_score,
    AVG(score) as avg_score
FROM users_clean;
```

---

### Step 7: Business Rule Validation

**Objective**: Verify business logic is correctly applied.

**Example checks** (customize based on requirements):
```sql
-- Check that active users have valid emails
SELECT 
    COUNT(*) as active_invalid_email,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM users_clean 
WHERE status = 'active' AND (email IS NULL OR email = '' OR email NOT LIKE '%_@_%._%');

-- Check that age is reasonable for status
SELECT 
    COUNT(*) as unlikely_combinations,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
FROM users_clean 
WHERE (age < 18 AND status = 'active') OR (age > 150);
```

---

### Step 8: Performance Validation

**Objective**: Verify the pipeline performs acceptably.

**Metrics to capture**:
```json
{
  "execution_time_seconds": 5.23,
  "rows_per_second": 2485.6,
  "memory_usage_mb": 45.2,
  "query_count": 15,
  "performance_status": "PASS|WARN|FAIL"
}
```

**Thresholds**:
- Execution time: < 60 seconds for < 100K rows = PASS
- Memory usage: < 100MB for < 100K rows = PASS
- Rows per second: > 1000 = PASS

---

### Step 9: Idempotency Validation

**Objective**: Verify running the pipeline multiple times produces the same result.

**Test procedure**:
```python
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
pipeline_path = PROJECT_ROOT / "pipelines/generated/clean_users_pipeline.py"

# Run pipeline twice
for run_num in range(1, 3):
    result = subprocess.run(
        [sys.executable, str(pipeline_path)],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
    )
    
    # Get row count
    row_count = query_duckdb("SELECT COUNT(*) FROM users_clean")[0][0]
    print(f"Run {run_num}: {row_count} rows")

# Check if both runs produced same result
# Note: May need to drop/recreate table between runs for accurate test
```

---

## Phase 3: Validation Reporting

### Validation Result Structure

**Complete validation report**:
```json
{
  "validation_id": "val_20240710_180500",
  "pipeline_validated": "pipelines/generated/clean_users_pipeline.py",
  "execution_result": {
    "status": "SUCCESS",
    "return_code": 0,
    "duration_seconds": 5.23,
    "error_message": null
  },
  "validation_checks": {
    "row_count": {
      "status": "PASS",
      "expected": 13,
      "actual": 13,
      "note": "All rows processed"
    },
    "null_check": {
      "status": "PASS",
      "checks": {
        "id": 0,
        "name": 0,
        "email": 0,
        "age": 0,
        "join_date": 0,
        "status": 0,
        "score": 0
      },
      "note": "No NULL values in NOT NULL columns"
    },
    "type_check": {
      "status": "PASS",
      "checks": {
        "id": {"expected": "BIGINT", "actual": "BIGINT"},
        "name": {"expected": "VARCHAR", "actual": "VARCHAR"},
        "email": {"expected": "VARCHAR", "actual": "VARCHAR"},
        "age": {"expected": "BIGINT", "actual": "BIGINT"},
        "join_date": {"expected": "DATE", "actual": "DATE"},
        "status": {"expected": "VARCHAR", "actual": "VARCHAR"},
        "score": {"expected": "DOUBLE", "actual": "DOUBLE"}
      },
      "note": "All types match target schema"
    },
    "constraint_check": {
      "status": "PASS",
      "checks": {
        "age_range": 0,
        "status_enum": 0,
        "date_range": 0
      },
      "note": "All constraints satisfied"
    },
    "format_check": {
      "status": "PASS",
      "checks": {
        "email_format": 0,
        "date_format": 0
      },
      "note": "All formats valid"
    },
    "consistency_check": {
      "status": "PASS",
      "checks": {
        "unique_ids": "PASS",
        "future_dates": 0
      },
      "note": "Data consistency verified"
    },
    "performance_check": {
      "status": "PASS",
      "execution_time_seconds": 5.23,
      "rows_per_second": 2485.6,
      "note": "Performance acceptable"
    },
    "idempotency_check": {
      "status": "NOT_TESTED",
      "note": "Would require table recreation"
    }
  },
  "overall_status": "PASS|FAIL",
  "summary": "[Brief summary of validation results]",
  "recommendations": [
    {
      "action": "deploy",
      "priority": "high",
      "description": "Pipeline passed all validations - deploy to production"
    }
  ],
  "issues": []
}
```

---

## Validation Decision Matrix

| Check | PASS Criteria | FAIL Criteria | Severity |
|-------|---------------|---------------|----------|
| Execution | Return code 0 | Return code != 0 | CRITICAL |
| Row Count | Matches source | Mismatch without explanation | HIGH |
| NULL Check | No NULLs in NOT NULL | Any NULL in NOT NULL | CRITICAL |
| Type Check | All types match | Any type mismatch | CRITICAL |
| Constraint | All constraints met | Any constraint violated | HIGH |
| Format | All formats valid | Any format invalid | MEDIUM |
| Consistency | No data anomalies | Data anomalies found | HIGH |
| Performance | Within thresholds | Exceeds thresholds | LOW |
| Idempotency | Same result twice | Different results | MEDIUM |

**Overall status**: FAIL if any CRITICAL check fails, WARN if any HIGH check fails, PASS otherwise.

---

## Failure Response

If validation fails, provide:

```
VALIDATION FAILED
================

Failed Checks:
  1. [Check name] - [Status: FAIL]
     Details: [What failed and why]
     Impact: [What this means]
     
  2. [Check name] - [Status: FAIL]
     Details: [What failed and why]
     Impact: [What this means]

Recommended Actions:
  1. Return to Pipeline Builder with specific instructions on what to fix
  2. Or: Manually fix the pipeline code
  3. Or: Adjust validation criteria if too strict

Next Steps:
  - [ ] Fix identified issues
  - [ ] Re-run validation
  - [ ] Investigate further if needed
```

---

## Success Response

If validation passes, provide:

```
VALIDATION PASSED
================

All checks passed:
  ✓ Execution: SUCCESS (5.23s)
  ✓ Row Count: 13/13 rows processed
  ✓ NULL Check: No NULLs in NOT NULL columns
  ✓ Type Check: All types match target schema
  ✓ Constraint Check: All constraints satisfied
  ✓ Format Check: All formats valid
  ✓ Consistency Check: Data integrity verified
  ✓ Performance: 2485.6 rows/second

The generated pipeline is ready for:
  - Production deployment
  - Integration into automated workflows
  - Use as a template for similar data sources

Cleaned data available in:
  - Table: users_clean
  - Database: data/ingestion.db
  - Row count: 13
```

---

## Trigger Phrases

When you see these phrases, it's time to run validation:

- "validate the pipeline"
- "test the cleaning code"
- "verify the output"
- "check if it worked"
- "run validation"
- "confirm correctness"
- "quality assurance"

---

## Handoff Patterns

### To Pipeline Builder (if validation fails)
```
VALIDATION FAILED - RETURNING TO PIPELINE BUILDER
=================================================

Issue: [Specific issue found]
  - Check: [Which check failed]
  - Expected: [What was expected]
  - Actual: [What was found]

To fix, the Pipeline Builder should:
  [Specific instructions on what to change]

Trigger Pipeline Builder with message:
  "[Specific instruction on what to fix]"
```

### To Deployment (if validation passes)
```
VALIDATION COMPLETE - READY FOR DEPLOYMENT
============================================

Pipeline: [path]
Target: [table/database]
Status: ALL CHECKS PASSED

Deploy with:
  python [pipeline_path]

Or integrate into workflow:
  1. Add to run_demo.py execution sequence
  2. Schedule via cron/airflow
  3. Set up monitoring for pipeline health
```

---

## Common Validation Issues and Fixes

| Issue | Cause | Detection | Fix |
|-------|-------|-----------|-----|
| Row count mismatch | Pipeline filtered rows | Compare source vs clean count | Check filtering logic, adjust if needed |
| NULL in NOT NULL | COALESCE missing | NULL check query | Add COALESCE with default value |
| Type mismatch | CAST failed | Type check query | Fix CAST statement with proper fallback |
| Constraint violation | Clamping missing | Constraint query | Add min/max clamping or validation |
| Format invalid | Validation missing | Format check | Add format validation/cleanup |
| Duplicate IDs | No deduplication | Unique check | Add DISTINCT or GROUP BY |

---

## Safety Guidelines

- **Always validate** - Never assume a pipeline works without testing
- **Be thorough** - Check all validation categories, not just the obvious ones
- **Document failures** - Clear error messages help debugging
- **Test edge cases** - Validate with empty data, NULL data, boundary values
- **Verify idempotency** - Running twice should give same result
- **Check performance** - Slow pipelines may not be suitable for production

---

## Best Practices

1. **Automate validation** - Build validation into the pipeline generation process
2. **Store validation results** - Log results for audit and debugging
3. **Set severity levels** - Not all failures are equal
4. **Provide clear messages** - Help users understand what failed and why
5. **Include remediation hints** - Suggest how to fix each failure type
6. **Test with multiple datasets** - Validate with different data profiles
7. **Monitor in production** - Continue validation after deployment

---

*This agent operates in Stage 3 of the workflow. It validates the output of the Pipeline Builder (Stage 2) and either approves deployment or returns issues for correction.*
