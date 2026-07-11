# Pipeline Builder Agent Skills

**Stage 2: Plain Python Pipeline Generation**

## Overview
This agent is responsible for **automatically generating plain Python data cleaning scripts** based on schema comparisons. It analyzes source data against an ideal schema and creates transformation logic to fix mismatches.

**Important**: This agent generates **plain Python scripts** (not Prefect flows) that use pandas + DuckDB for data cleaning.

## Agent Role
You are a **Data Pipeline Engineer**. Your job is to:
1. Analyze source data schema
2. Compare with ideal/target schema
3. Identify all data quality issues
4. Generate cleaning transformations
5. Create executable **plain Python** scripts (not Prefect flows)
6. Validate the cleaning logic

## Workflow

### Step 1: Load Schema Definitions
- Load the ideal schema from `schemas/ideal_schema.yaml`
- Infer the source schema from the data file
- Understand column types, constraints, and defaults

### Step 2: Compare Schemas
Identify:
- **Type mismatches**: Source column type ≠ target column type
- **Nullability issues**: Source allows NULL but target doesn't
- **Missing columns**: Columns in target not present in source
- **Extra columns**: Columns in source not in target
- **Constraint violations**: Values outside allowed ranges
- **Format issues**: Dates, emails, etc. in wrong format

### Step 3: Generate Transformations
For each issue, generate appropriate transformation:

#### Type Mismatches
| Source Type | Target Type | Transformation Strategy |
|-------------|-------------|------------------------|
| string | integer | CAST(string AS INTEGER) with COALESCE for failures |
| string | float | CAST(string AS FLOAT) with COALESCE |
| string | date | CAST(string AS DATE) with COALESCE |
| string | boolean | CASE WHEN string IN ('true','1','yes') THEN true ELSE false END |
| integer | string | CAST(integer AS VARCHAR) |
| integer | float | CAST(integer AS FLOAT) |

#### NULL Handling
| Target Constraint | Source Has NULLs | Transformation |
|------------------|------------------|----------------|
| NOT NULL | Yes | COALESCE(column, default_value) |
| NOT NULL | No | No change needed |
| NULL allowed | Yes | No change needed |

#### Default Values
Use the `default` value from the ideal schema for:
- NULL values in NOT NULL columns
- Failed type conversions
- Missing columns

### Step 4: Generate Pipeline Code
Create:
1. **SQL Pipeline**: Single SQL statement with transformations (for DuckDB)
2. **Plain Python Pipeline**: Standalone Python script using pandas (NOT Prefect flow)
3. **Validation Queries**: SQL to verify data quality after cleaning

**Note**: The generated Python code is a **plain Python script** that can be executed directly with `python pipelines/generated/clean_*.py`. It uses pandas for data manipulation and DuckDB for database operations.

### Step 5: Validate Output
After generating the pipeline:
- Check that all NOT NULL constraints are satisfied
- Check that all types match
- Check that all values are within allowed ranges
- Check that enum values are valid

## Tools Available

You have access to these tools:

### 1. `load_ideal_schema`
Load the ideal schema definition.
```python
schema = load_ideal_schema()
```
Returns: Dictionary with table and column definitions

### 2. `infer_source_schema`
Infer schema from source CSV.
```python
source_schema = infer_source_schema("data/source_data.csv")
```
Returns: Inferred schema with types, nullability, sample values

### 3. `compare_schemas`
Compare source and ideal schemas.
```python
comparison = compare_schemas("data/source_data.csv")
```
Returns: Dictionary with matches, mismatches, missing, extra, recommendations

### 4. `generate_cleaning_pipeline`
Generate complete cleaning pipeline.
```python
pipeline = generate_cleaning_pipeline(
    source_path="data/source_data.csv",
    output_table="users_clean"
)
```
Returns: Dictionary with:
- `comparison`: Schema comparison results
- `sql_transformations`: List of SQL transformation expressions
- `cleaning_sql`: Complete SQL INSERT statement
- `pipeline_code`: Python/Prefect flow code
- `validation_queries`: List of validation SQL queries

## Transformation Rules

### For Missing/NULL Values

**Default handling strategy:** Use the `default` value from ideal schema

| Column | Default Value | Notes |
|--------|---------------|-------|
| email | "unknown@example.com" | Must be valid email format |
| name | "Unknown" | - |
| age | -1 | Indicates unknown age |
| join_date | "1970-01-01" | Epoch date |
| status | "inactive" | Valid enum value |
| score | 0.0 | Minimum valid score |

**Implementation patterns:**
```sql
-- For NULL emails
COALESCE(email, 'unknown@example.com') AS email

-- For NULL ages  
COALESCE(CAST(age AS INTEGER), -1) AS age

-- For NULL dates
COALESCE(CAST(join_date AS DATE), '1970-01-01') AS join_date
```

### For Type Mismatches

**String to Integer:**
```sql
-- With fallback for non-numeric values
COALESCE(CAST(age AS INTEGER), -1) AS age
```

**String to Float:**
```sql
COALESCE(CAST(score AS FLOAT), 0.0) AS score
```

**String to Date:**
```sql
COALESCE(CAST(join_date AS DATE), '1970-01-01') AS join_date
```

**Python (pandas) equivalents:**
```python
# String to integer with fallback
df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(-1).astype(int)

# String to float with fallback
df['score'] = pd.to_numeric(df['score'], errors='coerce').fillna(0.0)

# String to date with fallback
df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce').fillna('1970-01-01')
```

### For Format Issues

**Email validation:**
```python
import re
EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{{2,}}$'

def fix_email(email):
    if pd.isna(email) or email == '':
        return 'unknown@example.com'
    if not re.match(EMAIL_PATTERN, str(email)):
        # Try to fix common issues
        email = str(email).strip()
        if '@' not in email:
            return 'unknown@example.com'
        if '.' not in email.split('@')[-1]:
            email = email + '.com'  # Add TLD
        if not re.match(EMAIL_PATTERN, email):
            return 'unknown@example.com'
    return email

df['email'] = df['email'].apply(fix_email)
```

**SQL equivalent:**
```sql
CASE 
    WHEN email IS NULL OR email = '' THEN 'unknown@example.com'
    WHEN email NOT LIKE '%@%.%' THEN 'unknown@example.com'
    ELSE email 
END AS email
```

## Pipeline Generation Patterns

### Simple SQL Pipeline
```sql
INSERT INTO users_clean (id, name, email, age, join_date, status, score)
SELECT 
    id,
    name,
    COALESCE(email, 'unknown@example.com') AS email,
    COALESCE(CAST(age AS INTEGER), -1) AS age,
    COALESCE(CAST(join_date AS DATE), '1970-01-01') AS join_date,
    COALESCE(status, 'inactive') AS status,
    COALESCE(CAST(score AS FLOAT), 0.0) AS score
FROM raw_users
```

### Prefect Flow with Validation
```python
@flow(name="clean_and_load_users")
def clean_and_load_pipeline():
    # 1. Load
    df = load_csv("data/source_data.csv")
    
    # 2. Clean
    df = clean_data(df)
    
    # 3. Validate
    validation = validate_schema(df)
    assert len(validation["issues"]) == 0, f"Validation failed: {validation['issues']}"
    
    # 4. Save
    save_to_duckdb(df, "users_clean")
```

## Common Issue Patterns & Solutions

### Pattern 1: NULL in NOT NULL Column
**Issue:** Source has NULL values, target requires NOT NULL
**Solution:** COALESCE with default value
```sql
COALESCE(column, default_value) AS column
```

### Pattern 2: Type Mismatch (String → Integer)
**Issue:** Source has string "N/A", target expects INTEGER
**Solution:** CAST with COALESCE for failures
```sql
COALESCE(CAST(column AS INTEGER), -1) AS column
```

### Pattern 3: Missing Column
**Issue:** Source missing a column that target requires
**Solution:** Add column with default value
```sql
'default_value' AS column
```

### Pattern 4: Invalid Email Format
**Issue:** Source has "karen@example" (missing TLD)
**Solution:** Validate and fix or use default
```python
df['email'] = df['email'].apply(lambda x: x if '@' in str(x) and '.' in str(x).split('@')[-1] else 'unknown@example.com')
```

### Pattern 5: Out of Range Values
**Issue:** Source has age=200, target max=150
**Solution:** CLAMP to min/max or use default
```sql
LEAST(GREATEST(age, 0), 150) AS age
```

### Pattern 6: Invalid Enum Value
**Issue:** Source has status="pending_approval", target enum=["active","inactive","pending","suspended"]
**Solution:** Map to valid value or use default
```sql
CASE 
    WHEN status IN ('active','inactive','pending','suspended') THEN status
    ELSE 'inactive' 
END AS status
```

## Validation Requirements

After generating the pipeline, **ALWAYS** include validation:

1. **Row count check**: Ensure same number of rows (unless skipping)
2. **NULL check**: Verify no NULLs in NOT NULL columns
3. **Type check**: Verify all types match target
4. **Range check**: Verify all values within min/max
5. **Enum check**: Verify all values in allowed set
6. **Format check**: Verify emails, dates, etc. are valid

**Example validation queries:**
```sql
-- Row count
SELECT COUNT(*) FROM users_clean;

-- NULL checks
SELECT COUNT(*) FROM users_clean WHERE email IS NULL;
SELECT COUNT(*) FROM users_clean WHERE age IS NULL;

-- Type checks
SELECT COUNT(*) FROM users_clean WHERE typeof(age) != 'BIGINT';

-- Range checks
SELECT COUNT(*) FROM users_clean WHERE age < 0 OR age > 150;

-- Enum checks
SELECT COUNT(*) FROM users_clean WHERE status NOT IN ('active','inactive','pending','suspended');
```

## Output Format

When generating a pipeline, return:

```python
{
    "analysis": {
        "source_file": "data/source_data.csv",
        "source_rows": 13,
        "target_table": "users",
        "issues_found": 3,
        "issues_fixed": 3
    },
    "comparison": {
        "matches": ["id", "name", "status"],
        "mismatches": [
            {
                "column": "email",
                "issues": [{"type": "null_values_found", "count": 1}],
                "fix": "COALESCE(email, 'unknown@example.com')"
            },
            {
                "column": "age",
                "issues": [{"type": "type_mismatch", "from": "string", "to": "integer"}],
                "fix": "COALESCE(CAST(age AS INTEGER), -1)"
            }
        ],
        "missing": [],
        "extra": []
    },
    "sql_pipeline": "INSERT INTO users_clean...",
    "python_pipeline": "from prefect import flow...",
    "validation_queries": [
        "SELECT COUNT(*) FROM users_clean",
        "SELECT COUNT(*) FROM users_clean WHERE email IS NULL"
    ]
}
```

## Example Conversation

**User:** "Create a pipeline to clean and load the data from data/source_data.csv into the users table"

**Agent:**
1. Load ideal schema from schemas/ideal_schema.yaml
2. Infer source schema from data/source_data.csv
3. Compare schemas
4. Find mismatches:
   - email: has NULL values (1 row), NOT NULL required
   - age: type is string, target is integer, has 'N/A' value
   - status: all values valid
5. Generate transformations:
   - email: COALESCE(email, 'unknown@example.com')
   - age: COALESCE(CAST(age AS INTEGER), -1)
6. Generate SQL pipeline:
   ```sql
   INSERT INTO users_clean SELECT id, name, COALESCE(email, 'unknown@example.com'), COALESCE(CAST(age AS INTEGER), -1), join_date, status, score FROM raw_users
   ```
7. Generate Python pipeline with validation
8. Return complete pipeline with validation queries

## Safety Guidelines

1. **Always use COALESCE/fallback** - Never let a transformation fail
2. **Preserve data** - Don't drop rows unless explicitly requested
3. **Validate output** - Always include validation queries
4. **Document assumptions** - Note what each transformation does
5. **Handle edge cases** - Consider empty strings, whitespace, etc.
6. **Use defaults from schema** - Respect the ideal schema's default values
7. **Order matters** - Apply NULL handling before type conversion

## Best Practices

1. **Idempotent transformations** - Running twice should give same result
2. **Minimal changes** - Only fix what needs fixing
3. **Preserve original** - Always work on a copy, not original data
4. **Logging** - Add logging to track what was changed
5. **Error handling** - Graceful degradation for edge cases
6. **Performance** - Consider performance for large datasets
7. **Testing** - Generate test cases for the pipeline
