import pandas as pd
import duckdb
from pathlib import Path
import json
import re


def load_source_data(source_path: str) -> pd.DataFrame:
    """Load source CSV into DataFrame."""
    return pd.read_csv(source_path)


def load_ideal_schema(schema_path: str) -> dict:
    """Load ideal schema from YAML file."""
    import yaml
    with open(schema_path, 'r') as f:
        return yaml.safe_load(f)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean data according to ideal schema."""
    # Make a copy to avoid modifying original
    df = df.copy()

    # Apply transformations
    # Convert age to integer with clamping
    df['age_num'] = pd.to_numeric(df['age'], errors='coerce')
    df['age'] = df['age_num'].fillna(0).astype(int)
    
    df['age'] = df['age'].clip(lower=0)
    
    df['age'] = df['age'].clip(upper=150)
    
    df = df.drop(columns=['age_num'])
    df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce').fillna('1970-01-01')
    df['score'] = pd.to_numeric(df['score'], errors='coerce').fillna(0.0)
    df['score'] = df['score'].clip(lower=0.0)
    df['score'] = df['score'].clip(upper=100.0)
    df['email'] = df['email'].fillna('unknown@example.com')

    return df


def validate_cleaned_data(df: pd.DataFrame, ideal_schema: dict, table_name: str = "users") -> dict:
    """Validate cleaned data against ideal schema with comprehensive checks."""
    validation = {
        "row_count": len(df),
        "columns": list(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "dtypes": df.dtypes.to_dict(),
        "column_validations": {},
        "issues": []
    }

    # Get the table schema from ideal schema
    tables = ideal_schema.get("tables", {})
    if table_name not in tables:
        # Try to find the table by matching columns
        for t_name, t_schema in tables.items():
            t_columns = set(c["name"] for c in t_schema.get("columns", []))
            if set(df.columns) == t_columns:
                table_name = t_name
                break
    
    table_schema = tables.get(table_name, {})
    columns_def = table_schema.get("columns", [])
    
    # Convert columns to name -> defn mapping
    ideal_by_name = {col["name"]: col for col in columns_def}
    
    # Validate each column
    for col_name, col_def in ideal_by_name.items():
        if col_name not in df.columns:
            validation["issues"].append(f"Column '{col_name}' missing from cleaned data")
            continue
        
        col_validations = {
            "column": col_name,
            "ideal_type": col_def.get("type", "string"),
            "actual_type": str(df[col_name].dtype),
            "null_count": int(df[col_name].isnull().sum()),
            "issues": []
        }
        
        # Check nullability
        ideal_nullable = col_def.get("nullable", True)
        null_count = col_validations["null_count"]
        if null_count > 0 and not ideal_nullable:
            col_validations["issues"].append({
                "type": "null_values",
                "count": null_count,
                "severity": "high",
                "message": f"Column has {null_count} null values but is defined as NOT NULL"
            })
        
        # Check type
        ideal_type = col_def.get("type", "string").lower()
        actual_dtype = str(df[col_name].dtype)
        
        # Map pandas dtypes to our types
        dtype_mapping = {
            "int64": "integer", "int32": "integer", "Int64": "integer", "Int32": "integer",
            "float64": "float", "float32": "float", "Float64": "float", "Float32": "float",
            "object": "string", "string": "string", "str": "string",
            "datetime64[ns]": "date", "datetime64": "date",
            "datetime64[us]": "date", "<M8[us]": "date", "<M8[ns]": "date"
        }
        
        # Handle pandas 3.0+ StringDtype which has repr like "<StringDtype(na_value=nan)>"
        if actual_dtype.startswith("<StringDtype"):
            mapped_dtype = "string"
        elif actual_dtype.startswith("<Int"):
            mapped_dtype = "integer"
        elif actual_dtype.startswith("<Float"):
            mapped_dtype = "float"
        elif actual_dtype.startswith("<M8"):  # datetime64
            mapped_dtype = "date"
        else:
            mapped_dtype = dtype_mapping.get(actual_dtype, "unknown")
        
        if mapped_dtype != ideal_type:
            col_validations["issues"].append({
                "type": "type_mismatch",
                "expected": ideal_type,
                "actual": mapped_dtype,
                "severity": "high",
                "message": f"Expected type '{ideal_type}', got '{mapped_dtype}'"
            })
        
        # Check constraints
        constraints = col_def.get("constraints", {})
        
        # Range checks for numeric types
        if ideal_type in ["integer", "float"] and not df[col_name].isnull().all():
            non_null = df[col_name].dropna()
            if not non_null.empty:
                if "min" in constraints:
                    min_val = constraints["min"]
                    below_min = (non_null < min_val).sum()
                    if below_min > 0:
                        col_validations["issues"].append({
                            "type": "below_min",
                            "count": int(below_min),
                            "min": min_val,
                            "actual_min": float(non_null.min()),
                            "severity": "high",
                            "message": f"{below_min} values below minimum {min_val}"
                        })
                
                if "max" in constraints:
                    max_val = constraints["max"]
                    above_max = (non_null > max_val).sum()
                    if above_max > 0:
                        col_validations["issues"].append({
                            "type": "above_max",
                            "count": int(above_max),
                            "max": max_val,
                            "actual_max": float(non_null.max()),
                            "severity": "high",
                            "message": f"{above_max} values above maximum {max_val}"
                        })
        
        # Enum checks
        if "enum" in col_def:
            valid_values = col_def["enum"]
            non_null = df[col_name].dropna()
            if not non_null.empty:
                invalid_mask = ~non_null.isin(valid_values)
                invalid_count = invalid_mask.sum()
                if invalid_count > 0:
                    invalid_examples = non_null[invalid_mask].head(5).tolist()
                    col_validations["issues"].append({
                        "type": "invalid_enum",
                        "count": int(invalid_count),
                        "valid_values": valid_values,
                        "invalid_examples": invalid_examples,
                        "severity": "high",
                        "message": f"{invalid_count} values not in allowed enum: {valid_values}"
                    })
        
        # String length checks
        if ideal_type == "string" and not df[col_name].isnull().all():
            non_null = df[col_name].dropna()
            if not non_null.empty:
                if "min_length" in constraints:
                    min_len = constraints["min_length"]
                    too_short = (non_null.str.len() < min_len).sum()
                    if too_short > 0:
                        col_validations["issues"].append({
                            "type": "too_short",
                            "count": int(too_short),
                            "min_length": min_len,
                            "severity": "medium",
                            "message": f"{too_short} values shorter than minimum length {min_len}"
                        })
                
                if "max_length" in constraints:
                    max_len = constraints["max_length"]
                    too_long = (non_null.str.len() > max_len).sum()
                    if too_long > 0:
                        col_validations["issues"].append({
                            "type": "too_long",
                            "count": int(too_long),
                            "max_length": max_len,
                            "severity": "medium",
                            "message": f"{too_long} values longer than maximum length {max_len}"
                        })
        
        # Pattern check (for email, etc.)
        if col_def.get("format") == "email":
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            non_null = df[col_name].dropna()
            if not non_null.empty:
                invalid_emails = ~non_null.str.match(email_pattern, na=False)
                invalid_count = invalid_emails.sum()
                if invalid_count > 0:
                    col_validations["issues"].append({
                        "type": "invalid_email",
                        "count": int(invalid_count),
                        "severity": "high",
                        "message": f"{invalid_count} values have invalid email format"
                    })
        
        validation["column_validations"][col_name] = col_validations
        
        # Add issues to top-level
        for issue in col_validations["issues"]:
            validation["issues"].append(f"[{col_name}] {issue['message']}")
    
    # Check for extra columns
    for col_name in df.columns:
        if col_name not in ideal_by_name:
            validation["issues"].append(f"Extra column '{col_name}' not in ideal schema")
    
    validation["is_valid"] = len(validation["issues"]) == 0
    return validation


def save_to_duckdb(df: pd.DataFrame, table_name: str) -> int:
    """Save cleaned data to DuckDB."""
    conn = duckdb.connect(database="data/ingestion.db")
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    row_count = len(df)
    conn.close()
    return row_count


def clean_and_load_pipeline(source_path: str = "data/source_data.csv"):
    """Complete cleaning and loading pipeline."""

    # Step 1: Load source data
    df = load_source_data(source_path)
    print(f"Loaded {len(df)} rows from {source_path}")

    # Step 2: Clean data
    cleaned_df = clean_data(df)
    print(f"Cleaned data: {len(cleaned_df)} rows, columns: {list(cleaned_df.columns)}")

    # Step 3: Load ideal schema
    schema_path = Path(source_path).parent.parent / "schemas" / "ideal_schema.yaml"
    try:
        ideal_schema = load_ideal_schema(str(schema_path))
        print(f"Loaded ideal schema from {schema_path}")
    except Exception as e:
        print(f"Warning: Could not load ideal schema: {e}")
        ideal_schema = {}

    # Step 4: Validate
    table_name = "users_clean"
    validation = validate_cleaned_data(cleaned_df, ideal_schema, table_name)
    print(f"\nValidation Results:")
    print(f"  Is Valid: {validation.get('is_valid', False)}")
    print(f"  Row Count: {validation.get('row_count', 0)}")
    print(f"  Issues Found: {len(validation.get('issues', []))}")
    
    if validation.get("issues"):
        print(f"\n  Issues:")
        for issue in validation["issues"]:
            print(f"    - {issue}")
    else:
        print(f"  All validations passed!")

    # Step 5: Save to DuckDB
    rows_saved = save_to_duckdb(cleaned_df, table_name)
    print(f"\nSaved {rows_saved} rows to {table_name}")

    return {
        "source_rows": len(df),
        "cleaned_rows": len(cleaned_df),
        "saved_rows": rows_saved,
        "validation": validation
    }


if __name__ == "__main__":
    result = clean_and_load_pipeline()
    print(f"\nPipeline result: {result}")
