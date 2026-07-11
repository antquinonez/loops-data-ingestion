"""
Pipeline Builder Tools
These tools enable the agent to analyze schemas, generate cleaning code, and create pipelines.
"""

import json
import yaml
import csv
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
SCHEMAS_DIR = PROJECT_ROOT / "schemas"
PIPELINES_DIR = PROJECT_ROOT / "pipelines"


def load_ideal_schema(schema_path: Optional[str] = None) -> Dict[str, Any]:
    """Load the ideal schema definition.
    
    Args:
        schema_path: Optional path to schema file. If not provided, uses the default
                    path (SCHEMAS_DIR / "ideal_schema.yaml").
    
    Returns:
        Dictionary with schema definition, or dict with "error" key on failure.
    """
    if schema_path is None:
        schema_path = SCHEMAS_DIR / "ideal_schema.yaml"
    else:
        schema_path = Path(schema_path)
    
    if not schema_path.exists():
        return {"error": f"Ideal schema not found: {schema_path}"}
    
    try:
        with open(schema_path, 'r') as f:
            schema = yaml.safe_load(f)
        return schema
    except Exception as e:
        return {"error": f"Failed to load ideal schema: {e}"}


def infer_source_schema(file_path: str, sample_size: int = 100) -> Dict[str, Any]:
    """Infer schema from source CSV file."""
    path = Path(file_path)
    if not path.exists():
        return {"error": f"File not found: {file_path}"}
    
    try:
        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        if not rows:
            return {"error": "No data in file"}
        
        # Infer schema from first few rows
        inferred_schema = {
            "file": str(path),
            "row_count": len(rows),
            "columns": {}
        }
        
        for col_name in rows[0].keys():
            # Sample values
            values = [row[col_name] for row in rows[:min(sample_size, len(rows))] if row[col_name] is not None]
            
            # Infer type
            col_type = _infer_type(values)
            
            # Check for nulls
            null_count = sum(1 for row in rows if row[col_name] is None or row[col_name] == '')
            
            inferred_schema["columns"][col_name] = {
                "type": col_type,
                "nullable": null_count > 0,
                "null_count": null_count,
                "null_percentage": null_count / len(rows) if rows else 0,
                "sample_values": values[:5]
            }
        
        return inferred_schema
        
    except Exception as e:
        return {"error": f"Failed to infer schema: {e}"}


def _infer_type(values: List[str]) -> str:
    """Infer the most likely type from a list of string values."""
    if not values:
        return "unknown"
    
    # Check if all values are integers
    try:
        if all(v.lstrip('-').isdigit() for v in values if v):
            return "integer"
    except:
        pass
    
    # Check if all values are floats
    try:
        if all(_is_float(v) for v in values if v):
            return "float"
    except:
        pass
    
    # Check if all values are dates
    try:
        from dateutil.parser import parse
        if all(_is_date(v) for v in values if v):
            return "date"
    except:
        pass
    
    # Default to string
    return "string"


def _is_float(s: str) -> bool:
    """Check if string can be parsed as float."""
    try:
        float(s)
        return True
    except:
        return False


def _is_date(s: str) -> bool:
    """Check if string can be parsed as date."""
    from dateutil.parser import parse
    try:
        parse(s)
        return True
    except:
        return False


def compare_schemas(source_path: str, ideal_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Compare source schema with ideal schema and identify mismatches.
    
    Returns:
        Dictionary with:
        - matches: columns that match
        - mismatches: columns with issues
        - missing: columns in ideal but not in source
        - extra: columns in source but not in ideal
        - recommendations: suggested transformations
    """
    # Load schemas
    if ideal_path is None:
        ideal_schema = load_ideal_schema()
    else:
        with open(ideal_path, 'r') as f:
            ideal_schema = yaml.safe_load(f)
    
    if isinstance(ideal_schema, dict) and 'error' in ideal_schema:
        return ideal_schema
    
    source_schema = infer_source_schema(source_path)
    if isinstance(source_schema, dict) and 'error' in source_schema:
        return source_schema
    
    # Get column definitions
    # Determine which table to use from the ideal schema
    if ideal_path:
        import os
        schema_filename = os.path.basename(ideal_path).replace('.yaml', '').replace('_schema', '')
        table_name = schema_filename
    else:
        table_name = "users"
    
    ideal_table = ideal_schema.get("tables", {}).get(table_name, {})
    if not ideal_table:
        # Try to find any table in the schema
        tables = ideal_schema.get("tables", {})
        if tables:
            table_name = list(tables.keys())[0]
            ideal_table = tables[table_name]
        else:
            return {"error": "No tables found in ideal schema"}
    
    ideal_cols = ideal_table.get("columns", [])
    source_cols = source_schema.get("columns", {})
    
    # Convert to name -> defn mapping
    ideal_by_name = {col["name"]: col for col in ideal_cols} if isinstance(ideal_cols, list) else ideal_cols
    source_by_name = source_cols
    
    # Find matches, mismatches, missing, extra
    comparison = {
        "source_file": source_schema.get("file"),
        "source_rows": source_schema.get("row_count"),
        "matches": [],
        "mismatches": [],
        "missing": [],
        "extra": [],
        "recommendations": []
    }
    
    all_ideal_names = set(ideal_by_name.keys())
    all_source_names = set(source_by_name.keys())
    
    # Columns in both
    common_cols = all_ideal_names & all_source_names
    
    for col_name in common_cols:
        ideal_def = ideal_by_name[col_name]
        source_def = source_by_name[col_name]
        
        # Check type match
        ideal_type = ideal_def.get("type", "string").lower()
        source_type = source_def.get("type", "string").lower()
        
        # Check nullability
        ideal_nullable = ideal_def.get("nullable", False)
        source_nullable = source_def.get("nullable", False)
        
        # Collect issues
        issues = []
        
        if ideal_type != source_type:
            issues.append({
                "type": "type_mismatch",
                "expected": ideal_type,
                "actual": source_type,
                "severity": "high"
            })
        
        if ideal_nullable != source_nullable:
            if source_nullable and not ideal_nullable:
                issues.append({
                    "type": "nullability_mismatch",
                    "expected": "NOT NULL",
                    "actual": "NULL allowed",
                    "severity": "high",
                    "null_count": source_def.get("null_count", 0)
                })
        
        if source_def.get("null_count", 0) > 0 and not ideal_nullable:
            issues.append({
                "type": "null_values_found",
                "count": source_def.get("null_count", 0),
                "percentage": source_def.get("null_percentage", 0),
                "severity": "high"
            })
        
        if issues:
            comparison["mismatches"].append({
                "column": col_name,
                "issues": issues,
                "ideal_type": ideal_type,
                "source_type": source_type,
                "sample_values": source_def.get("sample_values", [])
            })
            
            # Generate recommendation
            recommendation = _generate_recommendation(col_name, ideal_def, source_def, issues)
            comparison["recommendations"].append(recommendation)
        else:
            comparison["matches"].append(col_name)
    
    # Missing from source
    comparison["missing"] = list(all_ideal_names - all_source_names)
    
    # Extra in source
    comparison["extra"] = list(all_source_names - all_ideal_names)
    
    return comparison


def _generate_recommendation(column: str, ideal_def: Dict, source_def: Dict, issues: List) -> Dict[str, Any]:
    """Generate a transformation recommendation for a column."""
    recommendations = []
    
    for issue in issues:
        if issue["type"] == "type_mismatch":
            recommendations.append({
                "action": "cast",
                "from": issue["actual"],
                "to": issue["expected"],
                "fallback": ideal_def.get("default", None),
                "code": f"COALESCE(CAST({column} AS {issue['expected'].upper()}), {ideal_def.get('default', 'NULL')})"
            })
        
        elif issue["type"] == "nullability_mismatch":
            default = ideal_def.get("default")
            if default is not None:
                recommendations.append({
                    "action": "coalesce",
                    "column": column,
                    "default": default,
                    "code": f"COALESCE({column}, {json.dumps(default)})"
                })
        
        elif issue["type"] == "null_values_found":
            default = ideal_def.get("default")
            if default is not None:
                recommendations.append({
                    "action": "fill_nulls",
                    "column": column,
                    "count": issue["count"],
                    "default": default,
                    "code": f"COALESCE({column}, {json.dumps(default)})"
                })
    
    return {
        "column": column,
        "issues": issues,
        "recommendations": recommendations
    }


def generate_cleaning_pipeline(source_path: str, ideal_path: Optional[str] = None, 
                               output_table: str = "users_clean",
                               source_table: str = "raw_users") -> Dict[str, Any]:
    """
    Generate a complete cleaning pipeline based on schema comparison.
    
    Args:
        source_path: Path to source CSV file
        ideal_path: Path to ideal schema YAML file (optional)
        output_table: Name of the output table
        source_table: Name of the source table in DuckDB (default: raw_users)
    
    Returns:
        Dictionary with:
        - comparison: schema comparison results
        - pipeline_code: generated Python code for cleaning
        - sql_transformations: SQL transformations to apply
        - validation_queries: queries to validate the cleaning
    """
    # Compare schemas
    comparison = compare_schemas(source_path, ideal_path)
    
    if isinstance(comparison, dict) and 'error' in comparison:
        return comparison
    
    # Load ideal schema for defaults
    ideal_schema = load_ideal_schema() if ideal_path is None else yaml.safe_load(open(ideal_path))
    
    # Determine which table to use - if ideal_path is provided, extract table name from it
    # Otherwise use "users" as default
    if ideal_path:
        # Try to get table name from the file name or from the schema
        import os
        schema_filename = os.path.basename(ideal_path).replace('.yaml', '').replace('_schema', '')
        table_name = schema_filename
    else:
        table_name = "users"
    
    ideal_table = ideal_schema.get("tables", {}).get(table_name, {})
    if not ideal_table:
        # Try to find any table in the schema
        tables = ideal_schema.get("tables", {})
        if tables:
            table_name = list(tables.keys())[0]
            ideal_table = tables[table_name]
        else:
            return {"error": "No tables found in ideal schema"}
    
    ideal_cols = ideal_table.get("columns", [])
    ideal_by_name = {col["name"]: col for col in ideal_cols} if isinstance(ideal_cols, list) else ideal_cols
    
    # Generate SQL transformations
    sql_transformations = []
    
    for mismatch in comparison.get("mismatches", []):
        col_name = mismatch["column"]
        for issue in mismatch["issues"]:
            if issue["type"] == "null_values_found" and not ideal_by_name.get(col_name, {}).get("nullable", True):
                default = ideal_by_name[col_name].get("default")
                if default is not None:
                    if isinstance(default, str):
                        sql_transformations.append(f"COALESCE({col_name}, '{default}') AS {col_name}")
                    else:
                        sql_transformations.append(f"COALESCE({col_name}, {default}) AS {col_name}")
            elif issue["type"] == "type_mismatch":
                ideal_type = issue["expected"].upper()
                default = ideal_by_name[col_name].get("default")
                fallback = json.dumps(default) if isinstance(default, str) else str(default)
                sql_transformations.append(f"COALESCE(CAST({col_name} AS {ideal_type}), {fallback}) AS {col_name}")
    
    # For missing columns, add them with defaults
    for col_name in comparison.get("missing", []):
        default = ideal_by_name[col_name].get("default")
        if isinstance(default, str):
            sql_transformations.append(f"'{default}' AS {col_name}")
        else:
            sql_transformations.append(f"{default} AS {col_name}")
    
    # Generate the SELECT statement
    select_clause = ", ".join(sql_transformations)
    
    # Also include matched columns that don't need transformation
    for col_name in comparison.get("matches", []):
        select_clause += f", {col_name}"
    
    # Generate the cleaning SQL
    table_desc = ideal_table.get("description", f"{table_name} table")
    cleaning_sql = f"""
    -- Cleaning transformation for {table_desc}
    INSERT INTO {output_table} ({', '.join(ideal_by_name.keys())})
    SELECT {select_clause}
    FROM {source_table}
    """
    
    # Generate Python code for a Prefect flow
    python_code = _generate_prefect_cleaning_flow(
        source_path, output_table, comparison, ideal_by_name, ideal_path if ideal_path else ""
    )
    
    # Generate validation queries
    validation_queries = _generate_validation_queries(output_table, ideal_by_name)
    
    return {
        "comparison": comparison,
        "sql_transformations": sql_transformations,
        "cleaning_sql": cleaning_sql.strip(),
        "pipeline_code": python_code,
        "validation_queries": validation_queries,
        "generated_at": datetime.now().isoformat()
    }




def _generate_validation_queries(table: str, ideal_by_name: Dict) -> List[str]:
    """Generate SQL queries to validate cleaned data."""
    queries = []
    
    # Row count
    queries.append(f"SELECT COUNT(*) FROM {table}")
    
    # Null checks for non-nullable columns
    for col_name, col_def in ideal_by_name.items():
        if not col_def.get("nullable", True):
            queries.append(f"SELECT COUNT(*) FROM {table} WHERE {col_name} IS NULL")
    
    # Type checks
    for col_name, col_def in ideal_by_name.items():
        ideal_type = col_def.get("type", "string").upper()
        queries.append(f"SELECT COUNT(*) FROM {table} WHERE typeof({col_name}) != '{ideal_type}'")
    
    # Range checks
    for col_name, col_def in ideal_by_name.items():
        constraints = col_def.get("constraints", {})
        if "min" in constraints:
            queries.append(f"SELECT COUNT(*) FROM {table} WHERE {col_name} < {constraints['min']}")
        if "max" in constraints:
            queries.append(f"SELECT COUNT(*) FROM {table} WHERE {col_name} > {constraints['max']}")
    
    # Enum checks
    for col_name, col_def in ideal_by_name.items():
        if "enum" in col_def:
            enum_values = ", ".join([f"'{v}'" for v in col_def["enum"]])
            queries.append(f"SELECT COUNT(*) FROM {table} WHERE {col_name} NOT IN ({enum_values})")
    
    return queries


# Export all tools
PIPELINE_TOOLS = {
    "load_ideal_schema": {
        "description": "Load the ideal schema definition from YAML",
        "function": load_ideal_schema,
    },
    "infer_source_schema": {
        "description": "Infer schema from source CSV file",
        "function": infer_source_schema,
    },
    "compare_schemas": {
        "description": "Compare source and ideal schemas, identify mismatches",
        "function": compare_schemas,
    },
    "generate_cleaning_pipeline": {
        "description": "Generate complete cleaning pipeline (SQL + Python)",
        "function": generate_cleaning_pipeline,
    },
}

def _generate_prefect_cleaning_flow(source_path: str, output_table: str, 
                                    comparison: Dict, ideal_by_name: Dict, 
                                    schema_file: str = "") -> str:
    """Generate a Prefect flow for data cleaning."""
    
    # Build the transformation logic
    transformations = []
    
    # First, handle all columns that need transformation based on their ideal type
    for col_name, col_def in ideal_by_name.items():
        ideal_type = col_def.get("type", "string").lower()
        constraints = col_def.get("constraints", {})
        default = col_def.get("default")
        
        # For date columns, always convert to datetime
        if ideal_type == "date":
            if default is not None:
                transformations.append(
                    f"df['{col_name}'] = pd.to_datetime(df['{col_name}'], errors='coerce').fillna('{default}')"
                )
            else:
                transformations.append(
                    f"df['{col_name}'] = pd.to_datetime(df['{col_name}'], errors='coerce')"
                )
        # For integer columns with constraints, handle clamping
        elif ideal_type == "integer" and constraints:
            min_val = constraints.get("min")
            max_val = constraints.get("max")
            if default is not None:
                # Convert to numeric, fill nulls, then clamp
                transformations.append(
                    f"# Convert {col_name} to integer with clamping\n"
                    f"df['{col_name}_num'] = pd.to_numeric(df['{col_name}'], errors='coerce')\n"
                    f"df['{col_name}'] = df['{col_name}_num'].fillna({default}).astype(int)\n"
                )
                if min_val is not None:
                    transformations.append(
                        f"df['{col_name}'] = df['{col_name}'].clip(lower={min_val})\n"
                    )
                if max_val is not None:
                    transformations.append(
                        f"df['{col_name}'] = df['{col_name}'].clip(upper={max_val})\n"
                    )
                transformations.append(
                    f"df = df.drop(columns=['{col_name}_num'])"
                )
            else:
                transformations.append(
                    f"df['{col_name}'] = pd.to_numeric(df['{col_name}'], errors='coerce').astype('Int64')"
                )
        # For float columns with constraints, handle clamping
        elif ideal_type == "float" and constraints:
            min_val = constraints.get("min")
            max_val = constraints.get("max")
            if default is not None:
                transformations.append(
                    f"df['{col_name}'] = pd.to_numeric(df['{col_name}'], errors='coerce').fillna({default})"
                )
                if min_val is not None:
                    transformations.append(
                        f"df['{col_name}'] = df['{col_name}'].clip(lower={min_val})"
                    )
                if max_val is not None:
                    transformations.append(
                        f"df['{col_name}'] = df['{col_name}'].clip(upper={max_val})"
                    )
        
        # Handle enum constraints - enforce valid values
        if "enum" in col_def:
            valid_values = col_def["enum"]
            default_value = col_def.get("default")
            if default_value is None:
                default_value = valid_values[0] if valid_values else None
            # Create a set for fast lookup
            transformations.append(
                f"# Enforce enum constraint for {col_name}"
            )
            transformations.append(
                f"valid_{col_name} = set({valid_values})"
            )
            if isinstance(default_value, str):
                transformations.append(
                    f"df['{col_name}'] = df['{col_name}'].apply(lambda x: x if x in valid_{col_name} else '{default_value}')"
                )
            else:
                transformations.append(
                    f"df['{col_name}'] = df['{col_name}'].apply(lambda x: x if x in valid_{col_name} else {default_value})"
                )
        
        # Handle string length constraints
        if ideal_type == "string":
            if "min_length" in constraints:
                min_len = constraints["min_length"]
                transformations.append(
                    f"# Enforce min_length for {col_name}"
                )
                transformations.append(
                    f"df['{col_name}'] = df['{col_name}'].apply(lambda x: x if len(str(x)) >= {min_len} else x.ljust({min_len}))"
                )
            if "max_length" in constraints:
                max_len = constraints["max_length"]
                transformations.append(
                    f"# Enforce max_length for {col_name}"
                )
                transformations.append(
                    f"df['{col_name}'] = df['{col_name}'].str[:{max_len}]"
                )
        
        # Handle pattern constraints (email, etc.)
        if "pattern" in col_def:
            pattern = col_def["pattern"]
            default_value = col_def.get("default", "")
            transformations.append(
                f"# Enforce pattern for {col_name}"
            )
            transformations.append(
                f"import re"
            )
            transformations.append(
                f"pattern_{col_name} = re.compile(r'{pattern}')"
            )
            if isinstance(default_value, str):
                transformations.append(
                    f"df['{col_name}'] = df['{col_name}'].apply(lambda x: x if pattern_{col_name}.match(str(x)) else '{default_value}')"
                )
            else:
                transformations.append(
                    f"df['{col_name}'] = df['{col_name}'].apply(lambda x: x if pattern_{col_name}.match(str(x)) else {default_value})"
                )
        
        # Handle format constraints (email)
        if col_def.get("format") == "email":
            default_email = col_def.get("default", "unknown@example.com")
            transformations.append(
                f"# Enforce email format for {col_name}"
            )
            transformations.append(
                f"import re"
            )
            # Use a comprehensive email regex
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            transformations.append(
                f"email_pattern = re.compile(r'{email_pattern}')"
            )
            transformations.append(
                f"df['{col_name}'] = df['{col_name}'].apply(lambda x: x if email_pattern.match(str(x)) else '{default_email}')"
            )
    
    # Then handle mismatches specifically
    for mismatch in comparison.get("mismatches", []):
        col_name = mismatch["column"]
        for issue in mismatch["issues"]:
            if issue["type"] == "null_values_found":
                default = ideal_by_name[col_name].get("default")
                if default is not None:
                    if isinstance(default, str):
                        transformations.append(
                            f"df['{col_name}'] = df['{col_name}'].fillna('{default}')"
                        )
                    else:
                        transformations.append(
                            f"df['{col_name}'] = df['{col_name}'].fillna({default})"
                        )
            elif issue["type"] == "type_mismatch":
                ideal_type = issue["expected"]
                default = ideal_by_name[col_name].get("default")
                
                # Skip if already handled above
                if ideal_type == "date" or (ideal_type == "integer" and ideal_by_name[col_name].get("constraints")):
                    continue
                    
                if ideal_type == "integer":
                    transformations.append(
                        f"# Convert {col_name} to integer\n"
                        f"df['{col_name}_int'] = pd.to_numeric(df['{col_name}'], errors='coerce')\n"
                        f"df['{col_name}'] = df['{col_name}_int'].fillna({default}).astype(int)\n"
                        f"df = df.drop(columns=['{col_name}_int'])"
                    )
                elif ideal_type == "float":
                    transformations.append(
                        f"df['{col_name}'] = pd.to_numeric(df['{col_name}'], errors='coerce').fillna({default})"
                    )
                elif ideal_type == "date":
                    transformations.append(
                        f"df['{col_name}'] = pd.to_datetime(df['{col_name}'], errors='coerce').fillna('{default}')"
                    )
    
    # For missing columns
    for col_name in comparison.get("missing", []):
        default = ideal_by_name[col_name].get("default")
        if isinstance(default, str):
            transformations.append(f"df['{col_name}'] = '{default}'")
        else:
            transformations.append(f"df['{col_name}'] = {default}")
    
    # Add proper indentation to each line in multi-line transformations
    # Each line needs 4 spaces to be inside the clean_data function
    indented_transformations = []
    for t in transformations:
        # Split by newlines and indent each line properly
        lines = t.split("\n")
        indented_lines = []
        for i, line in enumerate(lines):
            if i == 0:
                # First line gets 4 spaces
                if line.strip():
                    indented_lines.append(f"    {line}")
            else:
                # Continuation lines get 4 spaces
                if line.strip():
                    indented_lines.append(f"    {line}")
        indented_transformations.append("\n".join(indented_lines))
    
    transformation_code = "\n".join(indented_transformations)
    
    # Load template and replace placeholders
    template_path = PROJECT_ROOT / "agents" / "pipeline_builder" / "flow_template_prefect.txt"
    with open(template_path, 'r') as f:
        template = f.read()
    
    # Replace placeholders
    flow_code = template.replace("__SOURCE_PATH__", source_path)
    flow_code = flow_code.replace("__OUTPUT_TABLE__", output_table)
    flow_code = flow_code.replace("{transformations}", transformation_code)
    
    # Extract schema file name from path
    import os
    if schema_file:
        schema_filename = os.path.basename(schema_file)
    else:
        # Default to users schema
        schema_filename = "users_schema.yaml"
    flow_code = flow_code.replace("__SCHEMA_FILE__", schema_filename)
    
    # Extract table name for flow name
    if schema_file:
        table_name_for_flow = os.path.basename(schema_file).replace('_schema.yaml', '').replace('.yaml', '')
    else:
        table_name_for_flow = output_table
    flow_code = flow_code.replace("__table_name__", table_name_for_flow)
    
    return flow_code

# Export all tools
PIPELINE_TOOLS = {
    "load_ideal_schema": {
        "description": "Load the ideal schema definition from YAML",
        "function": load_ideal_schema,
    },
    "infer_source_schema": {
        "description": "Infer schema from source CSV file",
        "function": infer_source_schema,
    },
    "compare_schemas": {
        "description": "Compare source and ideal schemas, identify mismatches",
        "function": compare_schemas,
    },
    "generate_cleaning_pipeline": {
        "description": "Generate complete cleaning pipeline (SQL + Python)",
        "function": generate_cleaning_pipeline,
    },
}
