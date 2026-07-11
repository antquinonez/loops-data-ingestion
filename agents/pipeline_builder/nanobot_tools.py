"""
Nanobot-compatible Tool classes for pipeline builder.
These are proper Tool subclasses that can be registered with nanobot's ToolRegistry.
"""

import json
import yaml
import csv
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

from nanobot.agent.tools.base import Tool, tool_parameters, ToolResult

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
SCHEMAS_DIR = PROJECT_ROOT / "schemas"
PIPELINES_DIR = PROJECT_ROOT / "pipelines"


class LoadIdealSchemaTool(Tool):
    """Load the ideal schema definition from YAML."""
    
    @property
    def name(self) -> str:
        return "load_ideal_schema"
    
    @property
    def description(self) -> str:
        return "Load the ideal schema definition from YAML file"
    
    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {},
            "additionalProperties": False
        }
    
    async def execute(self, **kwargs: Any) -> Any:
        """Load ideal schema."""
        schema_path = SCHEMAS_DIR / "ideal_schema.yaml"
        if not schema_path.exists():
            return ToolResult.error(f"Ideal schema not found: {schema_path}")
        
        try:
            with open(schema_path, 'r') as f:
                schema = yaml.safe_load(f)
            return schema
        except Exception as e:
            return ToolResult.error(f"Failed to load ideal schema: {e}")


class InferSourceSchemaTool(Tool):
    """Infer schema from source CSV file."""
    
    @property
    def name(self) -> str:
        return "infer_source_schema"
    
    @property
    def description(self) -> str:
        return "Infer schema from source CSV file by analyzing its content"
    
    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the source CSV file"
                },
                "sample_size": {
                    "type": "integer",
                    "default": 100,
                    "description": "Number of rows to sample for type inference"
                }
            },
            "required": ["file_path"]
        }
    
    async def execute(self, file_path: str, sample_size: int = 100, **kwargs: Any) -> Any:
        """Infer schema from CSV file."""
        path = Path(file_path)
        if not path.exists():
            return ToolResult.error(f"File not found: {file_path}")
        
        try:
            with open(path, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            if not rows:
                return ToolResult.error("No data in file")
            
            inferred_schema = {
                "file": str(path),
                "row_count": len(rows),
                "columns": {}
            }
            
            for col_name in rows[0].keys():
                values = [row[col_name] for row in rows[:min(sample_size, len(rows))] if row[col_name] is not None]
                col_type = self._infer_type(values)
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
            return ToolResult.error(f"Failed to infer schema: {e}")
    
    def _infer_type(self, values: List[str]) -> str:
        """Infer the most likely type from a list of string values."""
        if not values:
            return "unknown"
        
        try:
            if all(v.lstrip('-').isdigit() for v in values if v):
                return "integer"
        except:
            pass
        
        try:
            if all(self._is_float(v) for v in values if v):
                return "float"
        except:
            pass
        
        try:
            from dateutil.parser import parse
            if all(self._is_date(v) for v in values if v):
                return "date"
        except:
            pass
        
        return "string"
    
    def _is_float(self, s: str) -> bool:
        """Check if string can be parsed as float."""
        try:
            float(s)
            return True
        except:
            return False
    
    def _is_date(self, s: str) -> bool:
        """Check if string can be parsed as date."""
        from dateutil.parser import parse
        try:
            parse(s)
            return True
        except:
            return False


class CompareSchemasTool(Tool):
    """Compare source and ideal schemas and identify mismatches."""
    
    @property
    def name(self) -> str:
        return "compare_schemas"
    
    @property
    def description(self) -> str:
        return "Compare source schema with ideal schema, identify type mismatches, nullability issues, and missing/extra columns"
    
    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "source_path": {
                    "type": "string",
                    "description": "Path to the source CSV file"
                },
                "ideal_path": {
                    "type": "string",
                    "default": None,
                    "description": "Path to the ideal schema YAML file (optional, defaults to ideal_schema.yaml)"
                }
            },
            "required": ["source_path"]
        }
    
    async def execute(self, source_path: str, ideal_path: Optional[str] = None, **kwargs: Any) -> Any:
        """Compare schemas and return comparison result."""
        # Load ideal schema
        if ideal_path is None:
            ideal_schema = await LoadIdealSchemaTool().execute()
        else:
            try:
                with open(ideal_path, 'r') as f:
                    ideal_schema = yaml.safe_load(f)
            except Exception as e:
                return ToolResult.error(f"Failed to load ideal schema: {e}")
        
        if isinstance(ideal_schema, ToolResult) and ideal_schema.is_error:
            return ideal_schema
        
        # Infer source schema
        source_schema = await InferSourceSchemaTool().execute(file_path=source_path)
        if isinstance(source_schema, ToolResult) and source_schema.is_error:
            return source_schema
        
        # Get column definitions
        if ideal_path:
            import os
            schema_filename = os.path.basename(ideal_path).replace('.yaml', '').replace('_schema', '')
            table_name = schema_filename
        else:
            table_name = "users"
        
        ideal_table = ideal_schema.get("tables", {}).get(table_name, {})
        if not ideal_table:
            tables = ideal_schema.get("tables", {})
            if tables:
                table_name = list(tables.keys())[0]
                ideal_table = tables[table_name]
            else:
                return ToolResult.error("No tables found in ideal schema")
        
        ideal_cols = ideal_table.get("columns", [])
        source_cols = source_schema.get("columns", {})
        
        ideal_by_name = {col["name"]: col for col in ideal_cols} if isinstance(ideal_cols, list) else ideal_cols
        source_by_name = source_cols
        
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
        common_cols = all_ideal_names & all_source_names
        
        for col_name in common_cols:
            ideal_def = ideal_by_name[col_name]
            source_def = source_by_name[col_name]
            
            ideal_type = ideal_def.get("type", "string").lower()
            source_type = source_def.get("type", "string").lower()
            ideal_nullable = ideal_def.get("nullable", False)
            source_nullable = source_def.get("nullable", False)
            
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
                recommendation = self._generate_recommendation(col_name, ideal_def, source_def, issues)
                comparison["recommendations"].append(recommendation)
            else:
                comparison["matches"].append(col_name)
        
        comparison["missing"] = list(all_ideal_names - all_source_names)
        comparison["extra"] = list(all_source_names - all_ideal_names)
        
        return comparison
    
    def _generate_recommendation(self, column: str, ideal_def: Dict, source_def: Dict, issues: List) -> Dict[str, Any]:
        """Generate a transformation recommendation."""
        recommendations = []
        
        for issue in issues:
            if issue["type"] == "type_mismatch":
                default = ideal_def.get("default")
                fallback = json.dumps(default) if isinstance(default, str) else str(default) if default is not None else "NULL"
                recommendations.append({
                    "action": "cast",
                    "from": issue["actual"],
                    "to": issue["expected"],
                    "fallback": default,
                    "code": f"COALESCE(CAST({column} AS {issue['expected'].upper()}), {fallback})"
                })
            elif issue["type"] in ["nullability_mismatch", "null_values_found"]:
                default = ideal_def.get("default")
                if default is not None:
                    recommendations.append({
                        "action": "coalesce",
                        "column": column,
                        "default": default,
                        "code": f"COALESCE({column}, {json.dumps(default)})"
                    })
        
        return {
            "column": column,
            "issues": issues,
            "recommendations": recommendations
        }


class GenerateCleaningPipelineTool(Tool):
    """Generate a complete cleaning pipeline based on schema comparison."""
    
    @property
    def name(self) -> str:
        return "generate_cleaning_pipeline"
    
    @property
    def description(self) -> str:
        return "Generate complete data cleaning pipeline (SQL + Python code) based on schema comparison. Creates a Prefect flow that loads, cleans, and saves data."
    
    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "source_path": {
                    "type": "string",
                    "description": "Path to source CSV file"
                },
                "ideal_path": {
                    "type": "string",
                    "default": None,
                    "description": "Path to ideal schema YAML file (optional)"
                },
                "output_table": {
                    "type": "string",
                    "default": "users_clean",
                    "description": "Name of the output table in DuckDB"
                },
                "source_table": {
                    "type": "string",
                    "default": "raw_users",
                    "description": "Name of the source table in DuckDB (default: raw_users)"
                }
            },
            "required": ["source_path"]
        }
    
    async def execute(
        self, 
        source_path: str, 
        ideal_path: Optional[str] = None,
        output_table: str = "users_clean",
        source_table: str = "raw_users",
        **kwargs: Any
    ) -> Any:
        """Generate cleaning pipeline."""
        # Compare schemas first
        compare_tool = CompareSchemasTool()
        comparison = await compare_tool.execute(source_path=source_path, ideal_path=ideal_path)
        
        if isinstance(comparison, ToolResult) and comparison.is_error:
            return comparison
        
        # Load ideal schema
        if ideal_path is None:
            ideal_schema = await LoadIdealSchemaTool().execute()
        else:
            try:
                with open(ideal_path, 'r') as f:
                    ideal_schema = yaml.safe_load(f)
            except Exception as e:
                return ToolResult.error(f"Failed to load ideal schema: {e}")
        
        if isinstance(ideal_schema, ToolResult) and ideal_schema.is_error:
            return ideal_schema
        
        # Determine table name
        if ideal_path:
            import os
            schema_filename = os.path.basename(ideal_path).replace('.yaml', '').replace('_schema', '')
            table_name = schema_filename
        else:
            table_name = "users"
        
        ideal_table = ideal_schema.get("tables", {}).get(table_name, {})
        if not ideal_table:
            tables = ideal_schema.get("tables", {})
            if tables:
                table_name = list(tables.keys())[0]
                ideal_table = tables[table_name]
            else:
                return ToolResult.error("No tables found in ideal schema")
        
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
        
        # For missing columns
        for col_name in comparison.get("missing", []):
            default = ideal_by_name[col_name].get("default")
            if isinstance(default, str):
                sql_transformations.append(f"'{default}' AS {col_name}")
            else:
                sql_transformations.append(f"{default} AS {col_name}")
        
        # Generate SELECT statement
        select_clause = ", ".join(sql_transformations)
        for col_name in comparison.get("matches", []):
            select_clause += f", {col_name}"
        
        table_desc = ideal_table.get("description", f"{table_name} table")
        cleaning_sql = f"""
-- Cleaning transformation for {table_desc}
INSERT INTO {output_table} ({', '.join(ideal_by_name.keys())})
SELECT {select_clause}
FROM {source_table}
""".strip()
        
        # Generate Python pipeline code
        pipeline_code = self._generate_prefect_cleaning_flow(
            source_path, output_table, comparison, ideal_by_name, ideal_path if ideal_path else ""
        )
        
        # Generate validation queries
        validation_queries = self._generate_validation_queries(output_table, ideal_by_name)
        
        return {
            "comparison": comparison,
            "sql_transformations": sql_transformations,
            "cleaning_sql": cleaning_sql,
            "pipeline_code": pipeline_code,
            "validation_queries": validation_queries,
            "generated_at": datetime.now().isoformat()
        }
    
    def _generate_validation_queries(self, table: str, ideal_by_name: Dict) -> List[str]:
        """Generate SQL queries to validate cleaned data."""
        queries = []
        queries.append(f"SELECT COUNT(*) FROM {table}")
        
        for col_name, col_def in ideal_by_name.items():
            if not col_def.get("nullable", True):
                queries.append(f"SELECT COUNT(*) FROM {table} WHERE {col_name} IS NULL")
            
            ideal_type = col_def.get("type", "string").upper()
            queries.append(f"SELECT COUNT(*) FROM {table} WHERE typeof({col_name}) != '{ideal_type}'")
            
            if "min" in col_def:
                queries.append(f"SELECT COUNT(*) FROM {table} WHERE {col_name} < {col_def['min']}")
            if "max" in col_def:
                queries.append(f"SELECT COUNT(*) FROM {table} WHERE {col_name} > {col_def['max']}")
            
            if "enum" in col_def:
                enum_values = ", ".join([f"'{v}'" for v in col_def["enum"]])
                queries.append(f"SELECT COUNT(*) FROM {table} WHERE {col_name} NOT IN ({enum_values})")
        
        return queries
    
    def _generate_prefect_cleaning_flow(
        self, source_path: str, output_table: str, comparison: Dict, 
        ideal_by_name: Dict, schema_file: str = ""
    ) -> str:
        """Generate a Prefect flow for data cleaning."""
        # This is a simplified version that creates a stand-alone Python script
        # For now, return a simple DuckDB-based cleaning script
        
        # Build transformation logic
        transformations = []
        
        for col_name, col_def in ideal_by_name.items():
            ideal_type = col_def.get("type", "string").lower()
            default = col_def.get("default")
            
            if ideal_type == "date":
                if default is not None:
                    transformations.append(
                        f"df['{col_name}'] = pd.to_datetime(df['{col_name}'], errors='coerce').fillna('{default}')"
                    )
                else:
                    transformations.append(
                        f"df['{col_name}'] = pd.to_datetime(df['{col_name}'], errors='coerce')"
                    )
            elif ideal_type == "integer":
                if default is not None:
                    transformations.append(
                        f"df['{col_name}'] = pd.to_numeric(df['{col_name}'], errors='coerce').fillna({default}).astype(int)"
                    )
                else:
                    transformations.append(
                        f"df['{col_name}'] = pd.to_numeric(df['{col_name}'], errors='coerce').astype('Int64')"
                    )
            elif ideal_type == "float":
                if default is not None:
                    transformations.append(
                        f"df['{col_name}'] = pd.to_numeric(df['{col_name}'], errors='coerce').fillna({default})"
                    )
                else:
                    transformations.append(
                        f"df['{col_name}'] = pd.to_numeric(df['{col_name}'], errors='coerce')"
                    )
        
        # Handle mismatches
        for mismatch in comparison.get("mismatches", []):
            col_name = mismatch["column"]
            for issue in mismatch["issues"]:
                if issue["type"] == "null_values_found":
                    default = ideal_by_name[col_name].get("default")
                    if default is not None:
                        if isinstance(default, str):
                            transformations.append(f"df['{col_name}'] = df['{col_name}'].fillna('{default}')")
                        else:
                            transformations.append(f"df['{col_name}'] = df['{col_name}'].fillna({default})")
        
        # Handle missing columns
        for col_name in comparison.get("missing", []):
            default = ideal_by_name[col_name].get("default")
            if isinstance(default, str):
                transformations.append(f"df['{col_name}'] = '{default}'")
            else:
                transformations.append(f"df['{col_name}'] = {default}")
        
        transformation_code = "\n    ".join(transformations)
        
        # Use the local flow template
        template_path = PROJECT_ROOT / "agents" / "pipeline_builder" / "flow_template_local.txt"
        try:
            with open(template_path, 'r') as f:
                template = f.read()
            
            # Replace placeholders in the template file
            pipeline_code = template.replace("__SOURCE_PATH__", source_path)
            pipeline_code = pipeline_code.replace("__OUTPUT_TABLE__", output_table)
            pipeline_code = pipeline_code.replace("{transformations}", transformation_code)
        except FileNotFoundError:
            # Fallback to simple template
            pipeline_code = f'''import pandas as pd
import duckdb
from pathlib import Path

def load_source_data(source_path: str):
    """Load source CSV into DataFrame."""
    return pd.read_csv(source_path)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean data according to ideal schema."""
    df = df.copy()
    {transformation_code}
    return df

def save_to_duckdb(df: pd.DataFrame, table_name: str) -> int:
    """Save cleaned data to DuckDB."""
    conn = duckdb.connect(database="data/ingestion.db")
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    row_count = len(df)
    conn.close()
    return row_count

def clean_and_load_pipeline(source_path: str = "{source_path}"):
    """Complete cleaning and loading pipeline."""
    df = load_source_data(source_path)
    print(f"Loaded {{len(df)}} rows from {{source_path}}")
    cleaned_df = clean_data(df)
    print(f"Cleaned data: {{len(cleaned_df)}} rows")
    rows_saved = save_to_duckdb(cleaned_df, "{output_table}")
    print(f"Saved {{rows_saved}} rows to {output_table}")
    return {{"source_rows": len(df), "cleaned_rows": len(cleaned_df), "saved_rows": rows_saved}}

if __name__ == "__main__":
    result = clean_and_load_pipeline()
    print(f"Pipeline result: {{result}}")
'''
        
        return pipeline_code


# Export the Tool classes
PIPELINE_TOOL_CLASSES = [
    LoadIdealSchemaTool,
    InferSourceSchemaTool,
    CompareSchemasTool,
    GenerateCleaningPipelineTool,
]
