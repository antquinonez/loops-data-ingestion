"""
Validation data models and utilities for pipeline execution validation.

This module provides structured data models for validation checks and reports,
used by the ValidationAgent to validate pipeline execution results.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field
import json


class ValidationCheck(BaseModel):
    """
    A single validation check to run against cleaned data.
    
    Validation checks are generated from the ideal schema and stored with each pipeline.
    They are executed deterministically after each pipeline execution.
    """
    id: str = Field(..., description="Unique identifier for this check")
    description: str = Field(..., description="Human-readable description of what this check validates")
    check_type: str = Field(..., description="Type of check: 'sql', 'row_count', 'null_check', 'type_check', etc.")
    query: Optional[str] = Field(default=None, description="SQL query to execute for sql-type checks")
    code: Optional[str] = Field(default=None, description="Python code for python-type checks")
    expected: Optional[Any] = Field(default=None, description="Expected result/value for the check")
    severity: str = Field(default="high", description="Severity level: 'low', 'medium', 'high', 'critical'")
    column: Optional[str] = Field(default=None, description="Column name this check applies to (if applicable)")
    table: Optional[str] = Field(default=None, description="Table name this check applies to")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return self.dict(exclude_none=True)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ValidationCheck':
        """Create from dictionary."""
        return cls(**data)


class CheckResult(BaseModel):
    """Result of executing a single validation check."""
    check_id: str = Field(..., description="ID of the check that was executed")
    description: str = Field(..., description="Description of the check")
    check_type: str = Field(..., description="Type of check")
    severity: str = Field(..., description="Severity level")
    status: str = Field(..., description="PASS, FAIL, or ERROR")
    actual: Optional[Any] = Field(default=None, description="Actual result from the check")
    expected: Optional[Any] = Field(default=None, description="Expected result")
    details: str = Field(default="", description="Additional details about the result")
    duration_ms: float = Field(default=0.0, description="Time taken to execute check in milliseconds")
    error: Optional[str] = Field(default=None, description="Error message if status is ERROR")


class ValidationReport(BaseModel):
    """
    Complete report from validating a pipeline's output.
    
    This report contains results of all validation checks run against
    the cleaned data produced by a pipeline execution.
    """
    pipeline_name: str = Field(..., description="Name of the pipeline that was validated")
    output_table: str = Field(..., description="Name of the output table that was validated")
    executed_at: datetime = Field(..., description="When the validation was executed")
    checks_passed: int = Field(..., description="Number of checks that passed")
    checks_failed: int = Field(..., description="Number of checks that failed")
    checks_errored: int = Field(default=0, description="Number of checks that errored")
    total_checks: int = Field(..., description="Total number of checks executed")
    overall_status: str = Field(..., description="Overall status: PASS, FAIL, or WARN")
    check_results: List[CheckResult] = Field(default_factory=list, description="Results of all individual checks")
    duration_seconds: float = Field(..., description="Total time to execute all checks")
    source_row_count: Optional[int] = Field(default=None, description="Row count in source table")
    output_row_count: Optional[int] = Field(default=None, description="Row count in output table")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return self.dict(exclude_none=True)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ValidationReport':
        """Create from dictionary."""
        return cls(**data)
    
    def get_failed_checks(self) -> List[CheckResult]:
        """Get list of failed checks."""
        return [r for r in self.check_results if r.status == 'FAIL']
    
    def get_errored_checks(self) -> List[CheckResult]:
        """Get list of errored checks."""
        return [r for r in self.check_results if r.status == 'ERROR']
    
    def get_critical_failures(self) -> List[CheckResult]:
        """Get list of checks with critical severity that failed."""
        return [r for r in self.check_results 
                if r.status == 'FAIL' and r.severity == 'critical']
    
    def print_summary(self):
        """Print a formatted summary of the validation report."""
        print("\n" + "=" * 80)
        print(f"VALIDATION REPORT: {self.pipeline_name}")
        print("=" * 80)
        print(f"Output Table: {self.output_table}")
        print(f"Executed at: {self.executed_at}")
        print(f"Duration: {self.duration_seconds:.2f}s")
        print(f"Overall Status: {self.overall_status}")
        print(f"Results: {self.checks_passed} passed, {self.checks_failed} failed, {self.checks_errored} errored")
        
        if self.source_row_count is not None and self.output_row_count is not None:
            print(f"Row Counts: Source={self.source_row_count}, Output={self.output_row_count}")
        
        if self.checks_failed > 0:
            print("\nFailed Checks:")
            for check in self.get_failed_checks():
                print(f"  ❌ {check.check_id}")
                print(f"     Description: {check.description}")
                print(f"     Expected: {check.expected}, Actual: {check.actual}")
                print(f"     Severity: {check.severity}")
                if check.details:
                    print(f"     Details: {check.details}")
        
        if self.checks_errored > 0:
            print("\nErrored Checks:")
            for check in self.get_errored_checks():
                print(f"  ⚠️  {check.check_id}: {check.error}")


def save_validation_report(report: ValidationReport, output_path: str = None) -> str:
    """
    Save a validation report to a JSON file.
    
    Args:
        report: The validation report to save
        output_path: Optional path for the output file
                    Defaults to logs/validation_{pipeline_name}_{timestamp}.json
    
    Returns:
        Path to the saved report file
    """
    from utils.paths import paths
    import os
    from datetime import datetime
    
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = str(paths.logs_dir / f"validation_{report.pipeline_name}_{timestamp}.json")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(report.to_dict(), f, indent=2, default=str)
    
    return output_path


def load_validation_report(file_path: str) -> ValidationReport:
    """
    Load a validation report from a JSON file.
    
    Args:
        file_path: Path to the report file
    
    Returns:
        ValidationReport object
    """
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    return ValidationReport.from_dict(data)


class ValidationCheckGenerator:
    """
    Utility class to generate validation checks from a schema.
    
    This can be used by the pipeline builder to generate checks
    that are stored with each pipeline.
    """
    
    @staticmethod
    def generate_from_schema(schema_path: str, table_name: str, output_table_name: Optional[str] = None) -> List[ValidationCheck]:
        """
        Generate validation checks from an ideal schema file.
        
        Args:
            schema_path: Path to the ideal schema YAML file
            table_name: Name of the table in the schema (for looking up schema definition)
            output_table_name: Optional actual table name to use in SQL queries.
                             If not provided, uses table_name.
            
        Returns:
            List of ValidationCheck objects
        """
        import yaml
        from pathlib import Path
        
        checks = []
        
        # Load schema
        with open(schema_path, 'r') as f:
            schema = yaml.safe_load(f)
        
        # Get table schema - find the matching table in the schema
        tables = schema.get('tables', {})
        schema_table_name = None
        
        # First try exact match
        if table_name in tables:
            schema_table_name = table_name
        else:
            # Try to find the table by name pattern
            for tname, tschema in tables.items():
                if table_name in tname or tname in table_name:
                    schema_table_name = tname
                    break
            else:
                raise ValueError(f"Table {table_name} not found in schema")
        
        # Use output_table_name for SQL queries if provided, otherwise use schema table name
        query_table_name = output_table_name if output_table_name else schema_table_name
        
        table_schema = tables[schema_table_name]
        columns = table_schema.get('columns', [])
        
        # Row count check
        checks.append(ValidationCheck(
            id=f"{query_table_name}_row_count",
            description=f"Verify {query_table_name} has data",
            check_type="sql",
            query=f"SELECT COUNT(*) FROM {query_table_name}",
            expected="> 0",
            severity="medium",
            table=query_table_name
        ))
        
        # For each column, generate appropriate checks
        for col in columns:
            col_name = col.get('name')
            col_type = col.get('type', 'string')
            constraints = col.get('constraints', {})
            nullable = col.get('nullable', True)
            col_format = col.get('format')
            col_enum = col.get('enum')
            
            if not col_name:
                continue
            
            # NULL check for NOT NULL columns
            if not nullable:
                checks.append(ValidationCheck(
                    id=f"{query_table_name}_{col_name}_not_null",
                    description=f"Verify {col_name} has no NULL values (NOT NULL constraint)",
                    check_type="sql",
                    query=f"SELECT COUNT(*) FROM {query_table_name} WHERE {col_name} IS NULL",
                    expected=0,
                    severity="critical",
                    column=col_name,
                    table=query_table_name
                ))
            
            # Type check
            duckdb_type = _map_schema_type_to_duckdb(col_type)
            checks.append(ValidationCheck(
                id=f"{query_table_name}_{col_name}_type_check",
                description=f"Verify {col_name} has correct type ({col_type})",
                check_type="sql",
                query=f"SELECT COUNT(*) FROM {query_table_name} WHERE typeof({col_name}) != '{duckdb_type}'",
                expected=0,
                severity="critical",
                column=col_name,
                table=query_table_name
            ))
            
            # Minimum value check
            if 'min' in constraints:
                min_val = constraints['min']
                checks.append(ValidationCheck(
                    id=f"{query_table_name}_{col_name}_min",
                    description=f"Verify {col_name} >= {min_val}",
                    check_type="sql",
                    query=f"SELECT COUNT(*) FROM {query_table_name} WHERE {col_name} < {min_val}",
                    expected=0,
                    severity="high",
                    column=col_name,
                    table=query_table_name
                ))
            
            # Maximum value check
            if 'max' in constraints:
                max_val = constraints['max']
                checks.append(ValidationCheck(
                    id=f"{query_table_name}_{col_name}_max",
                    description=f"Verify {col_name} <= {max_val}",
                    check_type="sql",
                    query=f"SELECT COUNT(*) FROM {query_table_name} WHERE {col_name} > {max_val}",
                    expected=0,
                    severity="high",
                    column=col_name,
                    table=query_table_name
                ))
            
            # Enum check
            if col_enum:
                enum_values = ", ".join([f"'{v}'" for v in col_enum])
                checks.append(ValidationCheck(
                    id=f"{query_table_name}_{col_name}_enum",
                    description=f"Verify {col_name} is in valid enum values",
                    check_type="sql",
                    query=f"SELECT COUNT(*) FROM {query_table_name} WHERE {col_name} NOT IN ({enum_values})",
                    expected=0,
                    severity="high",
                    column=col_name,
                    table=query_table_name
                ))
            
            # Email format check
            if col_format == 'email':
                checks.append(ValidationCheck(
                    id=f"{query_table_name}_{col_name}_email_format",
                    description=f"Verify {col_name} has valid email format",
                    check_type="sql",
                    query=f"""
                        SELECT COUNT(*) FROM {query_table_name} 
                        WHERE {col_name} IS NOT NULL 
                        AND {col_name} NOT LIKE '%_@_%._%'
                    """,
                    expected=0,
                    severity="medium",
                    column=col_name,
                    table=query_table_name
                ))
            
            # Date format check
            if col_type == 'date':
                checks.append(ValidationCheck(
                    id=f"{query_table_name}_{col_name}_date_valid",
                    description=f"Verify {col_name} contains valid dates",
                    check_type="sql",
                    query=f"SELECT COUNT(*) FROM {query_table_name} WHERE {col_name} IS NOT NULL AND typeof({col_name}) NOT IN ('DATE', 'TIMESTAMP')",
                    expected=0,
                    severity="high",
                    column=col_name,
                    table=query_table_name
                ))
            
            # String length checks
            if col_type == 'string':
                if 'min_length' in constraints:
                    min_len = constraints['min_length']
                    checks.append(ValidationCheck(
                        id=f"{query_table_name}_{col_name}_min_length",
                        description=f"Verify {col_name} length >= {min_len}",
                        check_type="sql",
                        query=f"SELECT COUNT(*) FROM {query_table_name} WHERE LENGTH({col_name}) < {min_len}",
                        expected=0,
                        severity="medium",
                        column=col_name,
                        table=query_table_name
                    ))
                if 'max_length' in constraints:
                    max_len = constraints['max_length']
                    checks.append(ValidationCheck(
                        id=f"{query_table_name}_{col_name}_max_length",
                        description=f"Verify {col_name} length <= {max_len}",
                        check_type="sql",
                        query=f"SELECT COUNT(*) FROM {query_table_name} WHERE LENGTH({col_name}) > {max_len}",
                        expected=0,
                        severity="medium",
                        column=col_name,
                        table=query_table_name
                    ))
        
        return checks


def _map_schema_type_to_duckdb(schema_type: str) -> str:
    """Map schema type to DuckDB type for validation.
    
    Note: This maps to the types returned by DuckDB's typeof() function,
    which may differ from the declared type in information_schema.
    """
    type_mapping = {
        'integer': 'INTEGER',
        'int': 'INTEGER',
        'bigint': 'INTEGER',
        'float': 'FLOAT',
        'double': 'FLOAT',
        'string': 'VARCHAR',
        'text': 'VARCHAR',
        'date': 'TIMESTAMP',  # DuckDB stores dates as TIMESTAMP with time 00:00:00
        'datetime': 'TIMESTAMP',
        'boolean': 'BOOLEAN',
        'bool': 'BOOLEAN',
    }
    return type_mapping.get(schema_type.lower(), 'VARCHAR')
