"""
Validation Agent for pipeline execution validation.

This agent validates pipeline execution results by comparing cleaned data
against the ideal schema. It uses deterministically generated checks that
are stored with each pipeline and executed after every pipeline run.

The ValidationAgent:
1. Loads validation checks generated from the ideal schema
2. Executes those checks against the database
3. Generates a ValidationReport with results
4. Saves the report for audit/debugging purposes
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

import duckdb

from utils.paths import paths
from utils.validation import (
    ValidationCheck,
    CheckResult,
    ValidationReport,
    save_validation_report,
    ValidationCheckGenerator,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = paths.get_abs("project_root")
VALIDATION_DIR = paths.pipelines_dir / "validation"


class ValidationAgent:
    """
    Agent that validates pipeline execution results.
    
    This agent runs deterministic validation checks against cleaned data
    to ensure it conforms to the ideal schema. Checks are generated once
    per pipeline from the schema and stored alongside the pipeline.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the ValidationAgent.
        
        Args:
            db_path: Path to DuckDB database. Defaults to project database.
        """
        self.db_path = db_path or str(paths.database)
        self.duckdb_conn = None
        
    def __enter__(self):
        """Context manager entry - connect to database."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close database connection."""
        self.close()
        
    def connect(self):
        """Connect to DuckDB database."""
        if self.duckdb_conn is None:
            self.duckdb_conn = duckdb.connect(database=self.db_path, read_only=True)
            logger.info(f"Connected to DuckDB at {self.db_path}")
        return self.duckdb_conn
        
    def close(self):
        """Close database connection."""
        if self.duckdb_conn is not None:
            self.duckdb_conn.close()
            self.duckdb_conn = None
            logger.info("DuckDB connection closed")
        
    def _execute_query(self, query: str) -> Any:
        """
        Execute a SQL query and return the result.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query result (scalar, list, or dict depending on query)
        """
        if self.duckdb_conn is None:
            self.connect()
            
        try:
            cursor = self.duckdb_conn.execute(query)
            result = cursor.fetchall()
            
            # If result is a single row with single column, return the value
            if len(result) == 1 and len(result[0]) == 1:
                return result[0][0]
            return result
        except Exception as e:
            logger.error(f"Query execution failed: {query}\nError: {e}")
            raise
            
    def _evaluate_check(self, check: ValidationCheck) -> CheckResult:
        """
        Execute a single validation check and return the result.
        
        Args:
            check: ValidationCheck to execute
            
        Returns:
            CheckResult with execution outcome
        """
        start_time = time.time()
        
        try:
            if check.check_type == "sql":
                result = self._execute_query(check.query)
                
                # Evaluate the result against expected
                if check.expected is not None:
                    expected = check.expected
                    
                    # Handle different comparison types
                    if isinstance(expected, str) and expected.startswith(">"):
                        # Greater than comparison
                        threshold = int(expected[1:].strip())
                        if result is not None and int(result) > threshold:
                            status = "PASS"
                        else:
                            status = "FAIL"
                            
                    elif isinstance(expected, str) and expected.startswith("<"):
                        # Less than comparison
                        threshold = int(expected[1:].strip())
                        if result is not None and int(result) < threshold:
                            status = "PASS"
                        else:
                            status = "FAIL"
                            
                    elif isinstance(expected, str) and expected.startswith("="):
                        # Equality comparison (special case: "= 0")
                        threshold = int(expected[1:].strip())
                        if result is not None and int(result) == threshold:
                            status = "PASS"
                        else:
                            status = "FAIL"
                    elif expected == 0:
                        # Most common: expect 0 violations
                        if result == 0:
                            status = "PASS"
                        else:
                            status = "FAIL"
                    else:
                        # Direct equality
                        if result == expected:
                            status = "PASS"
                        else:
                            status = "FAIL"
                else:
                    # No expected value, just check if query succeeded
                    status = "PASS"
                    
                actual = result
                details = ""
                error = None
                
            elif check.check_type == "row_count":
                # Execute query and check it's > 0
                result = self._execute_query(check.query)
                if result is not None and int(result) > 0:
                    status = "PASS"
                else:
                    status = "FAIL"
                actual = result
                details = ""
                error = None
                
            else:
                # For unknown check types, skip with warning
                status = "PASS"
                actual = None
                details = f"Unknown check type: {check.check_type}"
                error = None
                logger.warning(f"Unknown check type: {check.check_type}")
                
        except Exception as e:
            status = "ERROR"
            actual = None
            details = ""
            error = str(e)
            logger.error(f"Check {check.id} failed with error: {e}")
            
        duration_ms = (time.time() - start_time) * 1000
        
        return CheckResult(
            check_id=check.id,
            description=check.description,
            check_type=check.check_type,
            severity=check.severity,
            status=status,
            actual=actual,
            expected=check.expected,
            details=details,
            duration_ms=duration_ms,
            error=error
        )
        
    def validate_pipeline(
        self,
        pipeline_name: str,
        output_table: str,
        checks: List[ValidationCheck],
        save_report: bool = True,
        report_path: Optional[str] = None
    ) -> ValidationReport:
        """
        Validate a pipeline's output by running all associated checks.
        
        Args:
            pipeline_name: Name of the pipeline being validated
            output_table: Name of the output table to validate
            checks: List of ValidationCheck objects to execute
            save_report: Whether to save the report to disk
            report_path: Optional path to save report (defaults to validation dir)
            
        Returns:
            ValidationReport with all check results
        """
        executed_at = datetime.now()
        start_time = time.time()
        
        logger.info(f"Validating pipeline '{pipeline_name}' against table '{output_table}'")
        logger.info(f"Running {len(checks)} validation checks")
        
        # Get row counts for source and output tables
        source_row_count = None
        output_row_count = None
        
        try:
            # Try to get row count from raw_users or source table
            source_tables = ["raw_users", f"raw_{output_table}", output_table.replace("_clean", "")]
            for table in source_tables:
                try:
                    count = self._execute_query(f"SELECT COUNT(*) FROM {table}")
                    if count is not None:
                        source_row_count = int(count)
                        break
                except:
                    continue
                    
            # Get output table row count
            output_row_count = int(self._execute_query(f"SELECT COUNT(*) FROM {output_table}"))
        except Exception as e:
            logger.warning(f"Could not get row counts: {e}")
            
        # Execute all checks
        check_results = []
        checks_passed = 0
        checks_failed = 0
        checks_errored = 0
        
        for check in checks:
            result = self._evaluate_check(check)
            check_results.append(result)
            
            if result.status == "PASS":
                checks_passed += 1
                logger.debug(f"✓ {check.id}: PASS")
            elif result.status == "FAIL":
                checks_failed += 1
                logger.warning(f"✗ {check.id}: FAIL (expected {check.expected}, got {result.actual})")
            else:  # ERROR
                checks_errored += 1
                logger.error(f"⚠ {check.id}: ERROR - {result.error}")
                
        total_checks = len(checks)
        duration_seconds = time.time() - start_time
        
        # Determine overall status
        if checks_failed > 0:
            overall_status = "FAIL"
        elif checks_errored > 0:
            overall_status = "WARN"
        else:
            overall_status = "PASS"
            
        # Create report
        report = ValidationReport(
            pipeline_name=pipeline_name,
            output_table=output_table,
            executed_at=executed_at,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            checks_errored=checks_errored,
            total_checks=total_checks,
            overall_status=overall_status,
            check_results=check_results,
            duration_seconds=duration_seconds,
            source_row_count=source_row_count,
            output_row_count=output_row_count
        )
        
        # Save report
        if save_report:
            if report_path is None:
                # Ensure validation directory exists
                VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                report_path = str(VALIDATION_DIR / f"validation_{pipeline_name}_{timestamp}.json")
                
            save_validation_report(report, report_path)
            logger.info(f"Validation report saved to: {report_path}")
            
        # Log summary
        logger.info(f"Validation complete: {checks_passed} passed, {checks_failed} failed, {checks_errored} errored")
        logger.info(f"Overall status: {overall_status}")
        
        return report
        
    def load_checks(self, checks_path: str) -> List[ValidationCheck]:
        """
        Load validation checks from a JSON file.
        
        Args:
            checks_path: Path to the checks JSON file
            
        Returns:
            List of ValidationCheck objects
        """
        path = Path(checks_path)
        if not path.exists():
            raise FileNotFoundError(f"Checks file not found: {checks_path}")
            
        with open(path, 'r') as f:
            checks_data = json.load(f)
            
        checks = [ValidationCheck.from_dict(check_data) for check_data in checks_data]
        logger.info(f"Loaded {len(checks)} validation checks from {checks_path}")
        return checks
        
    def save_checks(self, checks: List[ValidationCheck], checks_path: str) -> str:
        """
        Save validation checks to a JSON file.
        
        Args:
            checks: List of ValidationCheck objects to save
            checks_path: Path to save the checks file
            
        Returns:
            Path to the saved checks file
        """
        path = Path(checks_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        checks_data = [check.to_dict() for check in checks]
        
        with open(path, 'w') as f:
            json.dump(checks_data, f, indent=2, default=str)
            
        logger.info(f"Saved {len(checks)} validation checks to {checks_path}")
        return checks_path
        
    def generate_and_save_checks(
        self,
        schema_path: str,
        table_name: str,
        checks_path: str
    ) -> List[ValidationCheck]:
        """
        Generate validation checks from a schema and save them.
        
        This is typically called once when a pipeline is first created,
        to generate the deterministic checks that will be used for all
        future validations of that pipeline.
        
        Args:
            schema_path: Path to the ideal schema YAML file
            table_name: Name of the table in the schema
            checks_path: Path to save the generated checks
            
        Returns:
            List of generated ValidationCheck objects
        """
        checks = ValidationCheckGenerator.generate_from_schema(
            schema_path=schema_path,
            table_name=table_name
        )
        
        self.save_checks(checks, checks_path)
        
        logger.info(f"Generated and saved {len(checks)} checks for table '{table_name}'")
        return checks


def get_checks_path(pipeline_name: str, output_table: str) -> Path:
    """
    Get the standard path for validation checks for a pipeline.
    
    Args:
        pipeline_name: Name of the pipeline
        output_table: Name of the output table
        
    Returns:
        Path to the checks JSON file
    """
    # Use output table name as the primary identifier
    table_base = output_table.replace("_clean", "")
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    return VALIDATION_DIR / f"{table_base}_validation_checks.json"


def validate_pipeline_output(
    pipeline_name: str,
    output_table: str,
    schema_path: str,
    db_path: Optional[str] = None,
    checks_path: Optional[str] = None
) -> ValidationReport:
    """
    Convenience function to validate a pipeline's output.
    
    This function:
    1. Generates or loads validation checks from schema
    2. Runs validation against the database
    3. Returns the validation report
    
    Args:
        pipeline_name: Name of the pipeline
        output_table: Name of the output table in database
        schema_path: Path to the ideal schema YAML file
        db_path: Optional path to DuckDB database
        checks_path: Optional path to existing checks file
        
    Returns:
        ValidationReport with results
    """
    agent = ValidationAgent(db_path=db_path)
    
    try:
        agent.connect()
        
        if checks_path:
            # Load existing checks
            checks = agent.load_checks(checks_path)
        else:
            # Generate checks from schema
            standard_checks_path = get_checks_path(pipeline_name, output_table)
            if standard_checks_path.exists():
                checks = agent.load_checks(str(standard_checks_path))
            else:
                # Generate new checks
                checks = ValidationCheckGenerator.generate_from_schema(
                    schema_path=schema_path,
                    table_name=output_table.replace("_clean", "")
                )
                # Save for future use
                agent.save_checks(checks, str(standard_checks_path))
                
        # Run validation
        report = agent.validate_pipeline(
            pipeline_name=pipeline_name,
            output_table=output_table,
            checks=checks
        )
        
        return report
        
    finally:
        agent.close()
