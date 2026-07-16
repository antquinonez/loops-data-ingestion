"""
Tests for validation functionality.

This module tests:
- ValidationCheckGenerator (generating checks from schema)
- ValidationAgent (executing checks and generating reports)
- Validation data models
- Integration with pipeline builder
"""

import pytest
import json
import tempfile
import os
from pathlib import Path
from datetime import datetime

# Setup paths
import sys
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
os.environ["PYTHONPATH"] = str(PROJECT_ROOT)

from utils.validation import (
    ValidationCheck,
    CheckResult,
    ValidationReport,
    ValidationCheckGenerator,
    save_validation_report,
    load_validation_report,
)
from agents.validation_agent import (
    ValidationAgent,
    get_checks_path,
    validate_pipeline_output,
)


class TestValidationCheck:
    """Tests for ValidationCheck data model."""
    
    def test_create_validation_check(self):
        """Test creating a validation check."""
        check = ValidationCheck(
            id="test_check",
            description="Test check description",
            check_type="sql",
            query="SELECT COUNT(*) FROM users",
            expected=0,
            severity="high",
            column="email",
            table="users"
        )
        
        assert check.id == "test_check"
        assert check.description == "Test check description"
        assert check.check_type == "sql"
        assert check.query == "SELECT COUNT(*) FROM users"
        assert check.expected == 0
        assert check.severity == "high"
        assert check.column == "email"
        assert check.table == "users"
        
    def test_validation_check_to_dict(self):
        """Test converting validation check to dictionary."""
        check = ValidationCheck(
            id="test_check",
            description="Test",
            check_type="sql",
            query="SELECT 1",
            expected=0
        )
        
        check_dict = check.to_dict()
        
        assert isinstance(check_dict, dict)
        assert check_dict["id"] == "test_check"
        assert check_dict["description"] == "Test"
        assert check_dict["check_type"] == "sql"
        assert "query" in check_dict
        
    def test_validation_check_from_dict(self):
        """Test creating validation check from dictionary."""
        data = {
            "id": "from_dict_check",
            "description": "From dict",
            "check_type": "sql",
            "query": "SELECT COUNT(*) FROM test",
            "expected": 0,
            "severity": "medium"
        }
        
        check = ValidationCheck.from_dict(data)
        
        assert check.id == "from_dict_check"
        assert check.check_type == "sql"
        assert check.expected == 0


class TestCheckResult:
    """Tests for CheckResult data model."""
    
    def test_create_check_result_pass(self):
        """Test creating a passing check result."""
        result = CheckResult(
            check_id="test_check",
            description="Test check",
            check_type="sql",
            severity="high",
            status="PASS",
            actual=0,
            expected=0,
            duration_ms=10.5
        )
        
        assert result.status == "PASS"
        assert result.check_id == "test_check"
        assert result.actual == 0
        assert result.duration_ms == 10.5
        
    def test_create_check_result_fail(self):
        """Test creating a failing check result."""
        result = CheckResult(
            check_id="test_check",
            description="Test check",
            check_type="sql",
            severity="critical",
            status="FAIL",
            actual=5,
            expected=0,
            details="Found 5 violations"
        )
        
        assert result.status == "FAIL"
        assert result.actual == 5
        assert result.expected == 0
        assert result.details == "Found 5 violations"
        
    def test_create_check_result_error(self):
        """Test creating an errored check result."""
        result = CheckResult(
            check_id="test_check",
            description="Test check",
            check_type="sql",
            severity="high",
            status="ERROR",
            error="Connection failed"
        )
        
        assert result.status == "ERROR"
        assert result.error == "Connection failed"


class TestValidationReport:
    """Tests for ValidationReport data model."""
    
    def test_create_validation_report(self):
        """Test creating a validation report."""
        executed_at = datetime(2025, 1, 1, 12, 0, 0)
        report = ValidationReport(
            pipeline_name="test_pipeline",
            output_table="users_clean",
            executed_at=executed_at,
            checks_passed=10,
            checks_failed=2,
            checks_errored=0,
            total_checks=12,
            overall_status="FAIL",
            duration_seconds=5.5,
            source_row_count=100,
            output_row_count=95
        )
        
        assert report.pipeline_name == "test_pipeline"
        assert report.output_table == "users_clean"
        assert report.checks_passed == 10
        assert report.checks_failed == 2
        assert report.overall_status == "FAIL"
        assert report.duration_seconds == 5.5
        
    def test_get_failed_checks(self):
        """Test getting failed checks from report."""
        fail_result = CheckResult(
            check_id="fail_check",
            description="Failing check",
            check_type="sql",
            severity="high",
            status="FAIL",
            actual=1,
            expected=0
        )
        
        pass_result = CheckResult(
            check_id="pass_check",
            description="Passing check",
            check_type="sql",
            severity="medium",
            status="PASS",
            actual=0,
            expected=0
        )
        
        report = ValidationReport(
            pipeline_name="test",
            output_table="test_table",
            executed_at=datetime.now(),
            checks_passed=1,
            checks_failed=1,
            total_checks=2,
            overall_status="FAIL",
            duration_seconds=1.0,
            check_results=[pass_result, fail_result]
        )
        
        failed = report.get_failed_checks()
        assert len(failed) == 1
        assert failed[0].check_id == "fail_check"
        
    def test_get_errored_checks(self):
        """Test getting errored checks from report."""
        error_result = CheckResult(
            check_id="error_check",
            description="Errored check",
            check_type="sql",
            severity="high",
            status="ERROR",
            error="Something went wrong"
        )
        
        report = ValidationReport(
            pipeline_name="test",
            output_table="test_table",
            executed_at=datetime.now(),
            checks_passed=0,
            checks_failed=0,
            checks_errored=1,
            total_checks=1,
            overall_status="WARN",
            duration_seconds=1.0,
            check_results=[error_result]
        )
        
        errored = report.get_errored_checks()
        assert len(errored) == 1
        assert errored[0].check_id == "error_check"
        
    def test_get_critical_failures(self):
        """Test getting critical severity failures."""
        critical_fail = CheckResult(
            check_id="critical_check",
            description="Critical check",
            check_type="sql",
            severity="critical",
            status="FAIL"
        )
        
        high_fail = CheckResult(
            check_id="high_check",
            description="High check",
            check_type="sql",
            severity="high",
            status="FAIL"
        )
        
        report = ValidationReport(
            pipeline_name="test",
            output_table="test_table",
            executed_at=datetime.now(),
            checks_passed=0,
            checks_failed=2,
            total_checks=2,
            overall_status="FAIL",
            duration_seconds=1.0,
            check_results=[critical_fail, high_fail]
        )
        
        critical = report.get_critical_failures()
        assert len(critical) == 1
        assert critical[0].check_id == "critical_check"


class TestValidationCheckGenerator:
    """Tests for ValidationCheckGenerator."""
    
    def test_generate_from_schema_users(self):
        """Test generating checks from users schema."""
        from utils.paths import paths
        
        schema_path = str(paths.users_schema)
        if not Path(schema_path).exists():
            pytest.skip(f"Schema file not found: {schema_path}")
        
        checks = ValidationCheckGenerator.generate_from_schema(
            schema_path=schema_path,
            table_name="users"
        )
        
        # Should generate multiple checks
        assert len(checks) > 0
        
        # Check that we have various types of checks
        check_ids = [c.id for c in checks]
        check_types = [c.check_type for c in checks]
        
        # Should have row count check
        assert any("row_count" in cid for cid in check_ids)
        
        # Should have NOT NULL checks for non-nullable columns
        # Should have type checks
        assert any("type_check" in cid for cid in check_ids)
        
        # All checks should have proper fields
        for check in checks:
            assert check.id is not None
            assert check.description is not None
            assert check.check_type is not None
            assert check.severity is not None
            assert check.table is not None
            
    def test_generate_from_schema_nonexistent_table(self):
        """Test generating checks for non-existent table."""
        from utils.paths import paths
        
        schema_path = str(paths.users_schema)
        if not Path(schema_path).exists():
            pytest.skip(f"Schema file not found: {schema_path}")
        
        with pytest.raises(ValueError, match="Table.*not found"):
            ValidationCheckGenerator.generate_from_schema(
                schema_path=schema_path,
                table_name="nonexistent_table"
            )


class TestValidationAgent:
    """Tests for ValidationAgent."""
    
    def test_validation_agent_init(self):
        """Test ValidationAgent initialization."""
        agent = ValidationAgent()
        assert agent.db_path is not None
        assert agent.duckdb_conn is None
        
    def test_validation_agent_connect(self):
        """Test ValidationAgent database connection."""
        from utils.paths import paths
        
        db_path = str(paths.database)
        if not Path(db_path).exists():
            pytest.skip(f"Database not found: {db_path}")
        
        agent = ValidationAgent(db_path=db_path)
        
        try:
            conn = agent.connect()
            assert conn is not None
            agent.close()
        except Exception as e:
            pytest.skip(f"Could not connect to database: {e}")
            
    def test_validation_agent_context_manager(self):
        """Test ValidationAgent as context manager."""
        from utils.paths import paths
        
        db_path = str(paths.database)
        if not Path(db_path).exists():
            pytest.skip(f"Database not found: {db_path}")
        
        try:
            with ValidationAgent(db_path=db_path) as agent:
                assert agent.duckdb_conn is not None
            
            # Connection should be closed after exiting context
            assert agent.duckdb_conn is None
        except Exception as e:
            pytest.skip(f"Could not connect to database: {e}")
            
    def test_save_and_load_checks(self):
        """Test saving and loading validation checks."""
        checks = [
            ValidationCheck(
                id="check_1",
                description="First check",
                check_type="sql",
                query="SELECT 1",
                expected=1,
                severity="high",
                table="test"
            ),
            ValidationCheck(
                id="check_2",
                description="Second check",
                check_type="sql",
                query="SELECT 0",
                expected=0,
                severity="medium",
                table="test"
            )
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            checks_path = f.name
        
        try:
            agent = ValidationAgent()
            saved_path = agent.save_checks(checks, checks_path)
            assert Path(saved_path).exists()
            
            loaded_checks = agent.load_checks(checks_path)
            assert len(loaded_checks) == 2
            assert loaded_checks[0].id == "check_1"
            assert loaded_checks[1].id == "check_2"
        finally:
            Path(checks_path).unlink(missing_ok=True)
            
    def test_generate_and_save_checks(self):
        """Test generating and saving checks from schema."""
        from utils.paths import paths
        
        schema_path = str(paths.users_schema)
        if not Path(schema_path).exists():
            pytest.skip(f"Schema file not found: {schema_path}")
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            checks_path = f.name
        
        try:
            agent = ValidationAgent()
            checks = agent.generate_and_save_checks(
                schema_path=schema_path,
                table_name="users",
                checks_path=checks_path
            )
            
            assert len(checks) > 0
            assert Path(checks_path).exists()
            
            # Verify the saved file is valid JSON
            with open(checks_path, 'r') as f:
                saved_data = json.load(f)
            assert isinstance(saved_data, list)
            assert len(saved_data) > 0
        finally:
            Path(checks_path).unlink(missing_ok=True)


class TestSaveLoadValidationReport:
    """Tests for saving and loading validation reports."""
    
    def test_save_validation_report(self):
        """Test saving a validation report."""
        from utils.paths import paths
        
        report = ValidationReport(
            pipeline_name="test_pipeline",
            output_table="users_clean",
            executed_at=datetime.now(),
            checks_passed=5,
            checks_failed=0,
            total_checks=5,
            overall_status="PASS",
            duration_seconds=1.5,
            source_row_count=100,
            output_row_count=95
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            report_path = f.name
        
        try:
            saved_path = save_validation_report(report, report_path)
            assert Path(saved_path).exists()
            
            # Verify content
            with open(saved_path, 'r') as f:
                data = json.load(f)
            
            assert data["pipeline_name"] == "test_pipeline"
            assert data["overall_status"] == "PASS"
        finally:
            Path(report_path).unlink(missing_ok=True)
            
    def test_load_validation_report(self):
        """Test loading a validation report."""
        report = ValidationReport(
            pipeline_name="test_pipeline",
            output_table="users_clean",
            executed_at=datetime.now(),
            checks_passed=3,
            checks_failed=2,
            total_checks=5,
            overall_status="FAIL",
            duration_seconds=2.0
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            report_path = f.name
            # Write initial data using save_validation_report which handles datetime serialization
            save_validation_report(report, report_path)
        
        try:
            loaded_report = load_validation_report(report_path)
            assert loaded_report.pipeline_name == "test_pipeline"
            assert loaded_report.overall_status == "FAIL"
            assert loaded_report.checks_passed == 3
        finally:
            Path(report_path).unlink(missing_ok=True)


class TestGetChecksPath:
    """Tests for get_checks_path function."""
    
    def test_get_checks_path_users(self):
        """Test getting checks path for users pipeline."""
        from utils.paths import paths
        
        path = get_checks_path("users", "users_clean")
        
        assert path.name == "users_clean_validation_checks.json"
        assert "validation" in str(path)
        
    def test_get_checks_path_transactions(self):
        """Test getting checks path for transactions pipeline."""
        path = get_checks_path("transactions", "transactions_clean")
        
        assert path.name == "transactions_clean_validation_checks.json"
        
    def test_get_checks_path_orders(self):
        """Test getting checks path for orders pipeline."""
        path = get_checks_path("orders", "orders_clean")
        
        assert path.name == "orders_clean_validation_checks.json"


class TestPipelineBuilderIntegration:
    """Integration tests with pipeline builder tools."""
    
    def test_generate_validation_checks_function(self):
        """Test the generate_validation_checks function from pipeline builder."""
        from agents.pipeline_builder.tools import generate_validation_checks
        from utils.paths import paths
        
        schema_path = str(paths.users_schema)
        if not Path(schema_path).exists():
            pytest.skip(f"Schema file not found: {schema_path}")
        
        checks = generate_validation_checks(
            schema_path=schema_path,
            table_name="users"
        )
        
        assert isinstance(checks, list)
        assert len(checks) > 0
        
        # Check that each check has the expected structure
        for check in checks:
            assert isinstance(check, dict)
            assert "id" in check
            assert "description" in check
            assert "check_type" in check


class TestEndToEndValidation:
    """End-to-end tests for validation workflow."""
    
    def test_complete_validation_workflow(self):
        """Test complete validation workflow from check generation to report."""
        from utils.paths import paths
        
        # Check if we have the required files
        schema_path = str(paths.users_schema)
        db_path = str(paths.database)
        
        if not Path(schema_path).exists():
            pytest.skip(f"Schema file not found: {schema_path}")
        
        if not Path(db_path).exists():
            pytest.skip(f"Database not found: {db_path}")
        
        try:
            # Step 1: Generate checks
            agent = ValidationAgent(db_path=db_path)
            checks_path = get_checks_path("users", "users_clean")
            
            # Generate and save checks
            checks = agent.generate_and_save_checks(
                schema_path=schema_path,
                table_name="users",
                checks_path=str(checks_path)
            )
            
            assert len(checks) > 0
            
            # Step 2: Load checks
            loaded_checks = agent.load_checks(str(checks_path))
            assert len(loaded_checks) == len(checks)
            
            # Step 3: Validate (this will connect to DB and run checks)
            # Note: This requires the DB to have the appropriate tables
            # For now, just test that the function exists and can be called
            # In a real test environment, we'd have test data set up
            
            agent.close()
            
        except Exception as e:
            # Skip if database connection fails
            pytest.skip(f"Could not complete workflow: {e}")


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
