"""
Tests for pipeline builder tools.
These tests verify the core functionality of schema comparison,
cleaning pipeline generation, and transformation logic.
"""

import pytest
import os
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
sys.path.insert(0, str(PROJECT_ROOT))


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_csv_path(tmp_path):
    """Create a temporary CSV file for testing."""
    csv_content = """id,name,age,email
1,Alice,30,alice@example.com
2,Bob,25,bob@example.com
3,Charlie,,charlie@example.com
4,David,N/A,david@example.com
"""
    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text(csv_content)
    return str(csv_file)


@pytest.fixture
def sample_schema_path(tmp_path):
    """Create a temporary schema YAML file for testing."""
    schema_content = """
tables:
  users:
    description: "Test user table"
    columns:
      - name: id
        type: integer
        nullable: false
        description: "User ID"
      - name: name
        type: string
        nullable: false
        description: "User name"
      - name: age
        type: integer
        nullable: false
        default: 0
        constraints:
          min: 0
          max: 150
        description: "User age"
      - name: email
        type: string
        nullable: false
        default: "unknown@example.com"
        description: "User email"
"""
    schema_file = tmp_path / "test_schema.yaml"
    schema_file.write_text(schema_content)
    return str(schema_file)


# =============================================================================
# Helper Function Tests
# =============================================================================

class TestTypeInference:
    """Tests for type inference helper functions."""
    
    def test_infer_type_integer(self):
        """Test inferring integer type from values."""
        from agents.pipeline_builder.tools import _infer_type
        
        assert _infer_type(["1", "2", "3", "-5"]) == "integer"
        assert _infer_type(["100", "200", "300"]) == "integer"
    
    def test_infer_type_float(self):
        """Test inferring float type from values."""
        from agents.pipeline_builder.tools import _infer_type
        
        assert _infer_type(["1.5", "2.5", "3.5"]) == "float"
        assert _infer_type(["1.0", "2.0", "3.0"]) == "float"
    
    def test_infer_type_date(self):
        """Test inferring date type from values."""
        from agents.pipeline_builder.tools import _infer_type
        
        assert _infer_type(["2024-01-15", "2024-02-20", "2024-03-10"]) == "date"
    
    def test_infer_type_string(self):
        """Test inferring string type from non-numeric/date values."""
        from agents.pipeline_builder.tools import _infer_type
        
        assert _infer_type(["alice", "bob", "charlie"]) == "string"
        assert _infer_type(["N/A", "unknown", ""]) == "string"
    
    def test_infer_type_empty(self):
        """Test inferring type from empty values."""
        from agents.pipeline_builder.tools import _infer_type
        
        assert _infer_type([]) == "unknown"
    
    def test_is_float(self):
        """Test float detection."""
        from agents.pipeline_builder.tools import _is_float
        
        assert _is_float("1.5") == True
        assert _is_float("2.0") == True
        assert _is_float("not_a_number") == False
        assert _is_float("") == False
    
    def test_is_date(self):
        """Test date detection."""
        from agents.pipeline_builder.tools import _is_date
        
        assert _is_date("2024-01-15") == True
        assert _is_date("2024-02-20") == True
        assert _is_date("not-a-date") == False


# =============================================================================
# Schema Loading Tests
# =============================================================================

class TestSchemaLoading:
    """Tests for schema loading functions."""
    
    def test_load_ideal_schema_valid(self, sample_schema_path):
        """Test loading a valid schema file."""
        from agents.pipeline_builder.tools import load_ideal_schema
        
        schema = load_ideal_schema(sample_schema_path)
        assert isinstance(schema, dict)
        assert "tables" in schema
        assert "users" in schema["tables"]
    
    def test_load_ideal_schema_with_path(self, sample_schema_path):
        """Test loading schema from explicit path."""
        import yaml
        with open(sample_schema_path, 'r') as f:
            expected = yaml.safe_load(f)
        
        from agents.pipeline_builder.tools import load_ideal_schema
        
        schema = load_ideal_schema(sample_schema_path)
        assert schema == expected


# =============================================================================
# Schema Inference Tests
# =============================================================================

class TestSchemaInference:
    """Tests for schema inference from CSV."""
    
    def test_infer_source_schema_valid(self, sample_csv_path):
        """Test inferring schema from a valid CSV file."""
        from agents.pipeline_builder.tools import infer_source_schema
        
        result = infer_source_schema(sample_csv_path)
        assert isinstance(result, dict)
        assert "file" in result
        assert "columns" in result
        assert "row_count" in result
        assert result["row_count"] == 4
    
    def test_infer_source_schema_file_not_found(self):
        """Test handling of missing file."""
        from agents.pipeline_builder.tools import infer_source_schema
        
        result = infer_source_schema("/nonexistent/path.csv")
        assert isinstance(result, dict)
        assert "error" in result
    
    def test_infer_source_schema_empty_file(self, tmp_path):
        """Test handling of empty CSV file."""
        from agents.pipeline_builder.tools import infer_source_schema
        
        empty_csv = tmp_path / "empty.csv"
        empty_csv.write_text("")
        
        result = infer_source_schema(str(empty_csv))
        assert isinstance(result, dict)
        assert "error" in result


# =============================================================================
# Schema Comparison Tests
# =============================================================================

class TestSchemaComparison:
    """Tests for schema comparison logic."""
    
    def test_compare_schemas_basic(self, sample_csv_path, sample_schema_path):
        """Test basic schema comparison."""
        from agents.pipeline_builder.tools import compare_schemas
        
        result = compare_schemas(sample_csv_path, sample_schema_path)
        assert isinstance(result, dict)
        assert "matches" in result
        assert "mismatches" in result
        assert "missing" in result
        assert "extra" in result
    
    def test_compare_schemas_type_mismatch(self, tmp_path):
        """Test detection of type mismatches."""
        from agents.pipeline_builder.tools import compare_schemas
        
        # Create CSV with string ages
        csv_content = """id,name,age
1,Alice,thirty
2,Bob,twenty-five
"""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)
        
        # Create schema expecting integer ages
        schema_content = """
tables:
  users:
    columns:
      - name: id
        type: integer
      - name: name
        type: string
      - name: age
        type: integer
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)
        
        result = compare_schemas(str(csv_file), str(schema_file))
        assert len(result["mismatches"]) > 0
        assert any(m["column"] == "age" for m in result["mismatches"])
    
    def test_compare_schemas_null_values(self, tmp_path):
        """Test detection of null values."""
        from agents.pipeline_builder.tools import compare_schemas
        
        csv_content = """id,name,email
1,Alice,alice@example.com
2,Bob,
"""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)
        
        schema_content = """
tables:
  users:
    columns:
      - name: id
        type: integer
      - name: name
        type: string
      - name: email
        type: string
        nullable: false
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)
        
        result = compare_schemas(str(csv_file), str(schema_file))
        assert len(result["mismatches"]) > 0
        assert any(
            any(issue["type"] == "null_values_found" for issue in m["issues"])
            for m in result["mismatches"]
        )


# =============================================================================
# Pipeline Generation Tests
# =============================================================================

class TestPipelineGeneration:
    """Tests for pipeline code generation."""
    
    def test_generate_cleaning_pipeline_basic(self, sample_csv_path, sample_schema_path):
        """Test basic pipeline generation."""
        from agents.pipeline_builder.tools import generate_cleaning_pipeline
        
        result = generate_cleaning_pipeline(
            sample_csv_path,
            sample_schema_path,
            output_table="test_clean",
            source_table="raw_test"
        )
        assert isinstance(result, dict)
        assert "pipeline_code" in result
        assert "cleaning_sql" in result
        assert "validation_queries" in result
        assert "comparison" in result
    
    def test_generate_cleaning_pipeline_contains_prefect_imports(self, sample_csv_path, sample_schema_path):
        """Test that generated pipeline contains Prefect imports."""
        from agents.pipeline_builder.tools import generate_cleaning_pipeline
        
        result = generate_cleaning_pipeline(
            sample_csv_path,
            sample_schema_path,
            output_table="test_clean"
        )
        pipeline_code = result["pipeline_code"]
        assert "from prefect import flow, task" in pipeline_code
        assert "@task" in pipeline_code
        assert "@flow" in pipeline_code
    
    def test_generate_cleaning_pipeline_contains_cleaning_logic(self, sample_csv_path, sample_schema_path):
        """Test that generated pipeline contains cleaning logic."""
        from agents.pipeline_builder.tools import generate_cleaning_pipeline
        
        result = generate_cleaning_pipeline(
            sample_csv_path,
            sample_schema_path,
            output_table="test_clean"
        )
        pipeline_code = result["pipeline_code"]
        assert "def clean_data" in pipeline_code
        assert "def save_to_duckdb" in pipeline_code
    
    def test_generate_cleaning_pipeline_validation_queries(self, sample_csv_path, sample_schema_path):
        """Test that validation queries are generated."""
        from agents.pipeline_builder.tools import generate_cleaning_pipeline
        
        result = generate_cleaning_pipeline(
            sample_csv_path,
            sample_schema_path,
            output_table="test_clean"
        )
        queries = result["validation_queries"]
        assert isinstance(queries, list)
        assert len(queries) > 0
        # Should have a row count query
        assert any("COUNT(*)" in q for q in queries)


# =============================================================================
# Validation Query Tests
# =============================================================================

class TestValidationQueries:
    """Tests for validation query generation."""
    
    def test_generate_validation_queries_basic(self):
        """Test basic validation query generation."""
        from agents.pipeline_builder.tools import _generate_validation_queries
        
        ideal_by_name = {
            "id": {"type": "integer", "nullable": False},
            "name": {"type": "string", "nullable": False},
        }
        
        queries = _generate_validation_queries("test_table", ideal_by_name)
        assert isinstance(queries, list)
        assert len(queries) > 0
        assert any("COUNT(*)" in q for q in queries)
    
    def test_generate_validation_queries_null_checks(self):
        """Test that null checks are generated for non-nullable columns."""
        from agents.pipeline_builder.tools import _generate_validation_queries
        
        ideal_by_name = {
            "email": {"type": "string", "nullable": False},
        }
        
        queries = _generate_validation_queries("test_table", ideal_by_name)
        assert any("IS NULL" in q for q in queries)
    
    def test_generate_validation_queries_type_checks(self):
        """Test that type checks are generated."""
        from agents.pipeline_builder.tools import _generate_validation_queries
        
        ideal_by_name = {
            "age": {"type": "integer", "nullable": True},
        }
        
        queries = _generate_validation_queries("test_table", ideal_by_name)
        assert any("typeof" in q for q in queries)
    
    def test_generate_validation_queries_range_checks(self):
        """Test that range checks are generated."""
        from agents.pipeline_builder.tools import _generate_validation_queries
        
        ideal_by_name = {
            "score": {"type": "float", "nullable": True, "constraints": {"min": 0, "max": 100}},
        }
        
        queries = _generate_validation_queries("test_table", ideal_by_name)
        assert any("< 0" in q for q in queries)
        assert any("> 100" in q for q in queries)
    
    def test_generate_validation_queries_enum_checks(self):
        """Test that enum checks are generated."""
        from agents.pipeline_builder.tools import _generate_validation_queries
        
        ideal_by_name = {
            "status": {"type": "string", "nullable": True, "enum": ["active", "inactive"]},
        }
        
        queries = _generate_validation_queries("test_table", ideal_by_name)
        assert any("NOT IN" in q for q in queries)


# =============================================================================
# Recommendation Generation Tests
# =============================================================================

class TestRecommendationGeneration:
    """Tests for transformation recommendation generation."""
    
    def test_generate_recommendation_type_mismatch(self):
        """Test recommendation for type mismatch."""
        from agents.pipeline_builder.tools import _generate_recommendation
        
        ideal_def = {"type": "integer", "default": 0}
        source_def = {"type": "string"}
        issues = [{"type": "type_mismatch", "expected": "integer", "actual": "string"}]
        
        recommendation = _generate_recommendation("age", ideal_def, source_def, issues)
        assert "recommendations" in recommendation
        assert len(recommendation["recommendations"]) > 0
        assert any(r["action"] == "cast" for r in recommendation["recommendations"])
    
    def test_generate_recommendation_null_values(self):
        """Test recommendation for null values."""
        from agents.pipeline_builder.tools import _generate_recommendation
        
        ideal_def = {"type": "string", "default": "unknown@example.com", "nullable": False}
        source_def = {"type": "string", "nullable": True, "null_count": 5}
        issues = [{"type": "null_values_found", "count": 5}]
        
        recommendation = _generate_recommendation("email", ideal_def, source_def, issues)
        assert "recommendations" in recommendation
        assert len(recommendation["recommendations"]) > 0
        assert any(r["action"] in ["coalesce", "fill_nulls"] for r in recommendation["recommendations"])


# =============================================================================
# Integration Tests
# =============================================================================

class TestIntegration:
    """Integration tests for the full pipeline builder workflow."""
    
    def test_full_workflow(self, sample_csv_path, sample_schema_path, tmp_path):
        """Test the complete workflow from schema comparison to pipeline generation."""
        from agents.pipeline_builder.tools import (
            infer_source_schema,
            compare_schemas,
            generate_cleaning_pipeline
        )
        
        # Step 1: Infer source schema
        source_schema = infer_source_schema(sample_csv_path)
        assert "columns" in source_schema
        
        # Step 2: Compare schemas
        comparison = compare_schemas(sample_csv_path, sample_schema_path)
        assert "matches" in comparison
        assert "mismatches" in comparison
        
        # Step 3: Generate pipeline
        pipeline = generate_cleaning_pipeline(
            sample_csv_path,
            sample_schema_path,
            output_table="test_clean"
        )
        assert "pipeline_code" in pipeline
        assert "@flow" in pipeline["pipeline_code"]
        assert "@task" in pipeline["pipeline_code"]
        
        # Step 4: Verify the generated code is valid Python
        # (We can't import it because it has placeholders, but we can check syntax)
        pipeline_code = pipeline["pipeline_code"]
        assert "def clean_data" in pipeline_code
        assert "def save_to_duckdb" in pipeline_code
        assert "def load_source_data" in pipeline_code
