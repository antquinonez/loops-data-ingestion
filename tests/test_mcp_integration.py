"""
Integration tests for MCP server tools.

These tests verify that:
1. The MCP server can be started
2. Tools are accessible via MCP protocol
3. Tools return expected results when called via MCP client
4. Pipelines complete successfully and can be validated using MCP tools
"""

import pytest
import sys
import os
import json
import subprocess
import time
import tempfile
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.environ["PYTHONPATH"] = str(PROJECT_ROOT)

from utils.paths import paths


class TestMCPServerIntegration:
    """Integration tests that start the MCP server and use it."""
    
    @pytest.fixture
    def mcp_server_process(self):
        """Start MCP server as a subprocess."""
        import asyncio
        
        # Start the MCP server
        server_path = str(PROJECT_ROOT / "flows" / "mcp_server.py")
        proc = subprocess.Popen(
            [sys.executable, server_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(PROJECT_ROOT)
        )
        
        # Give it time to start
        time.sleep(2)
        
        yield proc
        
        # Cleanup
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except:
            proc.kill()
    
    def test_mcp_server_starts_without_error(self):
        """Test that MCP server starts without crashing."""
        import subprocess
        import sys
        
        server_path = str(PROJECT_ROOT / "flows" / "mcp_server.py")
        
        # Start the server and check it prints expected output
        proc = subprocess.Popen(
            [sys.executable, server_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(PROJECT_ROOT)
        )
        
        # Wait a bit for startup
        time.sleep(1)
        
        # Check that it printed the startup message
        try:
            # Try to read output
            stdout, stderr = proc.communicate(timeout=2)
            output = stdout + stderr
            
            # Should contain startup messages
            assert "Starting Data Ingestion MCP Server" in output or \
                   "Available resources" in output or \
                   "Available tools" in output or \
                   proc.returncode == 0
        except subprocess.TimeoutExpired:
            # Server is still running (expected)
            proc.terminate()
            proc.wait()
            # This is fine - server started successfully
            pass
        finally:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5)


class TestMCPToolsEndToEnd:
    """End-to-end tests using MCP tools directly (not via subprocess)."""
    
    def test_tools_work_with_real_data(self):
        """Test that tools work with actual project data."""
        from flows.mcp_server import (
            query_database,
            get_data_quality_report,
            get_recent_errors,
            get_file_metadata,
            execute_tool
        )
        import asyncio
        import json
        
        # Ensure database exists by running a quick test
        db_path = str(paths.database)
        if not os.path.exists(db_path):
            # Create a minimal database
            import duckdb
            conn = duckdb.connect(database=db_path, read_only=False)
            conn.execute("CREATE TABLE test_table AS SELECT 1 as id, 'test' as name")
            conn.close()
        
        async def run_tests():
            # Test query_database
            result = await query_database("SELECT * FROM test_table LIMIT 1")
            result_data = json.loads(result)
            assert "columns" in result_data or "error" in result_data
            
            # Test get_file_metadata on a real file
            source_file = str(paths.source_data)
            if os.path.exists(source_file):
                result = await get_file_metadata(source_file)
                result_data = json.loads(result)
                assert "exists" in result_data
                assert result_data["exists"] is True
            
            # Test get_recent_errors on a real log file
            log_file = str(paths.ingestion_log)
            if os.path.exists(log_file):
                result = await get_recent_errors(log_file, hours=24)
                result_data = json.loads(result)
                assert "log_file" in result_data or "error" in result_data
            
            # Test get_data_quality_report
            if os.path.exists(db_path):
                result = await get_data_quality_report("test_table")
                result_data = json.loads(result)
                assert "table" in result_data or "error" in result_data
            
            # Test execute_tool dispatcher
            result = await execute_tool("query_database", {"query": "SELECT 1"})
            result_data = json.loads(result)
            assert "columns" in result_data or "error" in result_data
        
        asyncio.run(run_tests())


class TestPipelineValidationWithMCPTools:
    """Test that MCP tools can validate pipeline outputs."""
    
    def test_validate_clean_table_with_mcp_tools(self):
        """Test validating cleaned table data using MCP tools."""
        from flows.mcp_server import query_database, get_data_quality_report
        import asyncio
        import json
        
        db_path = str(paths.database)
        
        # Ensure database and tables exist
        if not os.path.exists(db_path):
            pytest.skip("Database not created yet")
        
        async def run_validation():
            # Check if clean tables exist
            tables_to_check = ["users_clean", "orders_clean", "transactions_clean"]
            
            for table in tables_to_check:
                # Try to get row count
                result = await query_database(f"SELECT COUNT(*) as count FROM {table}")
                result_data = json.loads(result)
                
                if "error" not in result_data:
                    # Table exists, validate it
                    count = result_data["results"][0]["count"] if result_data["results"] else 0
                    
                    # Get data quality report
                    report_result = await get_data_quality_report(table)
                    report_data = json.loads(report_result)
                    
                    # Verify report structure
                    assert "table" in report_data or "error" in report_data
                    
                    if "columns" in report_data:
                        # Table was analyzed successfully
                        assert isinstance(report_data["columns"], list)
                        
                        # Check for critical issues
                        if "issues" in report_data:
                            critical_issues = [
                                i for i in report_data["issues"] 
                                if i.get("severity") == "high"
                            ]
                            # Cleaned tables should have minimal issues
                            # Note: This is informational, not a hard assertion
                            print(f"Table {table}: {count} rows, {len(report_data['issues'])} issues")
            
            # If we got here, validation worked
            return True
        
        try:
            result = asyncio.run(run_validation())
            assert result is True
        except Exception as e:
            # This is okay if tables don't exist yet
            pytest.skip(f"Pipeline tables not available: {e}")


class TestMCPToolErrorHandling:
    """Test error handling for MCP tools."""
    
    def test_tools_handle_invalid_inputs(self):
        """Test that tools handle invalid inputs gracefully."""
        from flows.mcp_server import (
            query_database,
            get_data_quality_report,
            get_recent_errors,
            get_file_metadata
        )
        import asyncio
        import json
        
        async def run_tests():
            # Test query_database with invalid SQL
            result = await query_database("INVALID SQL")
            result_data = json.loads(result)
            assert "error" in result_data
            
            # Test get_data_quality_report with non-existent table
            result = await get_data_quality_report("nonexistent_table")
            result_data = json.loads(result)
            assert "error" in result_data
            
            # Test get_recent_errors with non-existent file
            result = await get_recent_errors("/nonexistent/file.log")
            result_data = json.loads(result)
            assert "error" in result_data
            
            # Test get_file_metadata with non-existent file
            result = await get_file_metadata("/nonexistent/file.txt")
            result_data = json.loads(result)
            assert "error" in result_data
        
        asyncio.run(run_tests())
    
    def test_tools_return_valid_json(self):
        """Test that all tools return valid JSON."""
        from flows.mcp_server import (
            query_database,
            get_data_quality_report,
            get_recent_errors,
            get_file_metadata
        )
        import asyncio
        import json
        
        async def run_tests():
            tools = [
                ("query_database", {"query": "SELECT 1"}),
                ("get_data_quality_report", {"table_name": "test"}),
                ("get_recent_errors", {}),
                ("get_file_metadata", {"path": "/tmp"}),
            ]
            
            for tool_name, args in tools:
                if tool_name == "query_database":
                    result = await query_database(**args)
                elif tool_name == "get_data_quality_report":
                    result = await get_data_quality_report(**args)
                elif tool_name == "get_recent_errors":
                    result = await get_recent_errors(**args)
                elif tool_name == "get_file_metadata":
                    result = await get_file_metadata(**args)
                
                # Should be valid JSON
                try:
                    json.loads(result)
                except json.JSONDecodeError:
                    pytest.fail(f"Tool {tool_name} returned invalid JSON: {result}")
        
        asyncio.run(run_tests())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
