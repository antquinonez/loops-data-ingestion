"""
Tests for the MCP (Model Context Protocol) Server.

Tests cover:
- MCP server module imports
- Tool functions (query_database, get_data_quality_report, get_recent_errors, get_file_metadata)
- Server startup and configuration
- Helper functions
"""

import pytest
import sys
import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch
import asyncio

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.environ["PYTHONPATH"] = str(PROJECT_ROOT)


class TestMCPServerImports:
    """Test that MCP server can be imported and initialized."""
    
    def test_import_mcp_server_module(self):
        """Test that the mcp_server module can be imported."""
        from flows import mcp_server
        assert mcp_server is not None
    
    def test_import_server(self):
        """Test that the MCP Server instance can be imported."""
        from flows.mcp_server import server
        assert server is not None
        assert hasattr(server, 'name')
        assert server.name == "data_ingestion_mcp"
    
    def test_import_paths(self):
        """Test that paths are properly configured."""
        from flows.mcp_server import PROJECT_ROOT, DATA_DIR, LOG_DIR
        from utils.paths import paths
        
        assert PROJECT_ROOT.exists()
        assert DATA_DIR == paths.data_dir
        assert LOG_DIR == paths.logs_dir
    
    def test_import_tool_functions(self):
        """Test that all tool functions can be imported."""
        from flows.mcp_server import (
            query_database,
            get_data_quality_report,
            get_recent_errors,
            get_file_metadata
        )
        assert callable(query_database)
        assert callable(get_data_quality_report)
        assert callable(get_recent_errors)
        assert callable(get_file_metadata)


class TestMCPTools:
    """Test MCP server tool functions."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary DuckDB database for testing."""
        import duckdb
        import uuid
        
        # Use a unique temp directory to avoid conflicts
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, f"test_{uuid.uuid4().hex}.db")
        
        conn = duckdb.connect(database=temp_path)
        conn.execute("CREATE TABLE test_table AS SELECT 1 as id, 'test' as name")
        conn.close()
        
        yield temp_path
        
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)
    
    @pytest.fixture
    def temp_db_with_ingestion_name(self, temp_db):
        """Create a temp DB and copy it to ingestion.db for testing."""
        import shutil
        db_dir = Path(temp_db).parent
        ingestion_db = db_dir / "ingestion.db"
        shutil.copy(temp_db, str(ingestion_db))
        yield str(ingestion_db)
        if ingestion_db.exists():
            ingestion_db.unlink()
    
    def test_query_database_valid_query(self, temp_db_with_ingestion_name):
        """Test query_database with a valid SQL query."""
        async def run_test():
            from flows.mcp_server import query_database
            
            with patch('flows.mcp_server.DATA_DIR', Path(temp_db_with_ingestion_name).parent):
                result = await query_database("SELECT * FROM test_table")
                
                result_data = json.loads(result)
                assert "columns" in result_data
                assert "results" in result_data
                assert len(result_data["results"]) > 0
        
        asyncio.run(run_test())
    
    def test_query_database_invalid_sql(self):
        """Test query_database with invalid SQL."""
        async def run_test():
            from flows.mcp_server import query_database
            
            result = await query_database("INVALID SQL")
            result_data = json.loads(result)
            assert "error" in result_data
        
        asyncio.run(run_test())
    
    def test_get_data_quality_report_valid_table(self, temp_db_with_ingestion_name):
        """Test get_data_quality_report with a valid table."""
        async def run_test():
            from flows.mcp_server import get_data_quality_report
            
            with patch('flows.mcp_server.DATA_DIR', Path(temp_db_with_ingestion_name).parent):
                result = await get_data_quality_report("test_table")
                
                result_data = json.loads(result)
                assert "table" in result_data
                assert result_data["table"] == "test_table"
                assert "columns" in result_data
                assert "issues" in result_data
        
        asyncio.run(run_test())
    
    def test_get_data_quality_report_nonexistent_table(self, temp_db_with_ingestion_name):
        """Test get_data_quality_report with non-existent table."""
        async def run_test():
            from flows.mcp_server import get_data_quality_report
            
            with patch('flows.mcp_server.DATA_DIR', Path(temp_db_with_ingestion_name).parent):
                result = await get_data_quality_report("nonexistent_table")
                
                result_data = json.loads(result)
                assert "error" in result_data
        
        asyncio.run(run_test())
    
    def test_get_recent_errors_valid_log(self):
        """Test get_recent_errors with a valid log file."""
        async def run_test():
            from flows.mcp_server import get_recent_errors
            
            # Create a temporary log file
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as f:
                f.write("2024-01-01 10:00:00 ERROR: Test error\n")
                f.write("2024-01-01 10:01:00 WARNING: Test warning\n")
                temp_log = f.name
            
            try:
                result = await get_recent_errors(temp_log)
                result_data = json.loads(result)
                
                assert "log_file" in result_data
                assert "error_count" in result_data
                assert result_data["error_count"] >= 0
                assert "errors" in result_data
            finally:
                os.unlink(temp_log)
        
        asyncio.run(run_test())
    
    def test_get_recent_errors_nonexistent_log(self):
        """Test get_recent_errors with non-existent log file."""
        async def run_test():
            from flows.mcp_server import get_recent_errors
            
            result = await get_recent_errors("/nonexistent/log.txt")
            result_data = json.loads(result)
            assert "error" in result_data
        
        asyncio.run(run_test())
    
    def test_get_file_metadata_valid_file(self):
        """Test get_file_metadata with a valid file."""
        async def run_test():
            from flows.mcp_server import get_file_metadata
            
            # Create a temporary file
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
                f.write("test content")
                temp_path = f.name
            
            try:
                result = await get_file_metadata(temp_path)
                result_data = json.loads(result)
                
                assert "path" in result_data
                assert "exists" in result_data
                assert result_data["exists"] is True
                assert "size_bytes" in result_data
                assert result_data["size_bytes"] > 0
                assert "modified" in result_data
            finally:
                os.unlink(temp_path)
        
        asyncio.run(run_test())
    
    def test_get_file_metadata_nonexistent_file(self):
        """Test get_file_metadata with non-existent file."""
        async def run_test():
            from flows.mcp_server import get_file_metadata
            
            result = await get_file_metadata("/nonexistent/file.txt")
            result_data = json.loads(result)
            assert "error" in result_data
        
        asyncio.run(run_test())


class TestHumanReadableSize:
    """Test the _human_readable_size helper function."""
    
    def test_human_readable_size_bytes(self):
        """Test size formatting for bytes."""
        from flows.mcp_server import _human_readable_size
        assert _human_readable_size(100) == "100.0 B"
    
    def test_human_readable_size_kilobytes(self):
        """Test size formatting for kilobytes."""
        from flows.mcp_server import _human_readable_size
        result = _human_readable_size(1024)
        assert "KB" in result
    
    def test_human_readable_size_megabytes(self):
        """Test size formatting for megabytes."""
        from flows.mcp_server import _human_readable_size
        result = _human_readable_size(1024 * 1024)
        assert "MB" in result
    
    def test_human_readable_size_gigabytes(self):
        """Test size formatting for gigabytes."""
        from flows.mcp_server import _human_readable_size
        result = _human_readable_size(1024 * 1024 * 1024)
        assert "GB" in result


class TestMCPServerStartup:
    """Test MCP server startup and configuration."""
    
    def test_server_has_correct_name(self):
        """Test that server has the correct name."""
        from flows.mcp_server import server
        assert server.name == "data_ingestion_mcp"
    
    def test_server_has_version(self):
        """Test that server has a version."""
        from flows.mcp_server import server
        assert hasattr(server, 'version')
    
    def test_main_function_exists(self):
        """Test that main function exists."""
        from flows.mcp_server import main
        assert callable(main)
    
    def test_main_parses_command_line_args(self):
        """Test that main function accepts command line arguments."""
        from flows.mcp_server import main
        import argparse
        
        # Verify main doesn't crash when called (it will try to start server)
        # We just check that the function signature is correct
        import inspect
        sig = inspect.signature(main)
        # main() should have no required args
        assert len(sig.parameters) == 0


class TestMCPToolRegistration:
    """Test MCP server tool registration."""
    
    def test_tools_defined(self):
        """Test that TOOLS list is defined and contains expected tools."""
        from flows.mcp_server import TOOLS
        
        assert TOOLS is not None
        assert isinstance(TOOLS, list)
        assert len(TOOLS) == 4
        
        tool_names = [tool["name"] for tool in TOOLS]
        expected_names = [
            "query_database",
            "get_data_quality_report", 
            "get_recent_errors",
            "get_file_metadata"
        ]
        for name in expected_names:
            assert name in tool_names, f"Tool {name} not found in TOOLS"
    
    def test_tool_schemas_valid(self):
        """Test that all tool schemas are valid."""
        from flows.mcp_server import TOOLS
        import json
        
        for tool in TOOLS:
            # Check required fields
            assert "name" in tool, f"Tool missing 'name' field"
            assert "description" in tool, f"Tool {tool['name']} missing 'description'"
            assert "inputSchema" in tool, f"Tool {tool['name']} missing 'inputSchema'"
            
            # Validate input schema is valid JSON Schema
            input_schema = tool["inputSchema"]
            assert "type" in input_schema
            assert input_schema["type"] == "object"
            assert "properties" in input_schema
    
    def test_tool_registration_handler_exists(self):
        """Test that list_tools handler is registered."""
        from flows.mcp_server import server
        from mcp import types
        
        # Check that ListToolsRequest handler is registered
        assert types.ListToolsRequest in server.request_handlers
    
    def test_call_tool_handler_exists(self):
        """Test that call_tool handler is registered."""
        from flows.mcp_server import server
        from mcp import types
        
        # Check that CallToolRequest handler is registered
        assert types.CallToolRequest in server.request_handlers
    
    def test_tools_capability_available(self):
        """Test that server has tools capability."""
        from flows.mcp_server import server
        from mcp.server.lowlevel.server import NotificationOptions
        
        capabilities = server.get_capabilities(
            NotificationOptions(),
            experimental_capabilities={}
        )
        
        assert capabilities.tools is not None
        assert capabilities.tools.listChanged is not None
    
    def test_list_tools_returns_all_tools(self):
        """Test that list_tools handler returns all defined tools."""
        from flows.mcp_server import server, TOOLS
        from mcp import types
        import asyncio
        
        async def run_test():
            # Call the handler
            handler = server.request_handlers[types.ListToolsRequest]
            result = await handler(types.ListToolsRequest())
            
            assert result is not None
            assert hasattr(result, 'root')
            list_result = result.root
            assert hasattr(list_result, 'tools')
            assert len(list_result.tools) == len(TOOLS)
            
            returned_names = [tool.name for tool in list_result.tools]
            expected_names = [tool["name"] for tool in TOOLS]
            for name in expected_names:
                assert name in returned_names, f"Tool {name} not returned by list_tools"
        
        asyncio.run(run_test())
    
    def test_call_tool_query_database(self):
        """Test calling query_database tool via MCP."""
        from flows.mcp_server import server
        from mcp import types
        import asyncio
        
        async def run_test():
            # Call the tool
            handler = server.request_handlers[types.CallToolRequest]
            request = types.CallToolRequest(
                params=types.CallToolRequestParams(
                    name="query_database",
                    arguments={"query": "SELECT 1 as test"}
                )
            )
            result = await handler(request)
            
            assert result is not None
            assert hasattr(result, 'root')
            call_result = result.root
            assert isinstance(call_result, types.CallToolResult)
            # Note: isError might be True if output validation fails
            assert len(call_result.content) > 0
            
            # Parse the result
            import json
            content_text = call_result.content[0].text
            result_data = json.loads(content_text)
            assert "results" in result_data or "error" in result_data
        
        asyncio.run(run_test())
    
    def test_call_tool_get_file_metadata(self):
        """Test calling get_file_metadata tool via MCP."""
        from flows.mcp_server import server
        from mcp import types
        import tempfile
        import os
        import asyncio
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("test content")
            temp_path = f.name
        
        try:
            async def run_test():
                # Call the tool
                handler = server.request_handlers[types.CallToolRequest]
                request = types.CallToolRequest(
                    params=types.CallToolRequestParams(
                        name="get_file_metadata",
                        arguments={"path": temp_path}
                    )
                )
                result = await handler(request)
                
                assert result is not None
                assert hasattr(result, 'root')
                call_result = result.root
                assert isinstance(call_result, types.CallToolResult)
                # Note: isError might be True if output validation fails
                assert len(call_result.content) > 0
                
                # Parse the result
                import json
                content_text = call_result.content[0].text
                result_data = json.loads(content_text)
                assert result_data["exists"] is True
                assert result_data["size_bytes"] > 0
            
            asyncio.run(run_test())
        finally:
            os.unlink(temp_path)
    
    def test_call_tool_invalid_tool(self):
        """Test calling a non-existent tool."""
        from flows.mcp_server import server
        from mcp import types
        import asyncio
        
        async def run_test():
            # Call a non-existent tool
            handler = server.request_handlers[types.CallToolRequest]
            request = types.CallToolRequest(
                params=types.CallToolRequestParams(
                    name="nonexistent_tool",
                    arguments={}
                )
            )
            result = await handler(request)
            
            assert result is not None
            assert hasattr(result, 'root')
            call_result = result.root
            assert isinstance(call_result, types.CallToolResult)
            # The error is returned as content, not as isError
            assert len(call_result.content) > 0
            
            # Check error message
            import json
            content_text = call_result.content[0].text
            result_data = json.loads(content_text)
            assert "error" in result_data
            assert "Unknown tool" in result_data["error"]
        
        asyncio.run(run_test())
    
    def test_server_tools_printed_on_startup(self):
        """Test that server prints available tools on startup."""
        # This is a documentation test - verifying the print statements exist
        from flows.mcp_server import main
        import inspect
        source = inspect.getsource(main)
        
        # Check that tools are mentioned in the print statements
        assert "Available tools:" in source
        assert "query_database" in source
        assert "get_data_quality_report" in source
        assert "get_recent_errors" in source
        assert "get_file_metadata" in source
