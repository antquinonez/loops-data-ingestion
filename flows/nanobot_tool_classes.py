"""
Nanobot-compatible Tool classes for investigation tools.
These wrap the simple function-based tools from nanobot_tools.py into proper Tool subclasses.
"""

import json
from typing import Any, Dict, Optional
from nanobot.agent.tools.base import Tool, ToolResult

from flows.nanobot_tools import (
    read_logs,
    query_duckdb,
    inspect_file,
    check_schema,
    send_slack_alert,
    get_ingestion_status,
)


class ReadLogsTool(Tool):
    """Read application logs to find error details."""
    
    @property
    def name(self) -> str:
        return "read_logs"
    
    @property
    def description(self) -> str:
        return "Read application logs to find error details"
    
    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the log file"},
                "tail_n": {"type": "integer", "default": 100, "description": "Number of lines to return from the end"}
            },
            "required": ["path"]
        }
    
    async def execute(self, path: str, tail_n: int = 100, **kwargs: Any) -> Any:
        """Read logs from file."""
        result = read_logs(path, tail_n)
        return result


class QueryDuckDBTool(Tool):
    """Query the DuckDB database for investigation."""
    
    @property
    def name(self) -> str:
        return "query_duckdb"
    
    @property
    def description(self) -> str:
        return "Query the DuckDB database for investigation"
    
    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SQL query to execute"}
            },
            "required": ["query"]
        }
    
    async def execute(self, query: str, **kwargs: Any) -> Any:
        """Execute a query against DuckDB."""
        result = query_duckdb(query)
        return result


class InspectFileTool(Tool):
    """Inspect source data files for investigation."""
    
    @property
    def name(self) -> str:
        return "inspect_file"
    
    @property
    def description(self) -> str:
        return "Inspect source data files for investigation"
    
    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the file"},
                "sample_size": {"type": "integer", "default": 10, "description": "Number of sample rows"}
            },
            "required": ["path"]
        }
    
    async def execute(self, path: str, sample_size: int = 10, **kwargs: Any) -> Any:
        """Inspect a file and return metadata."""
        result = inspect_file(path, sample_size)
        return result


class CheckSchemaTool(Tool):
    """Validate data against expected schema."""
    
    @property
    def name(self) -> str:
        return "check_schema"
    
    @property
    def description(self) -> str:
        return "Validate data against expected schema"
    
    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the data file"},
                "schema": {"type": "object", "default": None, "description": "Expected schema (optional)"}
            },
            "required": ["path"]
        }
    
    async def execute(self, path: str, schema: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Any:
        """Validate data against schema."""
        result = check_schema(path, schema)
        return result


class SendSlackAlertTool(Tool):
    """Send investigation results to Slack."""
    
    @property
    def name(self) -> str:
        return "send_slack_alert"
    
    @property
    def description(self) -> str:
        return "Send investigation results to Slack"
    
    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "message": {"type": "string", "description": "Alert message"},
                "severity": {"type": "string", "enum": ["low", "medium", "high", "critical"], "default": "medium"}
            },
            "required": ["message"]
        }
    
    async def execute(self, message: str, severity: str = "medium", **kwargs: Any) -> Any:
        """Send a Slack alert."""
        result = send_slack_alert(message, severity)
        return result


class GetIngestionStatusTool(Tool):
    """Get current status of the ingestion pipeline."""
    
    @property
    def name(self) -> str:
        return "get_ingestion_status"
    
    @property
    def description(self) -> str:
        return "Get current status of the ingestion pipeline"
    
    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {},
        }
    
    async def execute(self, **kwargs: Any) -> Any:
        """Get ingestion status."""
        result = get_ingestion_status()
        return result


# Export all Tool classes
NANOBOT_TOOL_CLASSES = [
    ReadLogsTool,
    QueryDuckDBTool,
    InspectFileTool,
    CheckSchemaTool,
    SendSlackAlertTool,
    GetIngestionStatusTool,
]
