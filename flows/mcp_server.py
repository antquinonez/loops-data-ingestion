#!/usr/bin/env python3
"""
MCP (Model Context Protocol) Server for Data Ingestion Resources.

This server exposes additional resources and tools to AI agents
that can be used alongside nanobot for enhanced troubleshooting.

Resources exposed:
- ingestion.log (read-only, for real-time log monitoring)
- source_data.csv (read-only, for data inspection)
- database schema information

To use: Start this server and configure your AI client to connect to it.
"""

import sys
import asyncio
import json
import os
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure project root is in Python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault("PYTHONPATH", str(PROJECT_ROOT))

from mcp.server import Server
from mcp.types import (
    TextContent,
    ImageContent,
    EmbeddedResource,
    ListResourcesResult,
    ReadResourceRequest,
    ResourceTemplate,
    Tool,
)

from utils.paths import paths

PROJECT_ROOT = paths.get_abs("project_root")
DATA_DIR = paths.data_dir
LOG_DIR = paths.logs_dir

# Setup logging
logger = logging.getLogger("mcp.server")
logger.setLevel(logging.INFO)

# Add console handler if not already configured
if not logger.handlers:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
    ))
    logger.addHandler(console_handler)


# Define the MCP server
server = Server("data_ingestion_mcp", version="1.0.0")


# ============================================================================
# RESOURCES - Read-only data that the AI can access
# ============================================================================

@server.list_resources()
async def handle_list_resources() -> ListResourcesResult:
    """List available resources for the AI to read."""
    resources = []
    
    # Ingestion log
    ingestion_log = LOG_DIR / "ingestion.log"
    if ingestion_log.exists():
        resources.append(ResourceTemplate(
            uri=f"file://{ingestion_log}",
            name="ingestion_log",
            description="Main ingestion pipeline log file",
            mimeType="text/plain",
            readOnly=True
        ))
    
    # Source data
    source_csv = DATA_DIR / "source_data.csv"
    if source_csv.exists():
        resources.append(ResourceTemplate(
            uri=f"file://{source_csv}",
            name="source_data",
            description="Source CSV file with intentional data quality issues",
            mimeType="text/csv",
            readOnly=True
        ))
    
    # Database
    db_path = DATA_DIR / "ingestion.db"
    if db_path.exists():
        resources.append(ResourceTemplate(
            uri=f"file://{db_path}",
            name="ingestion_db",
            description="DuckDB database with raw and processed tables",
            mimeType="application/vnd.duckdb",
            readOnly=True
        ))
    
    # SKILLS.md
    skills_md = PROJECT_ROOT / "SKILLS.md"
    if skills_md.exists():
        resources.append(ResourceTemplate(
            uri=f"file://{skills_md}",
            name="skills_troubleshooting",
            description="Troubleshooting guide and skills for data ingestion",
            mimeType="text/markdown",
            readOnly=True
        ))
    
    return ListResourcesResult(resources=resources)


@server.read_resource()
async def handle_read_resource(request: ReadResourceRequest) -> List[TextContent | ImageContent | EmbeddedResource]:
    """Read a resource by URI."""
    uri = request.uri
    
    # Handle file:// URIs
    if uri.startswith("file://"):
        file_path = Path(uri[7:])  # Remove 'file://' prefix
        
        if not file_path.exists():
            raise ValueError(f"File not found: {file_path}")
        
        # Check if it's in our allowed directories
        if not any(file_path.is_relative_to(d) for d in [DATA_DIR, LOG_DIR, PROJECT_ROOT]):
            raise ValueError(f"Access denied: {file_path}")
        
        # Read and return the file
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Return as text content
        return [TextContent(type="text", text=content)]
    
    raise ValueError(f"Unknown URI: {uri}")


# ============================================================================
# TOOLS - Actions the AI can execute
# ============================================================================

async def query_database(query: str) -> str:
    """
    Execute a read-only query against the DuckDB database.
    
    Args:
        query: SQL query to execute
    
    Returns:
        Query results as JSON
    """
    import duckdb
    
    db_path = str(DATA_DIR / "ingestion.db")
    
    try:
        logger.info(f"Executing query: {query[:100]}...")
        conn = duckdb.connect(database=db_path, read_only=True)
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        
        result = []
        for row in rows[:100]:  # Limit results
            result.append(dict(zip(columns, row)))
        
        conn.close()
        
        logger.info(f"Query completed: {len(rows)} rows returned")
        return json.dumps({
            "columns": columns,
            "row_count": len(rows),
            "results": result
        }, default=str)
        
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return json.dumps({"error": str(e), "query": query})


async def get_data_quality_report(table_name: str = "raw_users") -> str:
    """
    Generate a data quality report for a table.
    
    Args:
        table_name: Name of the table to analyze
    
    Returns:
        Data quality report as JSON
    """
    import duckdb
    
    db_path = str(DATA_DIR / "ingestion.db")
    
    try:
        logger.info(f"Generating data quality report for table: {table_name}")
        conn = duckdb.connect(database=db_path, read_only=True)
        
        # Get basic stats
        report = {
            "table": table_name,
            "columns": [],
            "issues": []
        }
        
        # Get column info
        columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
        
        for col_info in columns:
            col_name = col_info[0]
            col_type = col_info[1]
            
            # Get stats for this column
            stats = conn.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT {col_name}) as distinct,
                    COUNT(*) - COUNT({col_name}) as null_count,
                    MIN({col_name}) as min_val,
                    MAX({col_name}) as max_val
                FROM {table_name}
            """).fetchone()
            
            report["columns"].append({
                "name": col_name,
                "type": col_type,
                "total": stats[0],
                "distinct": stats[1],
                "null_count": stats[2],
                "null_percentage": round(stats[2] / stats[0] * 100, 2) if stats[0] > 0 else 0
            })
            
            # Check for issues
            if stats[2] > 0:
                report["issues"].append({
                    "type": "null_values",
                    "column": col_name,
                    "count": stats[2],
                    "severity": "high" if stats[2] / stats[0] > 0.5 else "medium"
                })
        
        conn.close()
        logger.info(f"Data quality report generated: {len(report['issues'])} issues found")
        return json.dumps(report, default=str, indent=2)
        
    except Exception as e:
        logger.error(f"Failed to generate data quality report: {e}")
        return json.dumps({"error": str(e)})


async def get_recent_errors(log_path: str = None, 
                           hours: int = 24) -> str:
    """
    Get recent errors from log files.
    
    Args:
        log_path: Path to the log file
        hours: Look back this many hours
    
    Returns:
        Recent errors as JSON
    """
    import re
    from datetime import datetime, timedelta
    
    errors = []
    cutoff = datetime.now() - timedelta(hours=hours)
    
    # Use default log path if not provided
    if log_path is None:
        log_path = str(paths.ingestion_log)
    
    logger.info(f"Scanning log file for errors: {log_path} (last {hours} hours)")
    
    try:
        with open(log_path, 'r') as f:
            for line in f:
                # Try to parse timestamp
                match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                if match:
                    ts_str = match.group(1)
                    ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                    
                    if ts >= cutoff:
                        if "ERROR" in line or "error" in line:
                            errors.append({
                                "timestamp": ts_str,
                                "level": "ERROR",
                                "message": line.strip()
                            })
                        elif "WARN" in line or "warning" in line:
                            errors.append({
                                "timestamp": ts_str,
                                "level": "WARNING",
                                "message": line.strip()
                            })
        
        logger.info(f"Found {len(errors)} errors/warnings in log file")
        return json.dumps({
            "log_file": log_path,
            "time_range": f"last {hours} hours",
            "error_count": len(errors),
            "errors": errors
        }, default=str)
        
    except Exception as e:
        logger.error(f"Failed to scan log file: {e}")
        return json.dumps({"error": str(e)})


async def get_file_metadata(path: str) -> str:
    """
    Get metadata about a file.
    
    Args:
        path: Path to the file
    
    Returns:
        File metadata as JSON
    """
    import os
    from datetime import datetime
    
    file_path = Path(path)
    
    logger.info(f"Getting metadata for file: {path}")
    
    if not file_path.exists():
        logger.warning(f"File not found: {path}")
        return json.dumps({"error": f"File not found: {path}"})
    
    stat = os.stat(file_path)
    
    logger.info(f"File metadata retrieved: {stat.st_size} bytes")
    return json.dumps({
        "path": str(file_path),
        "exists": True,
        "size_bytes": stat.st_size,
        "size_human": _human_readable_size(stat.st_size),
        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
        "is_file": os.path.isfile(file_path),
        "is_directory": os.path.isdir(file_path)
    })


def _human_readable_size(size_bytes: int) -> str:
    """Convert bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


# ============================================================================
# TOOLS - Actions the AI can execute via MCP
# ============================================================================

# Define tool schemas
TOOLS = [
    {
        "name": "query_database",
        "description": "Execute a read-only query against the DuckDB database",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SQL query to execute"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "get_data_quality_report",
        "description": "Generate a data quality report for a table",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Name of the table to analyze (default: raw_users)"}
            }
        }
    },
    {
        "name": "get_recent_errors",
        "description": "Get recent errors from log files",
        "inputSchema": {
            "type": "object",
            "properties": {
                "log_path": {"type": "string", "description": "Path to the log file"},
                "hours": {"type": "integer", "default": 24, "description": "Look back this many hours"}
            }
        }
    },
    {
        "name": "get_file_metadata",
        "description": "Get metadata about a file",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the file"}
            },
            "required": ["path"]
        }
    }
]


# Tool execution dispatcher
async def execute_tool(tool_name: str, arguments: dict) -> str:
    """Dispatch tool execution based on tool name."""
    tools_map = {
        "query_database": query_database,
        "get_data_quality_report": get_data_quality_report,
        "get_recent_errors": get_recent_errors,
        "get_file_metadata": get_file_metadata,
    }
    
    if tool_name not in tools_map:
        logger.warning(f"Tool not found: {tool_name}")
        return json.dumps({"error": f"Unknown tool: {tool_name}"})
    
    logger.info(f"Executing tool: {tool_name} with arguments: {arguments}")
    tool_func = tools_map[tool_name]
    
    # Call the tool function with unpacked arguments
    try:
        # All tool functions take keyword arguments
        result = await tool_func(**arguments)
        logger.info(f"Tool {tool_name} completed successfully")
        return result
    except TypeError as e:
        # Handle case where arguments don't match
        logger.error(f"Invalid arguments for {tool_name}: {e}")
        return json.dumps({"error": f"Invalid arguments for {tool_name}: {str(e)}"})
    except Exception as e:
        # Handle any other exceptions
        logger.error(f"Error executing {tool_name}: {e}")
        return json.dumps({"error": f"Error executing {tool_name}: {str(e)}"})


# Register tool handlers with the server
@server.list_tools()
async def handle_list_tools() -> list:
    """Return list of available tools."""
    from mcp import types
    
    logger.info(f"Listing available tools: {[t['name'] for t in TOOLS]}")
    
    tool_objects = []
    for tool_def in TOOLS:
        tool_obj = types.Tool(
            name=tool_def["name"],
            description=tool_def["description"],
            inputSchema=tool_def["inputSchema"],
            outputSchema=tool_def.get("outputSchema")
        )
        tool_objects.append(tool_obj)
    
    logger.info(f"Returning {len(tool_objects)} tools")
    return tool_objects


@server.call_tool()
async def handle_call_tool(tool_name: str, arguments: dict) -> list:
    """Execute a tool and return the result."""
    from mcp.types import TextContent
    
    logger.info(f"Tool call received: {tool_name}")
    
    # Execute the tool
    result = await execute_tool(tool_name, arguments)
    
    # Return as text content
    logger.info(f"Tool call completed: {tool_name}")
    return [TextContent(type="text", text=result)]


# ============================================================================
# Server Configuration
# ============================================================================


def main():

    """Run the MCP server."""
    import argparse
    from mcp.server.stdio import stdio_server
    from mcp.server.models import InitializationOptions
    from mcp.server.lowlevel.server import NotificationOptions
    
    parser = argparse.ArgumentParser(description="Data Ingestion MCP Server")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8081, help="Port to listen on")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], 
                        help="Logging level")
    args = parser.parse_args()
    
    # Set log level
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    logger.info(f"Starting Data Ingestion MCP Server on {args.host}:{args.port}")
    logger.info("Available resources:")
    logger.info("  - ingestion.log (ingestion pipeline logs)")
    logger.info("  - source_data.csv (source data with intentional errors)")
    logger.info("  - ingestion.db (DuckDB database)")
    logger.info("  - SKILLS.md (troubleshooting guide)")
    logger.info("Available tools:")
    logger.info("  - query_database: Execute SQL queries")
    logger.info("  - get_data_quality_report: Generate data quality reports")
    logger.info("  - get_recent_errors: Get recent errors from logs")
    logger.info("  - get_file_metadata: Get file metadata")
    
    # Run the server using stdio transport
    async def run():
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name=server.name,
                    server_version=server.version if server.version else "1.0.0",
                    capabilities=server.get_capabilities(
                        NotificationOptions(),
                        experimental_capabilities={},
                    ),
                    instructions=server.instructions,
                    website_url=server.website_url,
                    icons=server.icons,
                ),
            )
    
    asyncio.run(run())


if __name__ == "__main__":
    main()
