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

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
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

PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = PROJECT_ROOT / "logs"


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
        conn = duckdb.connect(database=db_path, read_only=True)
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        
        result = []
        for row in rows[:100]:  # Limit results
            result.append(dict(zip(columns, row)))
        
        conn.close()
        
        return json.dumps({
            "columns": columns,
            "row_count": len(rows),
            "results": result
        }, default=str)
        
    except Exception as e:
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
        return json.dumps(report, default=str, indent=2)
        
    except Exception as e:
        return json.dumps({"error": str(e)})


async def get_recent_errors(log_path: str = "/home/aq/Documents/Source/loops/logs/ingestion.log", 
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
        
        return json.dumps({
            "log_file": log_path,
            "time_range": f"last {hours} hours",
            "error_count": len(errors),
            "errors": errors
        }, default=str)
        
    except Exception as e:
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
    
    if not file_path.exists():
        return json.dumps({"error": f"File not found: {path}"})
    
    stat = os.stat(file_path)
    
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
# Server Configuration
# ============================================================================

# Add tools to server
server.add_tool(query_database)
server.add_tool(get_data_quality_report)
server.add_tool(get_recent_errors)
server.add_tool(get_file_metadata)


def main():
    """Run the MCP server."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Data Ingestion MCP Server")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8081, help="Port to listen on")
    args = parser.parse_args()
    
    print(f"Starting Data Ingestion MCP Server on {args.host}:{args.port}")
    print("\nAvailable resources:")
    print("  - ingestion.log (ingestion pipeline logs)")
    print("  - source_data.csv (source data with intentional errors)")
    print("  - ingestion.db (DuckDB database)")
    print("  - SKILLS.md (troubleshooting guide)")
    print("\nAvailable tools:")
    print("  - query_database: Execute SQL queries")
    print("  - get_data_quality_report: Generate data quality reports")
    print("  - get_recent_errors: Get recent errors from logs")
    print("  - get_file_metadata: Get file metadata")
    
    # Run the server
    asyncio.run(server.run(args.host, args.port))


if __name__ == "__main__":
    main()
