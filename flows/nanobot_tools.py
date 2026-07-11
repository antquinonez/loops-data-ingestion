"""
Nanobot tools for data ingestion troubleshooting.
These tools are available to the nanobot agent for investigation.
"""

import duckdb
import os
import json
import csv
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime

# Set up logging for tools
logger = logging.getLogger("nanobot.tools")

DATA_DIR = Path("/home/aq/Documents/Source/loops/data")
DB_PATH = str(DATA_DIR / "ingestion.db")
SOURCE_FILE = str(DATA_DIR / "source_data.csv")
LOG_DIR = Path("/home/aq/Documents/Source/loops/logs")


def read_logs(path: str, tail_n: int = 100) -> str:
    """
    Read log files to find error details.
    
    Args:
        path: Path to the log file
        tail_n: Number of lines to return from the end
    
    Returns:
        Last tail_n lines of the log file as a single string
    """
    logger.info(f"Reading logs from {path}, last {tail_n} lines")
    
    if not os.path.exists(path):
        return f"ERROR: Log file not found: {path}"
    
    try:
        with open(path, 'r') as f:
            lines = f.readlines()
            return ''.join(lines[-tail_n:])
    except Exception as e:
        logger.error(f"Failed to read logs: {e}")
        return f"ERROR: Failed to read logs: {e}"


def query_duckdb(query: str) -> str:
    """
    Query the DuckDB database for investigation.
    
    Args:
        query: SQL query to execute
    
    Returns:
        Query results as JSON string
    """
    logger.info(f"Executing DuckDB query: {query[:100]}...")
    
    try:
        conn = duckdb.connect(database=DB_PATH, read_only=True)
        cursor = conn.cursor()
        
        # Execute the query
        cursor.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        
        # Get all rows
        rows = cursor.fetchall()
        
        # Convert to list of dicts
        result = []
        for row in rows:
            result.append(dict(zip(columns, row)))
        
        conn.close()
        
        logger.info(f"Query returned {len(result)} rows")
        return json.dumps({
            "columns": columns,
            "row_count": len(result),
            "results": result[:100]  # Limit to 100 rows for response size
        }, default=str, indent=2)
        
    except Exception as e:
        logger.error(f"DuckDB query failed: {e}")
        return json.dumps({
            "error": str(e),
            "query": query
        })


def inspect_file(path: str, sample_size: int = 10) -> str:
    """
    Inspect source data files for investigation.
    
    Args:
        path: Path to the file
        sample_size: Number of sample rows to include
    
    Returns:
        File metadata and sample data as JSON
    """
    logger.info(f"Inspecting file: {path}")
    
    if not os.path.exists(path):
        return json.dumps({"error": f"File not found: {path}"})
    
    try:
        file_size = os.path.getsize(path)
        file_stats = os.stat(path)
        
        # Read sample data
        sample_data = []
        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i < sample_size:
                    sample_data.append(row)
                else:
                    break
        
        # Get file info
        file_info = {
            "path": path,
            "size_bytes": file_size,
            "size_human": _human_readable_size(file_size),
            "modified": datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
            "rows": len(sample_data),
            "columns": list(sample_data[0].keys()) if sample_data else [],
            "sample_data": sample_data
        }
        
        logger.info(f"File inspection complete: {file_info['rows']} sample rows")
        return json.dumps(file_info, default=str, indent=2)
        
    except Exception as e:
        logger.error(f"File inspection failed: {e}")
        return json.dumps({"error": str(e), "path": path})


def check_schema(path: str, schema: Optional[Dict[str, Any]] = None) -> str:
    """
    Validate data against expected schema.
    
    Args:
        path: Path to the data file
        schema: Expected schema (optional, will infer from file if not provided)
    
    Returns:
        Validation report with errors as JSON
    """
    logger.info(f"Checking schema for: {path}")
    
    if not os.path.exists(path):
        return json.dumps({"error": f"File not found: {path}"})
    
    try:
        # Read all data
        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        if not rows:
            return json.dumps({"error": "No data in file"})
        
        columns = list(rows[0].keys())
        
        # Define expected schema if not provided
        if schema is None:
            schema = {
                "id": {"type": "integer", "nullable": False},
                "name": {"type": "string", "nullable": False},
                "email": {"type": "string", "nullable": False, "format": "email"},
                "age": {"type": "integer", "nullable": False, "min": 0, "max": 150},
                "join_date": {"type": "date", "nullable": False},
                "status": {"type": "string", "nullable": False, "enum": ["active", "inactive", "pending", "suspended"]},
                "score": {"type": "float", "nullable": False, "min": 0, "max": 100}
            }
        
        # Validate each row
        errors = []
        for row_index, row in enumerate(rows, start=1):
            row_errors = _validate_row(row, schema, row_index)
            errors.extend(row_errors)
        
        # Summary
        validation_report = {
            "file": path,
            "total_rows": len(rows),
            "total_errors": len(errors),
            "error_rate": len(errors) / len(rows) if rows else 0,
            "errors": errors[:50]  # Limit to 50 errors
        }
        
        logger.info(f"Schema validation: {len(errors)} errors found")
        return json.dumps(validation_report, default=str, indent=2)
        
    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        return json.dumps({"error": str(e), "path": path})


def _validate_row(row: Dict[str, Any], schema: Dict[str, Any], row_index: int) -> List[Dict[str, Any]]:
    """Validate a single row against schema."""
    errors = []
    
    for col, col_schema in schema.items():
        value = row.get(col, None)
        
        # Check nullable
        if value is None or value == '':
            if not col_schema.get('nullable', True):
                errors.append({
                    "row": row_index,
                    "column": col,
                    "error": "NULL value not allowed",
                    "value": value,
                    "severity": "high"
                })
            continue
        
        # Check type
        expected_type = col_schema.get('type', 'string')
        if not _validate_type(value, expected_type):
            errors.append({
                "row": row_index,
                "column": col,
                "error": f"Type mismatch: expected {expected_type}, got {type(value).__name__}",
                "value": value,
                "severity": "high"
            })
        
        # Check format
        if expected_type == 'string' and col_schema.get('format') == 'email':
            if not _validate_email(value):
                errors.append({
                    "row": row_index,
                    "column": col,
                    "error": "Invalid email format",
                    "value": value,
                    "severity": "medium"
                })
        
        # Check enum
        if 'enum' in col_schema and value not in col_schema['enum']:
            errors.append({
                "row": row_index,
                "column": col,
                "error": f"Value not in allowed enum: {col_schema['enum']}",
                "value": value,
                "severity": "medium"
            })
        
        # Check min/max (only if type validation passed)
        if expected_type in ['integer', 'float']:
            try:
                numeric_value = float(value)
                if 'min' in col_schema and numeric_value < col_schema['min']:
                    errors.append({
                        "row": row_index,
                        "column": col,
                        "error": f"Value below minimum: {col_schema['min']}",
                        "value": value,
                        "severity": "medium"
                    })
                if 'max' in col_schema and numeric_value > col_schema['max']:
                    errors.append({
                        "row": row_index,
                        "column": col,
                        "error": f"Value above maximum: {col_schema['max']}",
                        "value": value,
                        "severity": "medium"
                    })
            except (ValueError, TypeError):
                # Type mismatch already caught above, skip min/max check
                pass
    
    return errors


def _validate_type(value: Any, expected_type: str) -> bool:
    """Validate if value matches expected type."""
    if value is None or value == '':
        return True  # Nullable check handled separately
    
    # Get string representation for parsing
    str_value = str(value) if not isinstance(value, str) else value
    
    if expected_type == 'integer':
        try:
            int(str_value)
            return True
        except (ValueError, TypeError):
            return False
    
    elif expected_type == 'float':
        try:
            float(str_value)
            return True
        except (ValueError, TypeError):
            return False
    
    elif expected_type == 'date':
        try:
            # Try various date formats
            from dateutil.parser import parse
            parse(str_value)
            return True
        except:
            return False
    
    elif expected_type == 'string':
        return True  # Always accept strings
    
    return True


def _validate_email(email: str) -> bool:
    """Basic email validation."""
    if not email or not isinstance(email, str):
        return False
    return '@' in email and '.' in email.split('@')[-1]


def _human_readable_size(size_bytes: int) -> str:
    """Convert bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def send_slack_alert(message: str, severity: str = "medium") -> str:
    """
    Send alert to Slack (mock implementation for demo).
    
    Args:
        message: Alert message
        severity: Severity level (low, medium, high, critical)
    
    Returns:
        Success status as JSON
    """
    logger.info(f"Sending Slack alert (severity: {severity})")
    
    # In production, this would actually send to Slack
    # For demo purposes, we'll just log it
    alert_log_path = str(LOG_DIR / "slack_alerts.log")
    timestamp = datetime.now().isoformat()
    
    alert_entry = f"[{timestamp}] [{severity.upper()}] {message}\n"
    
    try:
        with open(alert_log_path, 'a') as f:
            f.write(alert_entry)
        
        logger.info(f"Slack alert logged to {alert_log_path}")
        return json.dumps({
            "status": "success",
            "message": "Alert logged (would send to Slack in production)",
            "severity": severity,
            "timestamp": timestamp
        })
    except Exception as e:
        logger.error(f"Failed to log Slack alert: {e}")
        return json.dumps({
            "status": "error",
            "error": str(e)
        })


def get_ingestion_status() -> str:
    """
    Get current status of the ingestion pipeline.
    
    Returns:
        Status information as JSON
    """
    logger.info("Getting ingestion status")
    
    status = {
        "database": DB_PATH,
        "source_file": SOURCE_FILE,
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        conn = duckdb.connect(database=DB_PATH, read_only=True)
        
        # Check if tables exist
        tables = conn.execute("SHOW TABLES").fetchall()
        status["tables"] = [t[0] for t in tables]
        
        # Get row counts
        for table in status["tables"]:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                status[f"{table}_row_count"] = count
            except:
                status[f"{table}_row_count"] = "error"
        
        conn.close()
        
    except Exception as e:
        status["error"] = str(e)
    
    logger.info(f"Ingestion status: {status}")
    return json.dumps(status, default=str, indent=2)


# Register tools for nanobot
NANOBOT_TOOLS = {
    "read_logs": {
        "description": "Read application logs to find error details",
        "function": read_logs,
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the log file"},
                "tail_n": {"type": "integer", "default": 100, "description": "Number of lines to return from the end"}
            },
            "required": ["path"]
        }
    },
    "query_duckdb": {
        "description": "Query the DuckDB database for investigation",
        "function": query_duckdb,
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SQL query to execute"}
            },
            "required": ["query"]
        }
    },
    "inspect_file": {
        "description": "Inspect source data files",
        "function": inspect_file,
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the file"},
                "sample_size": {"type": "integer", "default": 10, "description": "Number of sample rows"}
            },
            "required": ["path"]
        }
    },
    "check_schema": {
        "description": "Validate data against expected schema",
        "function": check_schema,
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the data file"},
                "schema": {"type": "object", "default": None, "description": "Expected schema (optional)"}
            },
            "required": ["path"]
        }
    },
    "send_slack_alert": {
        "description": "Send investigation results to Slack",
        "function": send_slack_alert,
        "parameters": {
            "type": "object",
            "properties": {
                "message": {"type": "string", "description": "Alert message"},
                "severity": {"type": "string", "enum": ["low", "medium", "high", "critical"], "default": "medium"}
            },
            "required": ["message"]
        }
    },
    "get_ingestion_status": {
        "description": "Get current status of the ingestion pipeline",
        "function": get_ingestion_status,
        "parameters": {
            "type": "object",
            "properties": {}
        }
    }
}


if __name__ == "__main__":
    # Test the tools
    print("Testing nanobot tools...")
    
    # Test inspect_file
    print("\n=== Inspecting source file ===")
    print(inspect_file(SOURCE_FILE))
    
    # Test check_schema
    print("\n=== Checking schema ===")
    print(check_schema(SOURCE_FILE))
    
    # Test get_ingestion_status
    print("\n=== Getting ingestion status ===")
    print(get_ingestion_status())
