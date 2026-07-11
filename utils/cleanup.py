"""
Cleanup utilities for the Loops Data Ingestion Project.

This module provides functions to clean up and maintain logs, databases,
and generated files between runs.
"""

import os
import sys
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional, List

# Import path configuration
try:
    from utils.paths import paths, get_project_root
except ImportError:
    # Fallback - use hardcoded path
    PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
    sys.path.insert(0, str(PROJECT_ROOT))
    os.environ["PYTHONPATH"] = str(PROJECT_ROOT)
    from utils.paths import paths, get_project_root
    PROJECT_ROOT = get_project_root()
else:
    PROJECT_ROOT = get_project_root()


def cleanup_all(archive_logs: bool = True, keep_db: bool = False) -> dict:
    """Clean up all generated artifacts, logs, and databases.
    
    Args:
        archive_logs: If True, archive logs instead of deleting them
        keep_db: If True, keep the database files (don't delete them)
    
    Returns:
        Dictionary with cleanup summary
    """
    summary = {
        "logs_cleaned": [],
        "logs_archived": [],
        "db_cleaned": [],
        "pipelines_cleaned": [],
        "errors": []
    }
    
    # Clean up logs
    if archive_logs:
        archive_result = archive_logs()
        summary["logs_archived"] = archive_result.get("archived", [])
        summary["errors"].extend(archive_result.get("errors", []))
    else:
        clean_result = clean_logs()
        summary["logs_cleaned"] = clean_result.get("cleaned", [])
        summary["errors"].extend(clean_result.get("errors", []))
    
    # Clean up database
    if not keep_db:
        db_result = clean_database()
        summary["db_cleaned"] = db_result.get("cleaned", [])
        summary["errors"].extend(db_result.get("errors", []))
    
    # Clean up generated pipelines
    pipe_result = clean_generated_pipelines()
    summary["pipelines_cleaned"] = pipe_result.get("cleaned", [])
    summary["errors"].extend(pipe_result.get("errors", []))
    
    return summary


def clean_logs() -> dict:
    """Remove all log files.
    
    Returns:
        Dictionary with list of cleaned files and any errors
    """
    result = {"cleaned": [], "errors": []}
    
    logs_dir = paths.logs_dir
    if logs_dir.exists():
        for log_file in logs_dir.glob("*.log"):
            try:
                log_file.unlink()
                result["cleaned"].append(str(log_file.name))
            except Exception as e:
                result["errors"].append(f"Failed to delete {log_file.name}: {e}")
        
        for log_file in logs_dir.glob("*.md"):
            try:
                log_file.unlink()
                result["cleaned"].append(str(log_file.name))
            except Exception as e:
                result["errors"].append(f"Failed to delete {log_file.name}: {e}")
    
    return result


def archive_logs(archive_dir: Optional[str] = None) -> dict:
    """Archive existing log files with timestamp.
    
    Args:
        archive_dir: Directory to archive logs to. Defaults to logs/archive/
    
    Returns:
        Dictionary with list of archived files and any errors
    """
    result = {"archived": [], "errors": []}
    
    logs_dir = paths.logs_dir
    if archive_dir is None:
        archive_dir = logs_dir / "archive"
    else:
        archive_dir = Path(archive_dir)
    
    # Create archive directory if it doesn't exist
    try:
        archive_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        result["errors"].append(f"Failed to create archive directory: {e}")
        return result
    
    # Create timestamped subdirectory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_dir = archive_dir / f"run_{timestamp}"
    
    try:
        timestamped_dir.mkdir(exist_ok=True)
    except Exception as e:
        result["errors"].append(f"Failed to create timestamped directory: {e}")
        return result
    
    # Archive all log files
    if logs_dir.exists():
        for log_file in logs_dir.glob("*.log"):
            try:
                dest = timestamped_dir / log_file.name
                shutil.copy2(log_file, dest)
                log_file.unlink()
                result["archived"].append(str(log_file.name))
            except Exception as e:
                result["errors"].append(f"Failed to archive {log_file.name}: {e}")
        
        for log_file in logs_dir.glob("*.md"):
            try:
                dest = timestamped_dir / log_file.name
                shutil.copy2(log_file, dest)
                log_file.unlink()
                result["archived"].append(str(log_file.name))
            except Exception as e:
                result["errors"].append(f"Failed to archive {log_file.name}: {e}")
    
    return result


def clean_database() -> dict:
    """Remove database files.
    
    Returns:
        Dictionary with list of cleaned files and any errors
    """
    result = {"cleaned": [], "errors": []}
    
    db_path = paths.database
    nanobot_db_path = paths.nanobot_db
    
    for db_file in [db_path, nanobot_db_path]:
        if db_file.exists():
            try:
                db_file.unlink()
                result["cleaned"].append(str(db_file.name))
            except Exception as e:
                result["errors"].append(f"Failed to delete {db_file.name}: {e}")
    
    return result


def clean_generated_pipelines() -> dict:
    """Remove all generated pipeline files.
    
    Returns:
        Dictionary with list of cleaned files and any errors
    """
    result = {"cleaned": [], "errors": []}
    
    generated_dir = paths.pipelines_dir / "generated"
    if generated_dir.exists():
        for pipe_file in generated_dir.glob("*.py"):
            try:
                pipe_file.unlink()
                result["cleaned"].append(str(pipe_file.name))
            except Exception as e:
                result["errors"].append(f"Failed to delete {pipe_file.name}: {e}")
    
    return result


def get_run_metadata() -> dict:
    """Get metadata about the current run.
    
    Returns:
        Dictionary with run timestamp, IDs, etc.
    """
    return {
        "timestamp": datetime.now().isoformat(),
        "run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "project_root": str(PROJECT_ROOT)
    }


def setup_run_logging(run_id: Optional[str] = None) -> dict:
    """Setup logging for a new run with unique identifiers.
    
    Args:
        run_id: Unique identifier for this run. If None, generates one.
    
    Returns:
        Dictionary with log file paths and handlers
    """
    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    log_dir = paths.logs_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create main run log
    run_log = log_dir / f"run_{run_id}.log"
    
    return {
        "run_id": run_id,
        "run_log": str(run_log),
        "log_dir": str(log_dir)
    }
