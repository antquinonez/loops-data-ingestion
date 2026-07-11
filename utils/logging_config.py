"""
Unified logging configuration for the Loops Data Ingestion Project.

This module provides centralized logging setup that captures both
custom application logs and Prefect task logs to the same files.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

# Import path configuration
try:
    from utils.paths import paths, get_project_root
except ImportError:
    import os
    PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
    sys.path.insert(0, str(PROJECT_ROOT))
    os.environ["PYTHONPATH"] = str(PROJECT_ROOT)
    from utils.paths import paths, get_project_root
    PROJECT_ROOT = get_project_root()
else:
    PROJECT_ROOT = get_project_root()


# Global run context
_current_run_id: Optional[str] = None
_current_log_file: Optional[str] = None


def setup_logging(run_id: Optional[str] = None, log_name: str = "loops") -> dict:
    """Setup unified logging configuration.
    
    Args:
        run_id: Unique run identifier. If None, uses timestamp.
        log_name: Base name for the logger
    
    Returns:
        Dictionary with logger and file paths
    """
    global _current_run_id, _current_log_file
    
    from datetime import datetime
    import os
    
    if run_id is None:
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    _current_run_id = run_id
    
    # Ensure logs directory exists
    logs_dir = paths.logs_dir
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    # Create log filename
    log_filename = f"{log_name}_{run_id}.log"
    log_path = logs_dir / log_filename
    _current_log_file = str(log_path)
    
    # Create a custom logger
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)
    
    # Clear existing handlers to avoid duplicate logging
    logger.handlers.clear()
    
    # File handler
    file_handler = logging.FileHandler(str(log_path))
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Also configure Prefect logging to use our handlers
    # This redirects Prefect's get_run_logger() output to our files
    prefect_logger = logging.getLogger('prefect')
    prefect_logger.handlers.clear()
    prefect_logger.addHandler(file_handler)
    prefect_logger.addHandler(console_handler)
    prefect_logger.setLevel(logging.DEBUG)
    
    # Configure root logger to avoid duplicate messages
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger.setLevel(logging.WARNING)
    
    return {
        "run_id": run_id,
        "log_file": str(log_path),
        "logger": logger,
        "prefect_logger": prefect_logger
    }


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get a logger with the current configuration.
    
    Args:
        name: Logger name. Defaults to 'loops'
    
    Returns:
        Configured logger instance
    """
    if name is None:
        name = "loops"
    return logging.getLogger(name)


def get_run_id() -> Optional[str]:
    """Get the current run ID."""
    return _current_run_id


def get_log_file() -> Optional[str]:
    """Get the current log file path."""
    return _current_log_file


def log_message(message: str, level: str = "INFO", logger_name: str = "loops") -> None:
    """Log a message with the current configuration.
    
    Args:
        message: Message to log
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        logger_name: Logger name to use
    """
    logger = logging.getLogger(logger_name)
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(message)


def setup_prefect_logging(run_id: Optional[str] = None) -> dict:
    """Setup logging specifically for Prefect flows.
    
    This ensures Prefect's task logging goes to both file and console.
    
    Args:
        run_id: Unique run identifier
    
    Returns:
        Dictionary with configuration details
    """
    return setup_logging(run_id, "prefect")


class TeeHandler(logging.Handler):
    """Handler that writes to both a file and another handler."""
    
    def __init__(self, file_handler: logging.Handler, console_handler: logging.Handler):
        super().__init__()
        self.file_handler = file_handler
        self.console_handler = console_handler
    
    def emit(self, record: logging.LogRecord) -> None:
        self.file_handler.emit(record)
        self.console_handler.emit(record)
    
    def close(self) -> None:
        self.file_handler.close()
        self.console_handler.close()
