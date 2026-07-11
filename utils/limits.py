"""
Pipeline Regeneration and Execution Limits Module

This module provides centralized tracking and enforcement of limits for pipeline
generation and execution to prevent infinite loops and resource exhaustion.

Usage:
    from utils.limits import (
        limits_config,
        PipelineAttemptTracker,
        check_limits,
        log_attempt,
        get_backoff_delay,
        is_repeated_error
    )
    
    # Initialize tracker
    tracker = PipelineAttemptTracker()
    
    # Check if we can proceed
    if check_limits(tracker, pipeline_name, 'regeneration'):
        # Proceed with pipeline generation
        pass
    else:
        # Limits exceeded
        print(tracker.get_limit_message(pipeline_name))
"""

import os
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
import yaml

from utils.paths import paths


# Global configuration cache
_limits_config: Optional[Dict[str, Any]] = None


def load_limits_config() -> Dict[str, Any]:
    """Load limits configuration from YAML file."""
    global _limits_config
    
    if _limits_config is not None:
        return _limits_config
    
    # Try to load from config/limits.yaml
    config_path = paths.config_dir / "limits.yaml"
    
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                _limits_config = yaml.safe_load(f)
            return _limits_config
        except Exception as e:
            print(f"Warning: Failed to load limits config: {e}")
    
    # Fallback to reasonable defaults
    _limits_config = {
        'limits': {
            'max_regenerations_per_pipeline': 3,
            'max_executions_per_pipeline': 2,
            'max_total_pipelines': 3,
            'max_total_attempts': 18
        },
        'timeouts': {
            'pipeline_execution': 60,
            'investigation': 300,
            'schema_comparison': 30
        },
        'behavior': {
            'enable_backoff': True,
            'backoff_base_seconds': 1,
            'backoff_max_seconds': 5,
            'detect_repeated_errors': True,
            'log_attempts': True
        }
    }
    
    return _limits_config


def get_limit(name: str, default: Any = None) -> Any:
    """Get a limit value from configuration."""
    config = load_limits_config()
    
    # Navigate through nested config
    parts = name.split('.')
    value = config
    for part in parts:
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return default
    
    return value if value is not None else default


class PipelineAttemptTracker:
    """
    Tracks pipeline regeneration and execution attempts.
    
    This class maintains state for all pipeline attempts during a single run,
    allowing enforcement of limits and detection of repeated errors.
    """
    
    def __init__(self):
        """Initialize the tracker with empty state."""
        self._attempts: Dict[str, Dict[str, Any]] = {}
        self._total_attempts: int = 0
        self._started_at: datetime = datetime.now()
        self._limits = load_limits_config()['limits']
        self._behavior = load_limits_config()['behavior']
        
        # Ensure logs directory exists
        if self._behavior.get('log_attempts', True):
            logs_dir = paths.logs_dir
            logs_dir.mkdir(parents=True, exist_ok=True)
    
    def reset(self):
        """Reset all tracking state."""
        self._attempts = {}
        self._total_attempts = 0
        self._started_at = datetime.now()
    
    def record_attempt(
        self,
        pipeline_name: str,
        attempt_type: str,  # 'regeneration' or 'execution'
        error: Optional[str] = None,
        success: bool = False
    ) -> bool:
        """
        Record a pipeline attempt.
        
        Args:
            pipeline_name: Name of the pipeline (e.g., 'users', 'orders')
            attempt_type: Type of attempt ('regeneration' or 'execution')
            error: Error message if attempt failed
            success: Whether the attempt succeeded
            
        Returns:
            True if attempt was recorded, False if limits exceeded
        """
        # Initialize pipeline tracking if not exists
        if pipeline_name not in self._attempts:
            self._attempts[pipeline_name] = {
                'regeneration_attempts': 0,
                'execution_attempts': 0,
                'last_error': None,
                'last_success': False,
                'errors': []
            }
        
        pipeline = self._attempts[pipeline_name]
        
        # Increment appropriate counter
        if attempt_type == 'regeneration':
            pipeline['regeneration_attempts'] += 1
            counter_name = 'regeneration_attempts'
            max_name = 'max_regenerations_per_pipeline'
        elif attempt_type == 'execution':
            pipeline['execution_attempts'] += 1
            counter_name = 'execution_attempts'
            max_name = 'max_executions_per_pipeline'
        else:
            raise ValueError(f"Unknown attempt type: {attempt_type}")
        
        # Increment total attempts
        self._total_attempts += 1
        
        # Store error if present
        if error:
            pipeline['last_error'] = error
            pipeline['errors'].append({
                'timestamp': datetime.now().isoformat(),
                'type': attempt_type,
                'attempt_num': pipeline[counter_name],
                'error': error
            })
        
        if success:
            pipeline['last_success'] = True
        
        # Log the attempt
        if self._behavior.get('log_attempts', True):
            self._log_attempt_to_file(pipeline_name, attempt_type, error, success)
        
        # Check if limits exceeded
        if not self._check_limits(pipeline_name, attempt_type):
            return False
        
        return True
    
    def _check_limits(self, pipeline_name: str, attempt_type: str) -> bool:
        """Check if limits are exceeded for this attempt."""
        pipeline = self._attempts.get(pipeline_name, {})
        
        # Check per-pipeline limits
        if attempt_type == 'regeneration':
            max_limit = self._limits.get('max_regenerations_per_pipeline', 3)
            current = pipeline.get('regeneration_attempts', 0)
            if current > max_limit:
                return False
        elif attempt_type == 'execution':
            max_limit = self._limits.get('max_executions_per_pipeline', 2)
            current = pipeline.get('execution_attempts', 0)
            if current > max_limit:
                return False
        
        # Check total pipelines limit
        max_pipelines = self._limits.get('max_total_pipelines', 3)
        if len(self._attempts) > max_pipelines:
            return False
        
        # Check total attempts limit
        max_total = self._limits.get('max_total_attempts', 18)
        if self._total_attempts > max_total:
            return False
        
        return True
    
    def get_limits(self, pipeline_name: str) -> Dict[str, int]:
        """Get current attempt counts and limits for a pipeline."""
        pipeline = self._attempts.get(pipeline_name, {})
        
        return {
            'regeneration_attempts': pipeline.get('regeneration_attempts', 0),
            'max_regenerations': self._limits.get('max_regenerations_per_pipeline', 3),
            'execution_attempts': pipeline.get('execution_attempts', 0),
            'max_executions': self._limits.get('max_executions_per_pipeline', 2),
            'total_attempts': self._total_attempts,
            'max_total_attempts': self._limits.get('max_total_attempts', 18),
            'total_pipelines': len(self._attempts),
            'max_pipelines': self._limits.get('max_total_pipelines', 3)
        }
    
    def get_limit_message(self, pipeline_name: str) -> str:
        """Get a human-readable message about limit status."""
        limits = self.get_limits(pipeline_name)
        
        messages = []
        
        # Check regeneration limit
        if limits['regeneration_attempts'] > limits['max_regenerations']:
            messages.append(
                f"Max regenerations ({limits['max_regenerations']}) exceeded "
                f"for {pipeline_name}: {limits['regeneration_attempts']} attempts"
            )
        
        # Check execution limit
        if limits['execution_attempts'] > limits['max_executions']:
            messages.append(
                f"Max executions ({limits['max_executions']}) exceeded "
                f"for {pipeline_name}: {limits['execution_attempts']} attempts"
            )
        
        # Check total attempts limit
        if limits['total_attempts'] > limits['max_total_attempts']:
            messages.append(
                f"Max total attempts ({limits['max_total_attempts']}) exceeded: "
                f"{limits['total_attempts']} attempts"
            )
        
        # Check total pipelines limit
        if limits['total_pipelines'] > limits['max_pipelines']:
            messages.append(
                f"Max pipelines ({limits['max_pipelines']}) exceeded: "
                f"{limits['total_pipelines']} pipelines"
            )
        
        if not messages:
            return f"Within limits for {pipeline_name}"
        
        return " | ".join(messages)
    
    def is_repeated_error(self, pipeline_name: str, error: str) -> bool:
        """
        Check if this is the same error as the last one for this pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            error: Current error message
            
        Returns:
            True if this is a repeated error, False otherwise
        """
        if not self._behavior.get('detect_repeated_errors', True):
            return False
        
        pipeline = self._attempts.get(pipeline_name, {})
        last_error = pipeline.get('last_error', '')
        
        if not last_error:
            return False
        
        # Normalize both errors for comparison
        def normalize(err: str) -> str:
            if not err:
                return ''
            # Replace all whitespace (including newlines, tabs) with single spaces
            import re
            return re.sub(r'\s+', ' ', err.lower()).strip()
        
        return normalize(error) == normalize(last_error)
    
    def get_backoff_delay(self, pipeline_name: str, attempt_type: str) -> float:
        """
        Get the backoff delay before the next attempt.
        
        Args:
            pipeline_name: Name of the pipeline
            attempt_type: Type of attempt ('regeneration' or 'execution')
            
        Returns:
            Delay in seconds
        """
        if not self._behavior.get('enable_backoff', True):
            return 0
        
        pipeline = self._attempts.get(pipeline_name, {})
        
        if attempt_type == 'regeneration':
            attempt_num = pipeline.get('regeneration_attempts', 0)
        else:
            attempt_num = pipeline.get('execution_attempts', 0)
        
        base = self._behavior.get('backoff_base_seconds', 1)
        max_delay = self._behavior.get('backoff_max_seconds', 5)
        
        # Exponential backoff: base * 2^(attempt_num - 1)
        delay = base * (2 ** (attempt_num - 1))
        
        return min(delay, max_delay)
    
    def get_attempt_history(self, pipeline_name: str) -> List[Dict[str, Any]]:
        """Get the attempt history for a specific pipeline."""
        pipeline = self._attempts.get(pipeline_name, {})
        return pipeline.get('errors', [])
    
    def _log_attempt_to_file(
        self,
        pipeline_name: str,
        attempt_type: str,
        error: Optional[str],
        success: bool
    ):
        """Log an attempt to the structured log file."""
        try:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'pipeline': pipeline_name,
                'type': attempt_type,
                'success': success,
                'error': error[:1000] if error else None,  # Truncate long errors
                'total_attempts': self._total_attempts
            }
            
            log_path = paths.logs_dir / "pipeline_attempts.jsonl"
            with open(log_path, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
        except Exception as e:
            # Don't let logging failures break the system
            pass


# Global tracker instance
tracker = PipelineAttemptTracker()


def check_limits(
    tracker: PipelineAttemptTracker,
    pipeline_name: str,
    attempt_type: str
) -> bool:
    """
    Check if a new attempt is allowed.
    
    Args:
        tracker: The PipelineAttemptTracker instance
        pipeline_name: Name of the pipeline
        attempt_type: Type of attempt ('regeneration' or 'execution')
        
    Returns:
        True if the attempt is allowed, False if limits exceeded
    """
    # Record a preliminary attempt to check limits
    # We'll record the real one after this check passes
    return tracker._check_limits(pipeline_name, attempt_type)


def log_attempt(
    tracker: PipelineAttemptTracker,
    pipeline_name: str,
    attempt_type: str,
    error: Optional[str] = None,
    success: bool = False
) -> bool:
    """
    Log a pipeline attempt and check limits.
    
    Args:
        tracker: The PipelineAttemptTracker instance
        pipeline_name: Name of the pipeline
        attempt_type: Type of attempt ('regeneration' or 'execution')
        error: Error message if failed
        success: Whether the attempt succeeded
        
    Returns:
        True if attempt was logged and within limits, False if limits exceeded
    """
    return tracker.record_attempt(pipeline_name, attempt_type, error, success)


def is_repeated_error(
    tracker: PipelineAttemptTracker,
    pipeline_name: str,
    error: str
) -> bool:
    """
    Check if this error has been seen before for this pipeline.
    
    Args:
        tracker: The PipelineAttemptTracker instance
        pipeline_name: Name of the pipeline
        error: The error message to check
        
    Returns:
        True if this is a repeated error
    """
    return tracker.is_repeated_error(pipeline_name, error)


def get_backoff_delay(
    tracker: PipelineAttemptTracker,
    pipeline_name: str,
    attempt_type: str
) -> float:
    """
    Get the backoff delay for the next attempt.
    
    Args:
        tracker: The PipelineAttemptTracker instance
        pipeline_name: Name of the pipeline
        attempt_type: Type of attempt ('regeneration' or 'execution')
        
    Returns:
        Delay in seconds
    """
    return tracker.get_backoff_delay(pipeline_name, attempt_type)


def get_limits_config() -> Dict[str, Any]:
    """Get the full limits configuration."""
    return load_limits_config()


def get_limit_value(name: str, default: Any = None) -> Any:
    """Get a specific limit value from configuration."""
    return get_limit(name, default)
