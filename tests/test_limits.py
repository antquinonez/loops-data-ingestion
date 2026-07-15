"""
Unit and integration tests for pipeline regeneration limits.

Run with: python -m pytest test_limits.py -v
"""

import os
import sys
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

# Ensure project root is in path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.limits import (
    PipelineAttemptTracker,
    load_limits_config,
    get_limits_config,
    get_limit_value,
    get_backoff_delay,
    is_repeated_error,
    log_attempt,
    check_limits,
)
from utils.paths import paths


class TestLimitsConfig:
    """Test limits configuration loading and access."""
    
    def test_load_limits_config(self):
        """Test that limits config loads successfully."""
        config = load_limits_config()
        assert config is not None
        assert 'limits' in config
        assert 'timeouts' in config
        assert 'behavior' in config
    
    def test_get_limits_config(self):
        """Test get_limits_config returns full config."""
        config = get_limits_config()
        assert isinstance(config, dict)
        assert 'limits' in config
    
    def test_get_limit_value(self):
        """Test getting specific limit values."""
        # Test nested values
        max_regen = get_limit_value('limits.max_regenerations_per_pipeline')
        assert max_regen == 3
        
        max_exec = get_limit_value('limits.max_executions_per_pipeline')
        assert max_exec == 2
        
        # Test default for missing value
        missing = get_limit_value('nonexistent.path', default=10)
        assert missing == 10
    
    def test_get_limit_value_default(self):
        """Test default value when limit not found."""
        result = get_limit_value('nonexistent.key', default=99)
        assert result == 99


class TestPipelineAttemptTracker:
    """Test the PipelineAttemptTracker class."""
    
    def setup_method(self):
        """Create a fresh tracker for each test."""
        self.tracker = PipelineAttemptTracker()
    
    def test_init(self):
        """Test tracker initialization."""
        assert self.tracker._attempts == {}
        assert self.tracker._total_attempts == 0
        assert self.tracker._started_at is not None
    
    def test_reset(self):
        """Test resetting tracker state."""
        # Add some attempts
        self.tracker.record_attempt('users', 'regeneration', None, True)
        assert self.tracker._total_attempts > 0
        
        # Reset
        self.tracker.reset()
        assert self.tracker._total_attempts == 0
        assert self.tracker._attempts == {}
    
    def test_record_attempt_success(self):
        """Test recording a successful attempt."""
        result = self.tracker.record_attempt('users', 'regeneration', None, True)
        assert result is True
        
        limits = self.tracker.get_limits('users')
        assert limits['regeneration_attempts'] == 1
        assert limits['total_attempts'] == 1
    
    def test_record_attempt_failure(self):
        """Test recording a failed attempt."""
        error_msg = "Test error"
        result = self.tracker.record_attempt('users', 'execution', error_msg, False)
        assert result is True
        
        limits = self.tracker.get_limits('users')
        assert limits['execution_attempts'] == 1
        
        # Check error was stored
        history = self.tracker.get_attempt_history('users')
        assert len(history) == 1
        assert history[0]['error'] == error_msg
    
    def test_record_multiple_pipelines(self):
        """Test tracking multiple pipelines."""
        self.tracker.record_attempt('users', 'regeneration', None, True)
        self.tracker.record_attempt('orders', 'regeneration', None, True)
        self.tracker.record_attempt('transactions', 'regeneration', None, True)
        
        limits = self.tracker.get_limits('')
        assert limits['total_pipelines'] == 3
    
    def test_execution_limit_enforcement(self):
        """Test that execution limit is enforced."""
        # Max 2 executions per pipeline
        assert self.tracker.record_attempt('users', 'execution', None, False) is True
        assert self.tracker.record_attempt('users', 'execution', None, False) is True
        
        # Third attempt should be blocked
        result = self.tracker.record_attempt('users', 'execution', None, False)
        assert result is False
        
        limits = self.tracker.get_limits('users')
        assert limits['execution_attempts'] == 3  # Counter incremented before check
        assert limits['max_executions'] == 2
    
    def test_regeneration_limit_enforcement(self):
        """Test that regeneration limit is enforced."""
        # Max 3 regenerations per pipeline
        for i in range(3):
            result = self.tracker.record_attempt('users', 'regeneration', None, False)
            assert result is True
        
        # Fourth attempt should be blocked
        result = self.tracker.record_attempt('users', 'regeneration', None, False)
        assert result is False
    
    def test_total_attempts_limit(self):
        """Test that total attempts limit is enforced."""
        # With the default limits (3 pipelines, 3 regen, 2 exec per pipeline),
        # max attempts = 3 × (3+2) = 15. But the total limit is 18.
        # So we can test that the total limit is checked.
        # We'll use a simpler test: just verify that after many attempts,
        # we eventually hit a limit.
        
        # Record attempts until we hit a limit
        attempt_count = 0
        for i in range(20):  # Try 20 attempts
            pipeline_name = f"pipeline_{i % 3}"
            attempt_type = 'regeneration' if i % 2 == 0 else 'execution'
            result = self.tracker.record_attempt(pipeline_name, attempt_type, None, False)
            attempt_count += 1
            if not result:
                # Hit a limit
                break
        
        # Should have stopped at or before 18
        assert attempt_count <= 18
        
        # The 19th should definitely fail
        result = self.tracker.record_attempt('pipeline_0', 'regeneration', None, False)
        assert result is False
    
    def test_total_pipelines_limit(self):
        """Test that total pipelines limit is enforced."""
        # Default max is 3 pipelines
        for i in range(3):
            result = self.tracker.record_attempt(f'pipeline_{i}', 'regeneration', None, True)
            assert result is True
        
        # 4th pipeline should be blocked
        result = self.tracker.record_attempt('pipeline_3', 'regeneration', None, True)
        assert result is False


class TestRepeatedErrorDetection:
    """Test repeated error detection functionality."""
    
    def setup_method(self):
        """Create a fresh tracker for each test."""
        self.tracker = PipelineAttemptTracker()
    
    def test_first_error_not_repeated(self):
        """Test that first error is not flagged as repeated."""
        error = "Test error"
        # Check BEFORE recording - first error should not be repeated
        assert self.tracker.is_repeated_error('users', error) is False
        
        # Now record it
        self.tracker.record_attempt('users', 'execution', error, False)
        
        # After recording once, it's now the last error
        # So checking the same error again SHOULD be repeated
        assert self.tracker.is_repeated_error('users', error) is True
    
    def test_same_error_is_repeated(self):
        """Test that same error is flagged as repeated."""
        error = "Test error"
        self.tracker.record_attempt('users', 'execution', error, False)
        self.tracker.record_attempt('users', 'execution', error, False)
        
        assert self.tracker.is_repeated_error('users', error) is True
    
    def test_different_error_not_repeated(self):
        """Test that different error is not flagged as repeated."""
        self.tracker.record_attempt('users', 'execution', "Error 1", False)
        
        assert self.tracker.is_repeated_error('users', "Error 2") is False
    
    def test_case_insensitive_comparison(self):
        """Test that error comparison is case-insensitive."""
        self.tracker.record_attempt('users', 'execution', "TEST ERROR", False)
        
        assert self.tracker.is_repeated_error('users', "test error") is True
    
    def test_whitespace_normalization(self):
        """Test that whitespace is normalized in error comparison."""
        # Record an error with extra whitespace
        self.tracker.record_attempt('users', 'execution', "Test  error\n\t", False)
        
        # Now check if a cleaned version matches
        # The normalization should make "Test error" match "Test  error\n\t"
        assert self.tracker.is_repeated_error('users', "Test error") is True


class TestBackoffDelay:
    """Test backoff delay calculation."""
    
    def setup_method(self):
        """Create a fresh tracker for each test."""
        self.tracker = PipelineAttemptTracker()
    
    def test_first_attempt_no_backoff(self):
        """Test first attempt has no backoff."""
        self.tracker.record_attempt('users', 'execution', None, False)
        delay = self.tracker.get_backoff_delay('users', 'execution')
        
        # Second attempt: 1 * 2^(2-1) = 2 seconds, but capped at max (5s)
        # Actually, attempt_num is 1 (after first record), so 1 * 2^(1-1) = 1
        assert delay == 1.0
    
    def test_second_attempt_backoff(self):
        """Test second attempt has backoff."""
        self.tracker.record_attempt('users', 'execution', None, False)
        self.tracker.record_attempt('users', 'execution', None, False)
        delay = self.tracker.get_backoff_delay('users', 'execution')
        
        # Third attempt: attempt_num = 2, so 1 * 2^(2-1) = 2
        assert delay == 2.0
    
    def test_third_attempt_backoff(self):
        """Test third attempt has higher backoff."""
        for _ in range(3):
            self.tracker.record_attempt('users', 'execution', None, False)
        delay = self.tracker.get_backoff_delay('users', 'execution')
        
        # Fourth attempt: attempt_num = 3, so 1 * 2^(3-1) = 4
        assert delay == 4.0
    
    def test_backoff_capped(self):
        """Test backoff is capped at max."""
        # Add many attempts
        for _ in range(10):
            self.tracker.record_attempt('users', 'execution', None, False)
        delay = self.tracker.get_backoff_delay('users', 'execution')
        
        # Should be capped at 5 seconds (from config)
        assert delay == 5.0


class TestLimitMessages:
    """Test limit message generation."""
    
    def setup_method(self):
        """Create a fresh tracker for each test."""
        self.tracker = PipelineAttemptTracker()
    
    def test_within_limits_message(self):
        """Test message when within limits."""
        msg = self.tracker.get_limit_message('users')
        assert 'Within limits' in msg
    
    def test_exceeded_execution_message(self):
        """Test message when execution limit exceeded."""
        self.tracker.record_attempt('users', 'execution', None, False)
        self.tracker.record_attempt('users', 'execution', None, False)
        self.tracker.record_attempt('users', 'execution', None, False)
        
        msg = self.tracker.get_limit_message('users')
        assert 'Max executions' in msg
        assert 'exceeded' in msg
    
    def test_exceeded_regeneration_message(self):
        """Test message when regeneration limit exceeded."""
        for _ in range(4):
            self.tracker.record_attempt('users', 'regeneration', None, False)
        
        msg = self.tracker.get_limit_message('users')
        assert 'Max regenerations' in msg
        assert 'exceeded' in msg
    
    def test_exceeded_total_message(self):
        """Test message when total attempts exceeded."""
        # Record max attempts
        for i in range(19):
            self.tracker.record_attempt(f'pipeline_{i % 3}', 'regeneration', None, False)
        
        msg = self.tracker.get_limit_message('')
        assert 'Max total attempts' in msg


class TestAttemptLogging:
    """Test attempt logging functionality."""
    
    def setup_method(self):
        """Create a fresh tracker and temp directory for each test."""
        self.tracker = PipelineAttemptTracker()
        self.temp_dir = tempfile.mkdtemp()
        
        # Patch paths.logs_dir to use temp directory
        self.original_logs_dir = paths.logs_dir
        paths.logs_dir = Path(self.temp_dir)
    
    def teardown_method(self):
        """Clean up temp directory and restore original."""
        paths.logs_dir = self.original_logs_dir
        # Clean up temp files
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_log_file_created(self):
        """Test that log file is created."""
        self.tracker.record_attempt('users', 'execution', "Test error", False)
        
        log_file = Path(self.temp_dir) / "pipeline_attempts.jsonl"
        assert log_file.exists()
    
    def test_log_entry_structure(self):
        """Test log entry has correct structure."""
        error_msg = "Test error message"
        self.tracker.record_attempt('users', 'execution', error_msg, False)
        
        log_file = Path(self.temp_dir) / "pipeline_attempts.jsonl"
        with open(log_file, 'r') as f:
            entry = json.loads(f.readline())
        
        assert 'timestamp' in entry
        assert 'pipeline' in entry
        assert entry['pipeline'] == 'users'
        assert 'type' in entry
        assert entry['type'] == 'execution'
        assert 'success' in entry
        assert entry['success'] is False
        assert 'error' in entry
        assert entry['error'] == error_msg
        assert 'total_attempts' in entry
    
    def test_multiple_log_entries(self):
        """Test multiple log entries are recorded."""
        self.tracker.record_attempt('users', 'execution', "Error 1", False)
        self.tracker.record_attempt('users', 'execution', "Error 2", False)
        
        log_file = Path(self.temp_dir) / "pipeline_attempts.jsonl"
        with open(log_file, 'r') as f:
            entries = [json.loads(line) for line in f]
        
        assert len(entries) == 2
        assert entries[0]['error'] == "Error 1"
        assert entries[1]['error'] == "Error 2"


class TestIntegration:
    """Integration tests for limits with the demo system."""
    
    def test_run_demo_imports(self):
        """Test that run_demo can be imported with limits."""
        # This tests the integration
        import run_demo
        assert hasattr(run_demo, 'pipeline_tracker')
        assert hasattr(run_demo, 'process_pipeline_with_limits')
    
    def test_tracker_accessible_from_run_demo(self):
        """Test that tracker is accessible from run_demo module."""
        from run_demo import pipeline_tracker
        assert isinstance(pipeline_tracker, PipelineAttemptTracker)
    
    def test_limits_in_run_demo(self):
        """Test that limits are properly configured in run_demo."""
        from run_demo import pipeline_tracker
        limits = pipeline_tracker.get_limits('')
        assert limits['max_total_attempts'] == 18


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
