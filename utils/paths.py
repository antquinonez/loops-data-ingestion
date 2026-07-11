"""
Centralized path management for the Loops Data Ingestion Project.

This module provides centralized path configuration with support for:
- Loading paths from config/paths.yaml
- Environment variable overrides (LOOPS_* variables)
- Relative path resolution against project root
- Path validation

Usage:
    from utils.paths import paths
    
    # Access paths as attributes
    data_dir = paths.data_dir
    db_path = paths.database
    
    # Or use get_abs() for explicit absolute paths
    project_root = paths.get_abs("project_root")
    
    # Convenience function for backward compatibility
    from utils.paths import get_project_root
    root = get_project_root()
"""

import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import yaml
import logging

logger = logging.getLogger(__name__)


class PathConfig:
    """Centralized path configuration with fallback support and environment overrides."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize path configuration.
        
        Args:
            config_path: Optional path to paths.yaml config file.
                        If None, will search in standard locations.
        """
        self._config, self._config_dir = self._load_config(config_path)
        self._validate_paths()

    def _load_config(self, config_path: Optional[str]) -> tuple[Dict[str, Any], Path]:
        """Load path configuration from YAML file.
        
        Search order:
        1. Explicit config_path if provided
        2. LOOPS_CONFIG_DIR environment variable + /paths.yaml
        3. Project root (from current file location) + /config/paths.yaml
        4. Current working directory + /config/paths.yaml
        """
        if config_path is not None:
            config_file = Path(config_path).expanduser().resolve()
            if config_file.exists():
                return self._load_yaml(config_file), config_file.parent
            else:
                raise FileNotFoundError(f"Config file not found: {config_path}")
        
        # Try environment variable
        env_config = os.environ.get("LOOPS_CONFIG_DIR")
        if env_config:
            env_path = Path(env_config).expanduser().resolve() / "paths.yaml"
            if env_path.exists():
                return self._load_yaml(env_path), env_path.parent
        
        # Try relative to this module's location
        module_dir = Path(__file__).resolve().parent.parent
        default_config = module_dir / "config" / "paths.yaml"
        if default_config.exists():
            return self._load_yaml(default_config), default_config.parent
        
        # Try current working directory
        cwd_config = Path.cwd() / "config" / "paths.yaml"
        if cwd_config.exists():
            return self._load_yaml(cwd_config), cwd_config.parent
        
        # Last resort: try parent directories
        # This handles cases where the script is run from within the project
        search_paths = [
            Path.cwd() / "config" / "paths.yaml",
            Path.cwd().parent / "config" / "paths.yaml",
            Path.cwd().parent.parent / "config" / "paths.yaml",
        ]
        
        for path in search_paths:
            if path.exists():
                resolved = path.resolve()
                return self._load_yaml(resolved), resolved.parent
        
        raise FileNotFoundError(
            "Could not find paths.yaml configuration. "
            "Please create config/paths.yaml or set LOOPS_CONFIG_DIR environment variable."
        )

    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        """Load YAML file and apply environment overrides."""
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
        
        return self._apply_env_overrides(config)

    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Allow environment variables to override config values.
        
        Environment variables should be prefixed with LOOPS_
        Example: LOOPS_PROJECT_ROOT=/new/path overrides project_root
        """
        overrides = {}
        for key, value in config.items():
            env_key = f"LOOPS_{key.upper()}"
            env_value = os.environ.get(env_key)
            if env_value is not None:
                overrides[key] = env_value
        
        return {**config, **overrides}

    def _validate_paths(self):
        """Validate that essential paths exist or can be created."""
        try:
            project_root = self.get_abs("project_root")
            if not project_root.exists():
                logger.warning(f"Project root does not exist: {project_root}")
            
            # Ensure essential directories exist
            essential_dirs = ["data_dir", "logs_dir", "schemas_dir", "pipelines_dir"]
            for dir_key in essential_dirs:
                if dir_key in self._config:
                    dir_path = self.get_abs(dir_key)
                    if not dir_path.exists():
                        logger.info(f"Creating directory: {dir_path}")
                        dir_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(f"Path validation warning: {e}")

    def get(self, key: str) -> str:
        """Get a path configuration value (as stored in config)."""
        if key not in self._config:
            raise KeyError(f"Path configuration key '{key}' not found")
        return self._config[key]

    def get_abs(self, key: str) -> Path:
        """Get an absolute path, resolving relative paths against project_root.
        
        Args:
            key: Configuration key name
            
        Returns:
            Absolute Path object
        """
        value = self.get(key)

        # Special-case project root to avoid recursive resolution
        if key == "project_root":
            base_dir = self._config_dir or Path.cwd()
            return (base_dir / value).resolve()

        # If it's already an absolute path, return it
        if os.path.isabs(value):
            return Path(value)

        # Otherwise, resolve relative to project root
        project_root = self.get_abs("project_root")
        return (project_root / value).resolve()

    def __getattr__(self, name: str) -> Path:
        """Allow attribute-style access: paths.data_dir, paths.db_path, etc."""
        if name in self._config:
            return self.get_abs(name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def __repr__(self) -> str:
        return f"PathConfig(project_root={self.get('project_root')!r})"

    def list_all(self) -> Dict[str, str]:
        """List all configured paths with their absolute resolutions."""
        return {key: str(self.get_abs(key)) for key in self._config.keys()}


# Global singleton instance
paths = PathConfig()

# Convenience function for backward compatibility
def get_project_root() -> Path:
    """Get the project root path."""
    return paths.get_abs("project_root")

# Ensure project root is in Python path for imports
_project_root = get_project_root()
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# Set PYTHONPATH environment variable
os.environ["PYTHONPATH"] = str(_project_root)
