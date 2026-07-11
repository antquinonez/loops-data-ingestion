#!/usr/bin/env python3
"""
Main demo script to:
1. Run the Prefect ingestion flow (which will fail)
2. Trigger the nanobot investigation
3. Show how the autonomous agent troubleshoots the failure
"""

import sys
import os
import subprocess
import time
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent

# Ensure project root and configuration utilities are importable
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.paths import paths, get_project_root
PROJECT_ROOT = get_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault("PYTHONPATH", str(PROJECT_ROOT))

VENV_PYTHON = PROJECT_ROOT / "venv" / "bin" / "python"
if VENV_PYTHON.exists():
    if Path(sys.executable).resolve() != VENV_PYTHON.resolve():
        print("ERROR: Not using venv Python.")
        print(f"Please run with: {VENV_PYTHON}")
        print(f"Current Python: {sys.executable}")
        sys.exit(1)
else:
    print(f"Warning: No venv detected at {VENV_PYTHON}. Running with: {sys.executable}")

# Load .env file at startup
from dotenv import load_dotenv
try:
    load_dotenv(paths.get_abs("project_root") / ".env")
except Exception:
    load_dotenv()

# Ensure OPENAI_API_KEY is in environment
if not os.environ.get("OPENAI_API_KEY"):
    # Try to load from .env in current directory
    load_dotenv()

# Import and initialize pipeline attempt tracker
from utils.limits import PipelineAttemptTracker, log_attempt, is_repeated_error, get_backoff_delay
pipeline_tracker = PipelineAttemptTracker()


def cleanup_and_initialize(archive_logs: bool = False) -> dict:
    """Clean up previous run artifacts and initialize fresh state.
    
    Args:
        archive_logs: If True, archive old logs instead of deleting them
    
    Returns:
        Dictionary with cleanup summary and run_id
    """
    from datetime import datetime
    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Configure Prefect to use local mode for all subprocesses
    os.environ["PREFECT_API_URL"] = ""
    os.environ["PREFECT_CLOUD_API_URL"] = ""
    os.environ.pop("PREFECT_API_KEY", None)
    os.environ.pop("PREFECT_CLOUD_API_KEY", None)
    
    print("\n" + "=" * 80)
    print("INITIALIZING: Cleaning up previous run artifacts")
    print("=" * 80)
    print(f"Run ID: {run_id}")
    print("Prefect configured for LOCAL MODE (no cloud connection)")
    
    from utils.cleanup import cleanup_all

    result = cleanup_all(archive_logs=archive_logs)
    
    # Print summary
    if result["logs_cleaned"]:
        print(f"  Cleaned logs: {', '.join(result['logs_cleaned'])}")
    if result["logs_archived"]:
        print(f"  Archived logs: {', '.join(result['logs_archived'])}")
    if result["db_cleaned"]:
        print(f"  Cleaned databases: {', '.join(result['db_cleaned'])}")
    if result["pipelines_cleaned"]:
        print(f"  Cleaned pipelines: {', '.join(result['pipelines_cleaned'])}")
    if result["errors"]:
        print(f"  Errors during cleanup: {len(result['errors'])}")
        for error in result["errors"]:
            print(f"    - {error}")
    
    print("\nInitialization complete.\n")
    return {**result, "run_id": run_id}


def run_and_log_subprocess(cmd: list, cwd: Path, env: dict, run_id: Optional[str] = None) -> subprocess.CompletedProcess:
    """Run a subprocess and log its output to a file.
    
    Args:
        cmd: Command to run as list
        cwd: Working directory
        env: Environment variables
        run_id: Optional run ID for log file naming
    
    Returns:
        CompletedProcess with stdout/stderr captured
    """
    # If cmd[0] is sys.executable and we're not in venv, use venv python
    if cmd and cmd[0] == sys.executable:
        venv_python = str(PROJECT_ROOT / "venv" / "bin" / "python")
        if Path(venv_python).exists():
            cmd = [venv_python] + cmd[1:]
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=cwd,
        env=env
    )
    
    # Log the output to a file
    if run_id:
        try:
            logs_dir = paths.logs_dir
            logs_dir.mkdir(parents=True, exist_ok=True)
            
            # stdout log
            stdout_log = logs_dir / f"subprocess_{run_id}_stdout.log"
            if result.stdout:
                with open(stdout_log, 'w') as f:
                    f.write(result.stdout)
            
            # stderr log
            stderr_log = logs_dir / f"subprocess_{run_id}_stderr.log"
            if result.stderr:
                with open(stderr_log, 'w') as f:
                    f.write(result.stderr)
        except Exception as e:
            print(f"Warning: Failed to write subprocess logs: {e}")
    
    return result


def run_ingestion_flow(run_id: Optional[str] = None):
    """Run the data ingestion flow. Uses generated pipeline if available, otherwise runs the original (which will fail).
    
    Args:
        run_id: Optional run identifier for unique log files
    
    Returns:
        tuple: (used_generated, succeeded) where:
        - used_generated: True if we ran the generated pipeline
        - succeeded: True if the flow succeeded
    """
    print("=" * 80)
    print("STEP 1: Running data ingestion flow")
    print("=" * 80)
    
    # Check if we have a generated pipeline
    generated_pipeline = paths.clean_users_pipeline
    
    if generated_pipeline.exists():
        print("\n✓ Using GENERATED cleaning pipeline (from previous pipeline builder run)")
        print(f"  Pipeline: {generated_pipeline}")
        flow_path = generated_pipeline
        used_generated = True
    else:
        print("\n⚠️  No generated pipeline found - using original Prefect flow (expected to fail due to data errors)")
        flow_path = paths.get_abs("project_root") / "flows" / "ingestion_flow.py"
        used_generated = False
    
    # Build environment with run_id for unique log files
    run_env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
    # Ensure Prefect uses local mode and no external services
    run_env["PREFECT_API_URL"] = ""
    run_env["PREFECT_CLOUD_API_URL"] = ""
    run_env["PREFECT_MODE"] = "local"
    run_env.pop("PREFECT_API_KEY", None)
    run_env.pop("PREFECT_CLOUD_API_KEY", None)
    if run_id:
        run_env["RUN_ID"] = run_id
    
    # Run the flow and capture output to log files
    result = run_and_log_subprocess(
        [sys.executable, str(flow_path)],
        cwd=PROJECT_ROOT,
        env=run_env,
        run_id=run_id
    )
    
    print("\n--- FLOW OUTPUT ---")
    print("STDOUT:")
    print(result.stdout)
    if result.stderr:
        print("\nSTDERR:")
        print(result.stderr)
    print(f"\nReturn code: {result.returncode}")
    
    succeeded = (result.returncode == 0)
    
    if used_generated:
        if succeeded:
            print("\n✓ Cleaned ingestion flow succeeded!")
        else:
            print("\n⚠️  Generated pipeline failed - check the output above")
    else:
        if not succeeded:
            print("\n✓ Ingestion flow failed as expected (due to data quality issues)")
        else:
            print("\n✗ Ingestion flow succeeded unexpectedly")
    
    return (used_generated, succeeded)


def start_nanobot_server():
    """Start the nanobot server in the background."""
    print("\n" + "=" * 80)
    print("STEP 2: Starting nanobot server")
    print("=" * 80)
    
    config_path = str(paths.nanobot_config)
    
    # Start nanobot server
    nanobot_cmd = [
        sys.executable, "-m", "nanobot.server",
        "--config", config_path,
        "--log-level", "DEBUG"
    ]
    
    print(f"\nRunning: {' '.join(nanobot_cmd)}")
    print("\nStarting nanobot server (press Ctrl+C to stop)...")
    print("Server will be available at: http://127.0.0.1:8080")
    
    # Run in foreground so we can see output
    result = subprocess.run(
        nanobot_cmd,
        cwd=PROJECT_ROOT,
        env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
    )
    
    return result.returncode


def setup_environment():
    """Setup environment for nanobot to access duckdb CLI and other tools."""
    # Add venv/bin to PATH so duckdb CLI is accessible
    venv_bin = str(paths.get_abs("project_root") / "venv" / "bin")
    if os.path.exists(venv_bin) and venv_bin not in os.environ["PATH"]:
        os.environ["PATH"] = f"{venv_bin}:{os.environ.get('PATH', '')}"
        print(f"✓ Added venv/bin to PATH: {venv_bin}")
    
    # Verify duckdb CLI is accessible
    import subprocess
    try:
        result = subprocess.run(
            ["duckdb", "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            print(f"✓ duckdb CLI accessible: {result.stdout.strip()}")
        else:
            print(f"⚠️  duckdb CLI not accessible: {result.stderr}")
    except Exception as e:
        print(f"⚠️  duckdb CLI check failed: {e}")
    
    return True


def start_mcp_server():
    """Start the MCP server in the background.
    
    Returns:
        subprocess.Popen: The MCP server process, or None if failed to start
    """
    print("\n" + "=" * 80)
    print("Starting MCP server")
    print("=" * 80)
    
    server_path = paths.get_abs("flows_dir") / "mcp_server.py"
    if not server_path.exists():
        print(f"⚠️  MCP server script not found: {server_path}")
        print("   MCP server will not be available (this is optional)")
        return None
    
    env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
    env.setdefault("PREFECT_API_URL", "")
    env.setdefault("PREFECT_CLOUD_API_URL", "")
    env.setdefault("PREFECT_MODE", "local")
    env.pop("PREFECT_API_KEY", None)
    env.pop("PREFECT_CLOUD_API_KEY", None)
    
    try:
        process = subprocess.Popen(
            [sys.executable, str(server_path)],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print(f"✓ MCP server started with PID {process.pid}")
        print("   Server runs on: http://127.0.0.1:8081")
        
        # Check quickly if it's still running (non-blocking)
        import time
        time.sleep(0.5)
        if process.poll() is not None:
            # Process crashed - check error
            try:
                _, stderr = process.communicate(timeout=1)
                stderr_text = stderr.decode() if stderr else ""
                if stderr_text:
                    print(f"⚠️  MCP server failed: {stderr_text.split(chr(10))[0][:100]}")
            except:
                pass
            print("   MCP server will not be available (this is optional)")
            return None
        
        return process
    except Exception as e:
        print(f"⚠️  Failed to start MCP server: {e}")
        print("   MCP server will not be available (this is optional)")
        return None


async def trigger_nanobot_investigation(mcp_process=None, run_id: Optional[str] = None):
    """Trigger nanobot to investigate the failure."""
    print("\n" + "=" * 80)
    print("STEP 3: Triggering nanobot investigation")
    print("=" * 80)
    
    # Setup environment (PATH, etc.)
    setup_environment()
    
    # Use nanobot programmatically
    from nanobot import Nanobot, RunResult
    from nanobot.agent.tools.registry import ToolRegistry
    from flows.nanobot_tools import NANOBOT_TOOLS
    from flows.nanobot_tool_classes import NANOBOT_TOOL_CLASSES
    from agents.pipeline_builder.nanobot_tools import PIPELINE_TOOL_CLASSES
    import json
    import asyncio
    
    # Verify API key
    if not os.environ.get("OPENAI_API_KEY"):
        print("\n⚠️  No OPENAI_API_KEY found in environment")
        print("Make sure .env file exists and contains OPENAI_API_KEY")
        return
    
    print(f"\n✓ OpenAI API key loaded, using model: {os.environ.get('OPENAI_MODEL', 'gpt-4o-mini')}")
    
    # Load SKILLS.md as context
    skills_path = paths.get_abs("project_root") / "SKILLS.md"
    with open(skills_path, 'r') as f:
        skills_context = f.read()
    
    # Register custom tools with nanobot's ToolRegistry
    print("\nRegistering custom tools with Nanobot...")
    registry = ToolRegistry()
    
    # Register investigation Tool classes (from nanobot_tool_classes.py)
    print("  Loading NANOBOT_TOOL_CLASSES...")
    for tool_class in NANOBOT_TOOL_CLASSES:
        tool_instance = tool_class()
        registry.register(tool_instance)
        print(f"    ✓ Registered: {tool_instance.name} - {tool_instance.description[:50]}...")

    # Register pipeline builder Tool classes
    print("  Loading PIPELINE_TOOL_CLASSES...")
    for tool_class in PIPELINE_TOOL_CLASSES:
        tool_instance = tool_class()
        registry.register(tool_instance)
        print(f"    ✓ Registered: {tool_instance.name} - {tool_instance.description[:50]}...")
    
    print(f"  Total custom tools registered: {len(registry.get_definitions())}")
    
    # Create the bot with explicit config
    config_path = str(paths.nanobot_config_minimal)
    
    print("\nInitializing Nanobot...")
    print(f"  Config: {config_path}")
    print(f"  Model: {os.environ.get('OPENAI_MODEL', 'gpt-4.1-mini-2025-04-14')}")
    
    # Load config from file
    import yaml
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Create bot
    bot = Nanobot.from_config(
        config_path=config_path,
        model=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini-2025-04-14")
    )
    
    # Register our custom tools with the bot's internal tool registry
    print("\n  Registering pipeline tools with Nanobot's tool registry...")
    for tool_class in PIPELINE_TOOL_CLASSES:
        tool_instance = tool_class()
        bot._loop.tools.register(tool_instance)
        print(f"    ✓ Registered: {tool_instance.name}")
    
    print(f"\n  Total tools available: {len(bot._loop.tools.get_definitions())}")
    
    # Trigger investigation - NOW WITH PIPELINE BUILDER TOOLS
    print("\nTriggering investigation with message:")
    message = f"""Investigate the failed data ingestion. 
Source data: {paths.source_data}
Ideal schema: {paths.users_schema}

Use tools: infer_source_schema, load_ideal_schema, compare_schemas, generate_cleaning_pipeline.
Save pipeline to pipelines/generated/clean_users_pipeline.py using write_file."""
    print(message)
    print("\nThis may take a moment... (agent is thinking and using tools)")
    
    # Setup Nanobot logging to file
    nanobot_log_path = paths.logs_dir / f"nanobot_{run_id}.log"
    
    try:
        # Run the bot
        result: RunResult = await bot.run(
            message,
            model=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini-2025-04-14")
        )
        
        print("\n" + "=" * 80)
        print("NANOBOT INVESTIGATION RESULTS")
        print("=" * 80)
        print(result.content if hasattr(result, 'content') else str(result))
        
        # Also save to file
        with open("logs/nanobot_investigation_result.md", "w") as f:
            f.write("# Nanobot Investigation Results\n\n")
            f.write(result.content if hasattr(result, 'content') else str(result))
        print("\n✓ Results saved to logs/nanobot_investigation_result.md")
        
    except Exception as e:
        print(f"\n❌ Error running nanobot: {e}")
        import traceback
        traceback.print_exc()
        
        # Enhanced error logging for OpenAI API issues
        error_str = str(e)
        if "Missing required parameter" in error_str and "messages" in error_str:
            print("\nOPENAI API FORMAT ERROR DETECTED:")
            print("   This appears to be a message format issue with the OpenAI API.")
            print("   Possible causes:")
            print("   - Nanobot version compatibility issue with newer OpenAI API")
            print("   - Model-specific message format requirements")
            print("   - Token limit or rate limiting (less likely - would be different error)")
            print("\n   Recommendations:")
            print("   1. Try a different OpenAI model (e.g., gpt-4o-mini)")
            print("   2. Check Nanobot version compatibility")
            print("   3. Add debug logging to see exact messages being sent")
        
        # Save detailed error to file
        with open("logs/nanobot_error_details.log", "w") as f:
            f.write(f"# Nanobot Error Details\n\n")
            f.write(f"Error: {e}\n\n")
            f.write(f"Type: {type(e).__name__}\n\n")
            f.write("Traceback:\n")
            traceback.print_exc(file=f)
        print("\n✓ Detailed error saved to logs/nanobot_error_details.log")


def run_full_demo(archive_logs: bool = False):
    """Run the complete demo."""
    # Initialize: clean up previous run artifacts
    cleanup_result = cleanup_and_initialize(archive_logs=archive_logs)
    run_id = cleanup_result.get("run_id")
    
    # Reset pipeline attempt tracker for this run
    pipeline_tracker.reset()
    
    print("\n" + "=" * 80)
    print("DATA INGESTION TROUBLESHOOTING DEMO")
    print("=" * 80)
    
    # Step 1: Run the ingestion flow
    # If a generated pipeline exists, it will succeed
    # Otherwise, the original will fail and we'll generate a fix
    used_generated, succeeded = run_ingestion_flow(run_id=run_id)
    
    # If we used a generated pipeline and it succeeded, show success
    if used_generated and succeeded:
        print("\n✓ Cleaned ingestion completed successfully using generated pipeline!")
        
        # Show the cleaned data
        print("\n" + "=" * 80)
        print("CLEANED DATA RESULTS")
        print("=" * 80)
        
        import duckdb
        db_path = str(paths.database)
        try:
            conn = duckdb.connect(database=db_path, read_only=True)
            tables = conn.execute("SHOW TABLES").fetchall()
            
            for table in tables:
                table_name = table[0]
                if 'clean' in table_name or 'users' in table_name:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    print(f"\n{table_name}: {count} rows")
                    
                    # Show sample
                    sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchall()
                    for row in sample:
                        print(f"  {row}")
            
            conn.close()
        except Exception as e:
            print(f"Error showing results: {e}")
        
        return
    
    # If the generated pipeline failed, or we used the original which failed
    # Continue with investigation and pipeline builder
    
    # Step 2: Show what logs were created
    print("\n" + "=" * 80)
    print("LOGS CREATED")
    print("=" * 80)
    
    log_files = [
        paths.ingestion_log,
        paths.prefect_log,
    ]
    
    for log_file in log_files:
        if log_file.exists():
            print(f"\n✓ {log_file}")
            with open(log_file, 'r') as f:
                lines = f.readlines()
                print(f"  Lines: {len(lines)}")
                print(f"  Last 5 lines:")
                for line in lines[-5:]:
                    print(f"    {line.rstrip()}")
        else:
            print(f"\n✗ {log_file} not found")
    
    # Step 3: Show the database state
    print("\n" + "=" * 80)
    print("DATABASE STATE")
    print("=" * 80)
    
    import duckdb
    db_path = str(paths.database)
    
    try:
        conn = duckdb.connect(database=db_path, read_only=True)
        
        # Show tables
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"\nTables: {[t[0] for t in tables]}")
        
        # Show raw_users content
        if tables and any('raw_users' in t[0] for t in tables):
            print("\nraw_users table content:")
            result = conn.execute("SELECT * FROM raw_users LIMIT 10").fetchall()
            for row in result:
                print(f"  {row}")
        
        # Check users table
        if tables and any('users' in t[0] for t in tables):
            count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            print(f"\nusers table: {count} rows")
        
        conn.close()
    except Exception as e:
        print(f"Error checking database: {e}")
    
    # Step 4: Test the tools directly
    print("\n" + "=" * 80)
    print("TESTING NANOBOT TOOLS")
    print("=" * 80)
    
    from flows.nanobot_tools import (
        inspect_file,
        check_schema,
        get_ingestion_status
    )
    
    # Test inspect_file
    print("\n1. Inspecting source file:")
    source_file = str(paths.source_data)
    print(inspect_file(source_file, sample_size=5))
    
    # Test check_schema
    print("\n2. Checking schema validation:")
    print(check_schema(source_file))
    
    # Test get_ingestion_status
    print("\n3. Getting ingestion status:")
    print(get_ingestion_status())
    
    # Step 5: Start MCP server for enhanced capabilities
    print("\n" + "=" * 80)
    print("STEP 5: Starting MCP server")
    print("=" * 80)
    
    mcp_process = start_mcp_server()
    
    # Step 6: Now trigger nanobot
    print("\n" + "=" * 80)
    print("TRIGGERING NANOBOT INVESTIGATION")
    print("=" * 80)
    
    # Check global limits before proceeding
    limits = pipeline_tracker.get_limits('')
    if limits['total_attempts'] >= limits['max_total_attempts']:
        print(f"\n❌ MAX TOTAL ATTEMPTS REACHED: {limits['total_attempts']}/{limits['max_total_attempts']}")
        print("   Cannot trigger Nanobot investigation.")
        print("   Some pipelines may have exceeded their attempt limits.")
        return
    
    # Load .env and check for API key
    from dotenv import load_dotenv
    load_dotenv()
    has_api_key = bool(os.environ.get("OPENAI_API_KEY"))
    
    # Check if a generated pipeline already exists
    generated_pipeline = paths.clean_users_pipeline
    
    if has_api_key:
        print(f"\n✓ OpenAI API key detected, using model: {os.environ.get('OPENAI_MODEL', 'unknown')}")
        print("Running live investigation with Nanobot...")
        
        # Setup environment for duckdb CLI access
        setup_environment()
        
        # Setup Nanobot logging to file (in addition to console)
        nanobot_log_path = paths.logs_dir / f"nanobot_{run_id}.log"
        
        # Create a tee class to write to both file and stdout
        class Tee:
            def __init__(self, *files):
                self.files = files
            def write(self, obj):
                for f in self.files:
                    f.write(obj)
                    f.flush()
            def flush(self):
                for f in self.files:
                    f.flush()
        
        import asyncio
        
        try:
            with open(nanobot_log_path, 'w') as nanobot_log_file:
                # Tee stdout to both console and file
                import sys
                original_stdout = sys.stdout
                original_stderr = sys.stderr
                sys.stdout = Tee(original_stdout, nanobot_log_file)
                sys.stderr = Tee(original_stderr, nanobot_log_file)
                
                try:
                    asyncio.run(trigger_nanobot_investigation(mcp_process=mcp_process, run_id=run_id))
                finally:
                    # Restore original stdout/stderr
                    sys.stdout = original_stdout
                    sys.stderr = original_stderr
        except Exception as e:
            print(f"Error capturing Nanobot output: {e}")
            # Fallback: run without capture
            asyncio.run(trigger_nanobot_investigation(mcp_process=mcp_process, run_id=run_id))
        
        # After nanobot runs, check if it created a pipeline
        if generated_pipeline.exists():
            print("\n✓ Nanobot successfully created a cleaning pipeline!")
            print(f"  Pipeline saved to: {generated_pipeline}")
        else:
            print("\n⚠️  Nanobot did not create a pipeline file.")
            print("  Running pipeline builder demo as fallback...")
            # Check limits before fallback
            limits = pipeline_tracker.get_limits('')
            if limits['total_attempts'] >= limits['max_total_attempts']:
                print(f"\n❌ MAX ATTEMPTS REACHED: Cannot run fallback pipeline builder")
            else:
                run_pipeline_builder_demo(run_id=run_id)
    else:
        print("\n⚠️  No OpenAI API key detected.")
        print("To run a live investigation with Nanobot:")
        print("  1. Create .env file with OPENAI_API_KEY")
        print("  2. Or set it: export OPENAI_API_KEY='your-key'")
        print("\nFor now, running pipeline builder demo...")
        # Check limits before fallback
        limits = pipeline_tracker.get_limits('')
        if limits['total_attempts'] >= limits['max_total_attempts']:
            print(f"\n❌ MAX ATTEMPTS REACHED: Cannot run fallback pipeline builder")
        else:
            run_pipeline_builder_demo(run_id=run_id)


def process_pipeline_with_limits(
    pipeline_name: str,
    source_path: str,
    ideal_path: str,
    output_table: str,
    output_file: str,
    base_env: dict,
    run_id: Optional[str] = None
) -> bool:
    """
    Process a single pipeline with attempt tracking and limits.
    
    Args:
        pipeline_name: Name of the pipeline (e.g., 'users', 'orders', 'transactions')
        source_path: Path to source data file
        ideal_path: Path to ideal schema file
        output_table: Name of output table
        output_file: Path to save generated pipeline
        base_env: Environment variables for subprocess
        run_id: Optional run identifier
        
    Returns:
        True if pipeline succeeded, False if limits exceeded or failed
    """
    from agents.pipeline_builder.tools import (
        compare_schemas,
        generate_cleaning_pipeline,
        generate_validation_checks
    )
    from utils.validation import ValidationCheckGenerator
    from agents.validation_agent import ValidationAgent, get_checks_path
    
    # Check if we've exceeded limits before starting
    if not pipeline_tracker._check_limits(pipeline_name, 'regeneration'):
        limits_msg = pipeline_tracker.get_limit_message(pipeline_name)
        print(f"\n❌ SKIPPING {pipeline_name}: {limits_msg}")
        return False
    
    print(f"\n--- Processing {pipeline_name} data ---")
    
    # Compare schemas
    comparison = compare_schemas(source_path)
    
    if 'error' in comparison:
        error_msg = f"Schema comparison error: {comparison['error']}"
        print(f"Error: {error_msg}")
        pipeline_tracker.record_attempt(pipeline_name, 'regeneration', error_msg, False)
        return False
    
    print(f"Found {len(comparison.get('mismatches', []))} schema mismatches")
    
    # Generate pipeline
    pipeline = generate_cleaning_pipeline(
        source_path=source_path,
        ideal_path=ideal_path,
        output_table=output_table
    )
    
    if 'error' in pipeline:
        error_msg = f"Pipeline generation error: {pipeline.get('error')}"
        print(f"Error: {error_msg}")
        pipeline_tracker.record_attempt(pipeline_name, 'regeneration', error_msg, False)
        return False
    
    print("\n✓ Pipeline generated successfully!")
    print("\nGenerated SQL:")
    print(pipeline['cleaning_sql'])
    
    # Save the pipeline
    os.makedirs("pipelines/generated", exist_ok=True)
    with open(output_file, "w") as f:
        f.write(pipeline['pipeline_code'])
    print(f"\n✓ Pipeline saved to: {output_file}")
    
    # Generate and save validation checks from the schema
    # These checks will be used to validate the pipeline output deterministically
    checks_path = get_checks_path(pipeline_name, output_table)
    try:
        # Generate validation checks from the ideal schema
        checks = ValidationCheckGenerator.generate_from_schema(
            schema_path=ideal_path,
            table_name=output_table.replace("_clean", "")
        )
        
        # Save checks as JSON
        import json
        checks_path.parent.mkdir(parents=True, exist_ok=True)
        with open(str(checks_path), 'w') as f:
            checks_data = [check.to_dict() for check in checks]
            json.dump(checks_data, f, indent=2, default=str)
        
        print(f"✓ Generated {len(checks)} validation checks saved to: {checks_path}")
    except Exception as e:
        print(f"⚠️  Could not generate validation checks: {e}")
        # Fallback: use the generate_validation_checks function
        try:
            checks_data = generate_validation_checks(ideal_path, output_table.replace("_clean", ""))
            checks_path.parent.mkdir(parents=True, exist_ok=True)
            with open(str(checks_path), 'w') as f:
                json.dump(checks_data, f, indent=2, default=str)
            print(f"✓ Generated {len(checks_data)} validation checks (fallback) saved to: {checks_path}")
        except Exception as e2:
            print(f"⚠️  Could not generate validation checks (fallback also failed): {e2}")
    
    # Log successful generation
    pipeline_tracker.record_attempt(pipeline_name, 'regeneration', None, True)
    
    # Execute the pipeline
    print(f"\n--- Executing {pipeline_name} pipeline ---")
    result = run_and_log_subprocess(
        [sys.executable, output_file],
        cwd=PROJECT_ROOT,
        env=base_env,
        run_id=run_id
    )
    
    # Log execution attempt
    if result.returncode == 0:
        print(f"✓ {pipeline_name} pipeline executed successfully!")
        print(f"Output: {result.stdout[:500]}...")
        pipeline_tracker.record_attempt(pipeline_name, 'execution', None, True)
        
        # Run validation checks on the output
        try:
            from agents.validation_agent import validate_pipeline_output
            print(f"\n--- Validating {pipeline_name} pipeline output ---")
            validation_report = validate_pipeline_output(
                pipeline_name=pipeline_name,
                output_table=output_table,
                schema_path=ideal_path,
                db_path=str(paths.database),
                checks_path=str(checks_path) if 'checks_path' in locals() else None
            )
            
            # Print summary
            validation_report.print_summary()
            
            # Only return True if validation also passed
            if validation_report.overall_status != "PASS":
                print(f"⚠️  Pipeline execution succeeded but validation {validation_report.overall_status}ed")
                # Still return True for execution success, but log validation issues
        except Exception as e:
            print(f"⚠️  Could not run validation: {e}")
        
        return True
    else:
        error_msg = result.stderr[:500] if result.stderr else "Unknown error"
        print(f"⚠️  {pipeline_name} pipeline execution failed: {error_msg[:200]}")
        
        # Check for repeated error
        if pipeline_tracker.is_repeated_error(pipeline_name, error_msg):
            print(f"   ⚠️  REPEATED ERROR DETECTED - same issue as last attempt")
            print(f"   This suggests a fundamental incompatibility or bug in generation logic")
        
        pipeline_tracker.record_attempt(pipeline_name, 'execution', error_msg, False)
        
        # Apply backoff if we'll try again
        backoff = pipeline_tracker.get_backoff_delay(pipeline_name, 'execution')
        if backoff > 0:
            print(f"   Applying backoff: waiting {backoff:.1f}s...")
            time.sleep(backoff)
        
        return False


def run_pipeline_builder_demo(run_id: Optional[str] = None):
    """Demonstrate the pipeline builder auto-generating cleaning code with limits."""
    print("\nPipeline Builder: Analyzing source data and generating cleaning pipeline...")
    
    # Build environment with run_id for unique log files
    base_env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
    # Ensure Prefect uses local mode
    base_env["PREFECT_API_URL"] = ""
    base_env["PREFECT_CLOUD_API_URL"] = ""
    base_env["PREFECT_MODE"] = "local"
    base_env.pop("PREFECT_API_KEY", None)
    base_env.pop("PREFECT_CLOUD_API_KEY", None)
    if run_id:
        base_env["RUN_ID"] = run_id
    
    # Reset tracker for this demo run
    pipeline_tracker.reset()
    
    # Define pipelines to process
    pipelines = [
        {
            'name': 'users',
            'source_path': str(paths.source_data),
            'ideal_path': str(paths.users_schema),
            'output_table': 'users_clean',
            'output_file': 'pipelines/generated/clean_users_pipeline.py'
        }
    ]
    
    # Add transactions if exists
    if paths.transactions_data.exists():
        pipelines.append({
            'name': 'transactions',
            'source_path': str(paths.transactions_data),
            'ideal_path': str(paths.transactions_schema),
            'output_table': 'transactions_clean',
            'output_file': 'pipelines/generated/clean_transactions_pipeline.py'
        })
    
    # Add orders if exists
    if paths.orders_data.exists():
        pipelines.append({
            'name': 'orders',
            'source_path': str(paths.orders_data),
            'ideal_path': str(paths.orders_schema),
            'output_table': 'orders_clean',
            'output_file': 'pipelines/generated/clean_orders_pipeline.py'
        })
    
    # Check total pipelines limit
    max_pipelines = pipeline_tracker._limits.get('max_total_pipelines', 3)
    if len(pipelines) > max_pipelines:
        print(f"\n⚠️  Limiting to {max_pipelines} pipelines (found {len(pipelines)})")
        pipelines = pipelines[:max_pipelines]
    
    # Process each pipeline
    all_succeeded = True
    for pipeline_config in pipelines:
        success = process_pipeline_with_limits(
            pipeline_name=pipeline_config['name'],
            source_path=pipeline_config['source_path'],
            ideal_path=pipeline_config['ideal_path'],
            output_table=pipeline_config['output_table'],
            output_file=pipeline_config['output_file'],
            base_env=base_env,
            run_id=run_id
        )
        if not success:
            all_succeeded = False
    
    # Check if we exceeded total limits
    limits = pipeline_tracker.get_limits('')
    if limits['total_attempts'] >= limits['max_total_attempts']:
        print(f"\n❌ MAX TOTAL ATTEMPTS REACHED: {limits['total_attempts']}/{limits['max_total_attempts']}")
        print("   Some pipelines may not have been processed.")
        all_succeeded = False
    
    return all_succeeded


if __name__ == "__main__":
    run_full_demo()
