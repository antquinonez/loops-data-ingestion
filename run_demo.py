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
from pathlib import Path
from typing import Optional

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
    sys.path.insert(0, str(PROJECT_ROOT))
    os.environ["PYTHONPATH"] = str(PROJECT_ROOT)

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
    os.environ.pop("PREFECT_API_KEY", None)
    
    print("\n" + "=" * 80)
    print("INITIALIZING: Cleaning up previous run artifacts")
    print("=" * 80)
    print(f"Run ID: {run_id}")
    print("Prefect configured for LOCAL MODE (no cloud connection)")
    
    try:
        from utils.cleanup import cleanup_all, clean_database, clean_generated_pipelines, clean_logs, archive_logs
    except ImportError:
        # Fallback: do manual cleanup
        print("\nCleanup module not available, performing manual cleanup...")
        result = {
            "logs_cleaned": [],
            "logs_archived": [],
            "db_cleaned": [],
            "pipelines_cleaned": [],
            "errors": []
        }
        
        # Clean database files
        for db_file in [paths.database, paths.nanobot_db]:
            if db_file.exists():
                try:
                    db_file.unlink()
                    result["db_cleaned"].append(str(db_file.name))
                except Exception as e:
                    result["errors"].append(f"Failed to delete {db_file.name}: {e}")
        
        # Clean generated pipelines
        generated_dir = paths.pipelines_dir / "generated"
        if generated_dir.exists():
            for pipe_file in generated_dir.glob("*.py"):
                try:
                    pipe_file.unlink()
                    result["pipelines_cleaned"].append(str(pipe_file.name))
                except Exception as e:
                    result["errors"].append(f"Failed to delete {pipe_file.name}: {e}")
        
        # Clean logs
        if archive_logs:
            logs_dir = paths.logs_dir
            if logs_dir.exists():
                import shutil
                from datetime import datetime
                archive_dir = logs_dir / "archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                timestamped_dir = archive_dir / f"run_{timestamp}"
                timestamped_dir.mkdir(exist_ok=True)
                
                for log_file in logs_dir.glob("*.log"):
                    try:
                        dest = timestamped_dir / log_file.name
                        shutil.copy2(log_file, dest)
                        log_file.unlink()
                        result["logs_archived"].append(str(log_file.name))
                    except Exception as e:
                        result["errors"].append(f"Failed to archive {log_file.name}: {e}")
                
                for log_file in logs_dir.glob("*.md"):
                    try:
                        dest = timestamped_dir / log_file.name
                        shutil.copy2(log_file, dest)
                        log_file.unlink()
                        result["logs_archived"].append(str(log_file.name))
                    except Exception as e:
                        result["errors"].append(f"Failed to archive {log_file.name}: {e}")
        else:
            logs_dir = paths.logs_dir
            if logs_dir.exists():
                for log_file in logs_dir.glob("*.log"):
                    try:
                        log_file.unlink()
                        result["logs_cleaned"].append(str(log_file.name))
                    except Exception as e:
                        result["errors"].append(f"Failed to delete {log_file.name}: {e}")
                
                for log_file in logs_dir.glob("*.md"):
                    try:
                        log_file.unlink()
                        result["logs_cleaned"].append(str(log_file.name))
                    except Exception as e:
                        result["errors"].append(f"Failed to delete {log_file.name}: {e}")
        
        return result
    
    # Use the cleanup module
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
        print("\n⚠️  No generated pipeline found - using original (will fail due to data errors)")
        flow_path = paths.get_abs("project_root") / "flows" / "ingestion_flow.py"
        used_generated = False
    
    # Build environment with run_id for unique log files
    run_env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
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
    """Start the MCP server in the background (currently disabled - MCP API compatibility issue)."""
    print("\n⚠️  MCP server disabled - API compatibility issue with mcp library")
    print("   Nanobot will use built-in tools (read_file, exec, etc.) instead")
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
    
    # Register nanobot_tools (simple function-based tools)
    print("  Loading NANOBOT_TOOLS...")
    for tool_name, tool_config in NANOBOT_TOOLS.items():
        # For now, skip these - they need to be converted to Tool classes
        # We'll focus on the pipeline tools
        print(f"    - {tool_name}: {tool_config['description'][:50]}...")
    
    # Register pipeline builder Tool classes
    print("  Loading PIPELINE_TOOL_CLASSES...")
    for tool_class in PIPELINE_TOOL_CLASSES:
        tool_instance = tool_class()
        registry.register(tool_instance)
        print(f"    ✓ Registered: {tool_instance.name} - {tool_instance.description[:50]}...")
    
    print(f"  Total custom tools registered: {len(PIPELINE_TOOL_CLASSES)}")
    
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
    message = """Investigate the failed data ingestion. Use: infer_source_schema, load_ideal_schema, compare_schemas, generate_cleaning_pipeline. Save pipeline to pipelines/generated/clean_users_pipeline.py using write_file."""
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
    
    # Step 5: Now trigger nanobot
    print("\n" + "=" * 80)
    print("TRIGGERING NANOBOT INVESTIGATION")
    print("=" * 80)
    
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
        
        import asyncio
        
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
        
        try:
            with open(nanobot_log_path, 'w') as nanobot_log_file:
                # Tee stdout to both console and file
                import sys
                original_stdout = sys.stdout
                original_stderr = sys.stderr
                sys.stdout = Tee(original_stdout, nanobot_log_file)
                sys.stderr = Tee(original_stderr, nanobot_log_file)
                
                try:
                    asyncio.run(trigger_nanobot_investigation(run_id=run_id))
                finally:
                    # Restore original stdout/stderr
                    sys.stdout = original_stdout
                    sys.stderr = original_stderr
        except Exception as e:
            print(f"Error capturing Nanobot output: {e}")
            # Fallback: run without capture
            asyncio.run(trigger_nanobot_investigation(run_id=run_id))
        
        # After nanobot runs, check if it created a pipeline
        if generated_pipeline.exists():
            print("\n✓ Nanobot successfully created a cleaning pipeline!")
            print(f"  Pipeline saved to: {generated_pipeline}")
        else:
            print("\n⚠️  Nanobot did not create a pipeline file.")
            print("  Running pipeline builder demo as fallback...")
            run_pipeline_builder_demo(run_id=run_id)
    else:
        print("\n⚠️  No OpenAI API key detected.")
        print("To run a live investigation with Nanobot:")
        print("  1. Create .env file with OPENAI_API_KEY")
        print("  2. Or set it: export OPENAI_API_KEY='your-key'")
        print("\nFor now, running pipeline builder demo...")
        run_pipeline_builder_demo(run_id=run_id)


def run_pipeline_builder_demo(run_id: Optional[str] = None):
    """Demonstrate the pipeline builder auto-generating cleaning code."""
    print("\nPipeline Builder: Analyzing source data and generating cleaning pipeline...")
    
    # Build environment with run_id for unique log files
    base_env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
    if run_id:
        base_env["RUN_ID"] = run_id
    
    from agents.pipeline_builder.tools import (
        load_ideal_schema,
        infer_source_schema,
        compare_schemas,
        generate_cleaning_pipeline
    )
    
    # Test with users data
    print("\n--- Processing users data ---")
    comparison = compare_schemas("data/source_data.csv")
    
    if 'error' in comparison:
        print(f"Error: {comparison['error']}")
    else:
        print(f"Found {len(comparison.get('mismatches', []))} schema mismatches")
        
        pipeline = generate_cleaning_pipeline(
            source_path="data/source_data.csv",
            ideal_path="schemas/users_schema.yaml",
            output_table="users_clean"
        )
        
        if 'error' not in pipeline:
            print("\n✓ Pipeline generated successfully!")
            print("\nGenerated SQL:")
            print(pipeline['cleaning_sql'])
            
            # Save the pipeline
            os.makedirs("pipelines/generated", exist_ok=True)
            with open("pipelines/generated/clean_users_pipeline.py", "w") as f:
                f.write(pipeline['pipeline_code'])
            print(f"\n✓ Pipeline saved to: pipelines/generated/clean_users_pipeline.py")
            
            # Execute the pipeline
            print("\n--- Executing generated pipeline ---")
            result = run_and_log_subprocess(
                [sys.executable, "pipelines/generated/clean_users_pipeline.py"],
                cwd=PROJECT_ROOT,
                env=base_env,
                run_id=run_id
            )
            
            if result.returncode == 0:
                print("✓ Pipeline executed successfully!")
                print(f"Output: {result.stdout[:500]}...")
            else:
                print(f"⚠️  Pipeline execution issue: {result.stderr[:200]}")
        else:
            print(f"Error generating pipeline: {pipeline.get('error')}")
    
    # Check if we have additional files
    transactions_csv = paths.transactions_data
    if transactions_csv.exists():
        print("\n--- Processing transactions data ---")
        pipeline = generate_cleaning_pipeline(
            source_path=str(transactions_csv),
            ideal_path=str(paths.transactions_schema),
            output_table="transactions_clean"
        )
        
        if 'error' not in pipeline:
            print("✓ Transactions pipeline generated successfully!")
            
            # Save the pipeline
            os.makedirs("pipelines/generated", exist_ok=True)
            with open("pipelines/generated/clean_transactions_pipeline.py", "w") as f:
                f.write(pipeline['pipeline_code'])
            print(f"✓ Pipeline saved to: pipelines/generated/clean_transactions_pipeline.py")
            
            # Execute the pipeline
            print("--- Executing transactions pipeline ---")
            result = run_and_log_subprocess(
                [sys.executable, "pipelines/generated/clean_transactions_pipeline.py"],
                cwd=PROJECT_ROOT,
                env=base_env,
                run_id=run_id
            )
            
            if result.returncode == 0:
                print("✓ Transactions pipeline executed successfully!")
                print(f"Output: {result.stdout[:200]}...")
            else:
                print(f"⚠️  Pipeline execution issue: {result.stderr[:200]}")
        else:
            print(f"Error generating transactions pipeline: {pipeline.get('error')}")
    
    # Check if we have orders data
    orders_csv = paths.orders_data
    if orders_csv.exists():
        print("\n--- Processing orders data ---")
        pipeline = generate_cleaning_pipeline(
            source_path=str(orders_csv),
            ideal_path=str(paths.orders_schema),
            output_table="orders_clean"
        )
        
        if 'error' not in pipeline:
            print("✓ Orders pipeline generated successfully!")
            
            # Save the pipeline
            os.makedirs("pipelines/generated", exist_ok=True)
            with open("pipelines/generated/clean_orders_pipeline.py", "w") as f:
                f.write(pipeline['pipeline_code'])
            print(f"✓ Pipeline saved to: pipelines/generated/clean_orders_pipeline.py")
            
            # Execute the pipeline
            print("--- Executing orders pipeline ---")
            result = run_and_log_subprocess(
                [sys.executable, "pipelines/generated/clean_orders_pipeline.py"],
                cwd=PROJECT_ROOT,
                env=base_env,
                run_id=run_id
            )
            
            if result.returncode == 0:
                print("✓ Orders pipeline executed successfully!")
                print(f"Output: {result.stdout[:200]}...")
            else:
                print(f"⚠️  Pipeline execution issue: {result.stderr[:200]}")
        else:
            print(f"Error generating orders pipeline: {pipeline.get('error')}")


if __name__ == "__main__":
    run_full_demo()
