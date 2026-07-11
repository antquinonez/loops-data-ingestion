#!/usr/bin/env python3
"""
Demo script for the Pipeline Builder Agent.
This demonstrates how the agent can automatically generate a data cleaning pipeline
based on comparing source data with an ideal schema.
"""

import sys
import os
from pathlib import Path

# Project root
PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
sys.path.insert(0, str(PROJECT_ROOT))

# Load environment
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")


def test_tools():
    """Test the pipeline builder tools directly."""
    print("=" * 80)
    print("TESTING PIPELINE BUILDER TOOLS")
    print("=" * 80)
    
    from agents.pipeline_builder.tools import (
        load_ideal_schema,
        infer_source_schema,
        compare_schemas,
        generate_cleaning_pipeline
    )
    import json
    
    # Define file-schema mappings
    file_schema_mappings = [
        ("data/source_data.csv", "schemas/users_schema.yaml", "users_clean", "raw_users"),
        ("data/orders.csv", "schemas/orders_schema.yaml", "orders_clean", "raw_orders"),
        ("data/transactions.csv", "schemas/transactions_schema.yaml", "transactions_clean", "raw_transactions"),
    ]
    
    all_pipelines = {}
    
    for i, (source_path, schema_path, output_table, source_table) in enumerate(file_schema_mappings, 1):
        if not os.path.exists(source_path):
            print(f"\n{i}. Skipping {source_path} (file not found)")
            continue
        if not os.path.exists(schema_path):
            print(f"\n{i}. Skipping {source_path} (schema not found)")
            continue
        
        print(f"\n{i}. Processing {os.path.basename(source_path)} with {os.path.basename(schema_path)}...")
        
        # Compare schemas
        comparison = compare_schemas(source_path, ideal_path=schema_path)
        if 'error' in comparison:
            print(f"   Error: {comparison['error']}")
            continue
        
        print(f"   ✓ Schema comparison complete")
        print(f"   Matches: {len(comparison.get('matches', []))}")
        print(f"   Mismatches: {len(comparison.get('mismatches', []))}")
        for mismatch in comparison.get('mismatches', []):
            print(f"     - {mismatch['column']}: {len(mismatch['issues'])} issues")
        
        # Generate pipeline
        pipeline = generate_cleaning_pipeline(
            source_path=source_path,
            ideal_path=schema_path,
            output_table=output_table,
            source_table=source_table
        )
        
        if 'error' in pipeline:
            print(f"   Error: {pipeline['error']}")
            continue
        
        print(f"   ✓ Pipeline generated successfully")
        print(f"\n   SQL Cleaning Statement:")
        print("   " + "-" * 76)
        for line in pipeline['cleaning_sql'].split('\n'):
            print(f"   {line}")
        
        # Save generated pipeline
        (PROJECT_ROOT / "pipelines" / "generated").mkdir(parents=True, exist_ok=True)
        pipeline_filename = f"clean_{output_table}.py"
        pipeline_path = PROJECT_ROOT / "pipelines" / "generated" / pipeline_filename
        with open(pipeline_path, "w") as f:
            f.write(pipeline['pipeline_code'])
        print(f"\n   ✓ Python pipeline saved to: {pipeline_path}")
        
        all_pipelines[output_table] = pipeline
    
    return all_pipelines


def run_pipeline_builder_agent():
    """Run the pipeline builder agent with Nanobot."""
    print("\n" + "=" * 80)
    print("RUNNING PIPELINE BUILDER AGENT")
    print("=" * 80)
    
    from nanobot import Nanobot, RunResult
    import asyncio
    from agents.pipeline_builder.tools import PIPELINE_TOOLS
    
    # Verify API key
    if not os.environ.get("OPENAI_API_KEY"):
        print("\n⚠️  No OPENAI_API_KEY found in environment")
        print("Set it: export OPENAI_API_KEY='your-key'")
        return
    
    # Create the bot with minimal config
    print("\nInitializing Pipeline Builder Nanobot...")
    
    # Prepare tools for nanobot
    tools_list = []
    for tool_name, tool_config in PIPELINE_TOOLS.items():
        tools_list.append({
            "name": tool_name,
            "description": tool_config["description"],
            "function": tool_config["function"]
        })
    
    # Create minimal config
    config = {
        "agents": {
            "defaults": {
                "workspace": str(PROJECT_ROOT)
            }
        },
        "providers": {
            "openai": {
                "api_key": "${OPENAI_API_KEY}"
            }
        }
    }
    
    bot = Nanobot(
        config=config,
        tools=tools_list,
        model=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini-2025-04-14")
    )
    
    # Trigger pipeline generation
    message = """
    Create a data cleaning pipeline for data/source_data.csv.
    
    Requirements:
    1. Load the ideal schema from schemas/users_schema.yaml
    2. Compare it with the source data
    3. Identify all data quality issues
    4. Generate SQL transformations to fix each issue
    5. Generate a Python/Prefect flow
    6. Include validation queries
    7. Use the default values from the ideal schema
    
    Source file: data/source_data.csv
    Target table: users
    Output table: users_clean
    """
    
    print(f"\nTriggering agent with message:")
    print(message)
    print("\nThis may take a moment... (agent is analyzing and generating)")
    
    async def run():
        try:
            result: RunResult = await bot.run(
                message,
                model=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini-2025-04-14")
            )
            
            print("\n" + "=" * 80)
            print("PIPELINE BUILDER RESULTS")
            print("=" * 80)
            print(result.content)
            
            # Save results
            with open("pipelines/generated/pipeline_builder_output.md", "w") as f:
                f.write("# Pipeline Builder Output\n\n")
                f.write(result.content)
            print("\n✓ Results saved to: pipelines/generated/pipeline_builder_output.md")
            
            return result.content
            
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    return asyncio.run(run())


def run_full_demo():
    """Run the complete pipeline builder demo."""
    print("\n" + "=" * 80)
    print("PIPELINE BUILDER DEMO")
    print("=" * 80)
    
    # Step 1: Test tools directly
    all_pipelines = test_tools()
    
    # Step 2: Run agent (if API key available)
    if os.environ.get("OPENAI_API_KEY"):
        try:
            run_pipeline_builder_agent()
        except Exception as e:
            print(f"\n⚠️  Agent demo failed (but tools work!): {e}")
            print("The pipeline builder tools work correctly as shown above.")
    else:
        print("\n" + "=" * 80)
        print("TO RUN THE AGENT")
        print("=" * 80)
        print("\nSet your OpenAI API key and run:")
        print("  export OPENAI_API_KEY='your-key'")
        print("  python demo_pipeline_builder.py")
        print("\nFor now, the tool test above shows what the agent can do.")
    
    # Step 3: Execute the generated pipelines
    print("\n" + "=" * 80)
    print("EXECUTING GENERATED PIPELINES")
    print("=" * 80)
    
    import subprocess
    import duckdb
    
    # Execute each generated pipeline
    for output_table, pipeline in all_pipelines.items():
        pipeline_path = PROJECT_ROOT / "pipelines" / "generated" / f"clean_{output_table}.py"
        
        if not pipeline_path.exists():
            print(f"\n⚠️  Pipeline not found: {pipeline_path}")
            continue
        
        print(f"\n--- Running {pipeline_path} ---")
        result = subprocess.run(
            [sys.executable, str(pipeline_path)],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
        )
        
        if result.returncode == 0:
            print("✓ Pipeline executed successfully!")
            
            # Query the cleaned data
            try:
                conn = duckdb.connect(database="data/ingestion.db")
                count = conn.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
                print(f"✓ {count} rows saved to {output_table} table")
                
                # Show sample
                sample = conn.execute(f"SELECT * FROM {output_table} LIMIT 3").fetchall()
                print("\nSample cleaned data:")
                for row in sample:
                    print(f"  {row}")
                conn.close()
            except Exception as e:
                print(f"  (Could not query database: {e})")
        else:
            print(f"⚠️  Pipeline failed with return code: {result.returncode}")
            if result.stderr:
                print(f"  Error: {result.stderr[:200]}")


if __name__ == "__main__":
    run_full_demo()
