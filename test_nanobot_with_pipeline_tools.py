#!/usr/bin/env python3
"""
Test script to see if Nanobot can use Pipeline Builder tools.
This gives Nanobot access to load_ideal_schema, compare_schemas, 
and generate_cleaning_pipeline functions.
"""

import sys

from pathlib import Path

# Project root
PROJECT_ROOT = Path("/home/aq/Documents/Source/loops")
sys.path.insert(0, str(PROJECT_ROOT))

# Load environment
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

# Verify API key
if not os.environ.get("OPENAI_API_KEY"):
    print("❌ No OPENAI_API_KEY found in environment")
    print("Set it: export OPENAI_API_KEY='your-key'")
    sys.exit(1)

print("=" * 80)
print("TEST: Nanobot with Pipeline Builder Tools")
print("=" * 80)

# Import all the tools we want to give to Nanobot
from flows.nanobot_tools import (
    read_logs,
    query_duckdb,
    inspect_file,
    check_schema,
    send_slack_alert,
    get_ingestion_status
)
from agents.pipeline_builder.tools import (
    load_ideal_schema,
    infer_source_schema,
    compare_schemas,
    generate_cleaning_pipeline
)

# Create a combined set of tools for Nanobot
# Nanobot expects: name, description, function
ALL_TOOLS = {
    # Nanobot investigation tools
    "read_logs": {
        "description": "Read application logs to find error details",
        "function": read_logs
    },
    "query_duckdb": {
        "description": "Query the DuckDB database for investigation",
        "function": query_duckdb
    },
    "inspect_file": {
        "description": "Inspect source data files",
        "function": inspect_file
    },
    "check_schema": {
        "description": "Validate data against expected schema",
        "function": check_schema
    },
    "send_slack_alert": {
        "description": "Send investigation results to Slack",
        "function": send_slack_alert
    },
    "get_ingestion_status": {
        "description": "Get current status of the ingestion pipeline",
        "function": get_ingestion_status
    },
    # Pipeline Builder tools
    "load_ideal_schema": {
        "description": "Load the ideal schema definition from YAML",
        "function": load_ideal_schema
    },
    "infer_source_schema": {
        "description": "Infer schema from source CSV file",
        "function": infer_source_schema
    },
    "compare_schemas": {
        "description": "Compare source and ideal schemas, identify mismatches",
        "function": compare_schemas
    },
    "generate_cleaning_pipeline": {
        "description": "Generate complete cleaning pipeline (SQL + Python)",
        "function": generate_cleaning_pipeline
    }
}

# Prepare tools list for Nanobot
tools_list = []
for tool_name, tool_config in ALL_TOOLS.items():
    tools_list.append({
        "name": tool_name,
        "description": tool_config["description"],
        "function": tool_config["function"]
    })

print(f"\n✓ Prepared {len(tools_list)} tools for Nanobot:")
for t in tools_list:
    print(f"  - {t['name']}: {t['description'][:50]}...")

# Import Nanobot
from nanobot import Nanobot

# Create config
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

print(f"\n✓ Initializing Nanobot with model: {os.environ.get('OPENAI_MODEL', 'gpt-4.1-mini-2025-04-14')}")

# Try to create bot - nanobot might not accept tools parameter
try:
    bot = Nanobot(
        config=config,
        model=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini-2025-04-14")
    )
    print("✓ Nanobot created successfully")
except Exception as e:
    print(f"❌ Error creating Nanobot: {e}")
    # Try from_config
    try:
        bot = Nanobot.from_config(
            config_path=str(PROJECT_ROOT / "config" / "nanobot_config_minimal.json"),
            model=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini-2025-04-14")
        )
        print("✓ Nanobot created from config")
    except Exception as e2:
        print(f"❌ Error: {e2}")
        sys.exit(1)

# The issue is that Nanobot doesn't have a way to pass custom tools via constructor
# Let's check if we can register them with the loop
print("\n✓ Checking if we can register custom tools...")

# Get the loop from the bot
loop = bot._loop

# Try to register tools with the loop's tool registry
from nanobot.agent.tools.registry import Tool
from nanobot.agent.tools.loader import ToolLoader

# Check the loop's registry
if hasattr(loop, 'tools'):
    print(f"  Loop has tools registry: {loop.tools}")
    print(f"  Current tools: {list(loop.tools.get_definitions())[:5]}")

# Now run Nanobot with a message that asks it to use the pipeline builder tools
print("\n" + "=" * 80)
print("TRIGGERING NANOBOT INVESTIGATION WITH PIPELINE BUILDER TOOLS")
print("=" * 80)

message = """
Data ingestion job has failed. Please:
1. Investigate the failure by checking logs/ingestion.log
2. Examine the source data at data/source_data.csv
3. Load the ideal schema from schemas/ideal_schema.yaml
4. Use compare_schemas to identify mismatches
5. Use generate_cleaning_pipeline to create a cleaning pipeline
6. Save the generated pipeline to pipelines/generated/

Please use the available tools to complete this entire workflow.
"""

print(f"\nMessage to Nanobot:")
print(message)

import asyncio

async def run_nanobot():
    try:
        result = await bot.run(
            message,
            model=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini-2025-04-14")
        )
        
        print("\n" + "=" * 80)
        print("NANOBOT RESULTS")
        print("=" * 80)
        print(result.content)
        
        # Check if any pipelines were generated
        
        gen_files = []
        for f in os.listdir("pipelines/generated"):
            if f.endswith(".py"):
                gen_files.append(f)
        
        if gen_files:
            print(f"\n✓ Nanobot generated files: {gen_files}")
        else:
            print("\n⚠️  Nanobot did not generate any pipeline files")
        
        return result
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

asyncio.run(run_nanobot())
