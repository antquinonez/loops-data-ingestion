#!/usr/bin/env python3
"""
Test script to verify that nanobot can use the pipeline builder tools.
"""

import asyncio
import os
from pathlib import Path

# Load .env
from dotenv import load_dotenv
load_dotenv(Path("/home/aq/Documents/Source/loops") / ".env")

if not os.environ.get("OPENAI_API_KEY"):
    print("ERROR: No OPENAI_API_KEY found")
    exit(1)

from nanobot import Nanobot, RunResult
from nanobot.agent.tools.registry import ToolRegistry
from agents.pipeline_builder.nanobot_tools import PIPELINE_TOOL_CLASSES

async def test_nanobot_with_pipeline_tools():
    print("=" * 80)
    print("Testing Nanobot with Pipeline Builder Tools")
    print("=" * 80)
    
    # Create nanobot
    bot = Nanobot.from_config(
        config_path=str(Path("/home/aq/Documents/Source/loops") / "config" / "nanobot_config_minimal.json"),
        model=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini-2025-04-14")
    )
    
    # Register pipeline tools
    print("\nRegistering pipeline builder tools...")
    for tool_class in PIPELINE_TOOL_CLASSES:
        tool_instance = tool_class()
        bot._loop.tools.register(tool_instance)
        print(f"  ✓ {tool_instance.name}")
    
    # Check total tools
    all_tools = bot._loop.tools.get_definitions()
    print(f"\nTotal tools available: {len(all_tools)}")
    
    # Test with a simple message that should use the tools
    message = """Use infer_source_schema to analyze data/source_data.csv, then use generate_cleaning_pipeline to create a cleaning pipeline. Save the result to a file."""
    
    print(f"\nSending message to nanobot:")
    print(f"  {message}")
    print("\nWaiting for response...")
    
    try:
        result: RunResult = await bot.run(
            message,
            model=os.environ.get("OPENAI_MODEL", "gpt-4.1-mini-2025-04-14")
        )
        
        print("\n" + "=" * 80)
        print("NANOBOT RESPONSE")
        print("=" * 80)
        print(result.content if hasattr(result, 'content') else str(result))
        
        # Check if pipeline was created
        pipeline_file = Path("/home/aq/Documents/Source/loops") / "pipelines" / "generated" / "clean_users_pipeline.py"
        if pipeline_file.exists():
            print(f"\n✓ Pipeline file created: {pipeline_file}")
        else:
            print(f"\n✗ Pipeline file NOT created")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_nanobot_with_pipeline_tools())
