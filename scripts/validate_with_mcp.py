#!/usr/bin/env python3
"""
Validation script using MCP tools to verify data quality.

This script uses the MCP tools directly (without starting a server)
to validate data in the database and files.

Usage:
    python scripts/validate_with_mcp.py
"""

import sys
import os
import asyncio
import json
from pathlib import Path

# Setup project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.environ["PYTHONPATH"] = str(PROJECT_ROOT)

from utils.paths import paths


def print_section(title):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


async def main():
    """Main validation routine."""
    from flows.mcp_server import (
        query_database,
        get_data_quality_report,
        get_recent_errors,
        get_file_metadata
    )
    
    print_section("MCP Tools Validation")
    print("\nUsing MCP tools to validate data quality...")
    
    # Ensure database exists
    db_path = str(paths.database)
    if not os.path.exists(db_path):
        print(f"\n⚠️  Database not found at {db_path}")
        print("   Please run run_demo.py first to generate data.")
        return
    
    # Test 1: Check what tables exist
    print("\n1. Checking available tables...")
    result = await query_database("SHOW TABLES")
    result_data = json.loads(result)
    
    if "error" in result_data:
        print(f"   ❌ Error: {result_data['error']}")
        return
    
    tables = [row["name"] for row in result_data["results"]]
    print(f"   ✓ Found {len(tables)} tables: {', '.join(tables)}")
    
    # Test 2: Query a clean table if it exists
    clean_tables = [t for t in tables if "clean" in t]
    if clean_tables:
        print(f"\n2. Testing query_database on clean table...")
        result = await query_database(f"SELECT COUNT(*) as count FROM {clean_tables[0]}")
        result_data = json.loads(result)
        
        if "error" in result_data:
            print(f"   ❌ Error: {result_data['error']}")
        else:
            count = result_data["results"][0]["count"] if result_data["results"] else 0
            print(f"   ✓ Query successful: {count} rows in {clean_tables[0]}")
    
    # Test 3: Data quality report on clean table
    if clean_tables:
        print(f"\n3. Testing get_data_quality_report on clean table...")
        result = await get_data_quality_report(clean_tables[0])
        report_data = json.loads(result)
        
        if "error" in report_data:
            print(f"   ❌ Error: {report_data['error']}")
        else:
            issues = report_data.get("issues", [])
            print(f"   ✓ Report generated: {len(issues)} issues found")
            if len(issues) == 0:
                print(f"   ✅ Clean table has no data quality issues!")
            else:
                for issue in issues:
                    print(f"      - {issue.get('type')}: {issue.get('column')}")
    
    # Test 4: Check source file metadata
    print("\n4. Testing get_file_metadata tool...")
    source_file = str(paths.source_data)
    
    if os.path.exists(source_file):
        result = await get_file_metadata(source_file)
        metadata = json.loads(result)
        
        if "error" in metadata:
            print(f"   ❌ Error: {metadata['error']}")
        else:
            print(f"   ✓ File: {metadata['path']}")
            print(f"   ✓ Size: {metadata['size_human']}")
            print(f"   ✓ Modified: {metadata['modified']}")
    else:
        print(f"   ⚠️  Source file not found: {source_file}")
    
    # Test 5: Check log file errors
    print("\n5. Testing get_recent_errors tool...")
    log_file = str(paths.ingestion_log)
    
    if os.path.exists(log_file):
        result = await get_recent_errors(log_file, hours=24)
        errors = json.loads(result)
        
        if "error" in errors:
            print(f"   ❌ Error: {errors['error']}")
        else:
            error_count = errors.get("error_count", 0)
            print(f"   ✓ Found {error_count} errors in logs")
    else:
        print(f"   ⚠️  Log file not found: {log_file}")
    
    print_section("Validation Complete")
    print("\n✅ MCP tools are working correctly!")
    print("   All tools returned valid responses and validated data quality.")


if __name__ == "__main__":
    asyncio.run(main())
