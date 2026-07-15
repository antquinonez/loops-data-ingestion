#!/usr/bin/env python3
"""
Demo script showing MCP tools being used to validate pipeline outputs.

This script:
1. Runs a cleaning pipeline
2. Uses MCP tools to validate the cleaned data
3. Reports on data quality

Usage:
    python demo_mcp_tools.py
"""

import sys
import os
import asyncio
import json
from pathlib import Path

# Setup project paths
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
os.environ["PYTHONPATH"] = str(PROJECT_ROOT)

from utils.paths import paths


def print_section(title):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


async def validate_with_mcp_tools():
    """Use MCP tools to validate pipeline outputs."""
    from flows.mcp_server import (
        query_database,
        get_data_quality_report,
        get_file_metadata
    )
    
    db_path = str(paths.database)
    
    # Check if database exists
    if not os.path.exists(db_path):
        print("Database not found. Please run the demo first to generate data.")
        print(f"Expected at: {db_path}")
        return False
    
    print_section("MCP Tools Pipeline Validation")
    
    # Step 1: Check what tables exist
    print("\n1. Checking database tables...")
    result = await query_database("SHOW TABLES")
    result_data = json.loads(result)
    
    if "error" in result_data:
        print(f"   Error querying database: {result_data['error']}")
        return False
    
    tables = [row["name"] for row in result_data["results"]]
    print(f"   Found {len(tables)} tables: {', '.join(tables)}")
    
    # Step 2: Validate clean tables
    print("\n2. Validating clean tables with MCP tools...")
    
    clean_tables = [t for t in tables if "clean" in t]
    if not clean_tables:
        print("   No clean tables found. Pipelines may not have run yet.")
        return False
    
    all_valid = True
    for table in clean_tables:
        print(f"\n   Validating table: {table}")
        
        # Get row count
        count_result = await query_database(f"SELECT COUNT(*) as count FROM {table}")
        count_data = json.loads(count_result)
        
        if "error" in count_data:
            print(f"     ❌ Error: {count_data['error']}")
            all_valid = False
            continue
        
        row_count = count_data["results"][0]["count"] if count_data["results"] else 0
        print(f"     Rows: {row_count}")
        
        # Get data quality report
        report_result = await get_data_quality_report(table)
        report_data = json.loads(report_result)
        
        if "error" in report_data:
            print(f"     ⚠️  Data quality report error: {report_data['error']}")
            continue
        
        columns = report_data.get("columns", [])
        issues = report_data.get("issues", [])
        
        print(f"     Columns: {len(columns)}")
        print(f"     Issues found: {len(issues)}")
        
        # Check for NULL issues (critical for cleaned data)
        null_issues = [i for i in issues if "null" in i.get("type", "").lower()]
        if null_issues:
            print(f"     ⚠️  NULL issues found: {len(null_issues)}")
            for issue in null_issues:
                print(f"        - {issue.get('column')}: {issue.get('error')}")
        else:
            print(f"     ✓ No NULL issues")
        
        # Check for type issues
        type_issues = [i for i in issues if "type" in i.get("type", "").lower() or "cast" in i.get("error", "").lower()]
        if type_issues:
            print(f"     ⚠️  Type issues found: {len(type_issues)}")
        else:
            print(f"     ✓ No type issues")
        
        if len(issues) == 0:
            print(f"     ✅ Table {table} is fully clean!")
    
    # Step 3: Check source file metadata
    print("\n3. Checking source file metadata with MCP tools...")
    
    source_file = str(paths.source_data)
    if os.path.exists(source_file):
        metadata_result = await get_file_metadata(source_file)
        metadata_data = json.loads(metadata_result)
        
        if "error" not in metadata_data:
            print(f"   File: {metadata_data['path']}")
            print(f"   Size: {metadata_data['size_human']}")
            print(f"   Modified: {metadata_data['modified']}")
            print(f"   ✓ File metadata retrieved successfully")
    
    print_section("Validation Summary")
    
    if all_valid:
        print("✅ All validations passed!")
        print("   MCP tools successfully validated pipeline outputs.")
    else:
        print("⚠️  Some validations had issues, but MCP tools worked correctly.")
    
    return all_valid


def run_pipeline_and_validate():
    """Run a cleaning pipeline and validate with MCP tools."""
    import subprocess
    
    print_section("Running Cleaning Pipeline")
    
    # Check if cleaning pipeline exists
    clean_pipeline = paths.clean_users_pipeline
    if not clean_pipeline.exists():
        print(f"Cleaning pipeline not found: {clean_pipeline}")
        print("Please run run_demo.py first to generate the pipeline.")
        return False
    
    print(f"\nRunning pipeline: {clean_pipeline}")
    
    # Run the pipeline
    result = subprocess.run(
        [sys.executable, str(clean_pipeline)],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT)
    )
    
    if result.returncode != 0:
        print(f"❌ Pipeline failed with return code {result.returncode}")
        print(f"STDERR: {result.stderr}")
        return False
    
    print("✅ Pipeline completed successfully")
    print(f"STDOUT: {result.stdout}")
    
    # Now validate with MCP tools
    return asyncio.run(validate_with_mcp_tools())


def main():
    """Main entry point."""
    print_section("MCP Tools Demo - Pipeline Validation")
    print("\nThis demo shows MCP tools being used to validate pipeline outputs.")
    
    # Run pipeline and validate
    success = run_pipeline_and_validate()
    
    print_section("Demo Complete")
    
    if success:
        print("✅ Demo completed successfully!")
        print("\nMCP tools are working correctly and can validate pipeline outputs.")
    else:
        print("⚠️  Demo completed with some issues.")
        print("\nNote: Run run_demo.py first to generate data and pipelines.")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
