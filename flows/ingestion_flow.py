"""
Prefect flow for data ingestion with intentional errors for troubleshooting demo.
This flow will fail due to data quality issues in the source CSV.
"""

import os
import sys

# Configure Prefect to use local mode ONLY (no cloud connection)
# This must be set BEFORE importing prefect
# For Prefect 3.x, use ephemeral local server
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
os.environ["PREFECT_CLOUD_API_URL"] = ""
os.environ["PREFECT_MODE"] = "local"
os.environ["PREFECT_EPHEMERAL_START"] = "true"
os.environ.pop("PREFECT_API_KEY", None)
os.environ.pop("PREFECT_CLOUD_API_KEY", None)

from prefect import flow, task, get_run_logger
import duckdb
import csv
import json
from datetime import datetime
from pathlib import Path

# Configure logging
import logging

# Import path configuration
from utils.paths import paths
from utils.logging_config import setup_logging, get_logger

# Get run ID from environment for unique log files
RUN_ID = os.environ.get('RUN_ID', datetime.now().strftime('%Y%m%d_%H%M%S'))

# Setup unified logging that captures both custom and Prefect logs
logging_config = setup_logging(run_id=RUN_ID, log_name="ingestion")
logger = get_logger("ingestion")

# Ensure Prefect uses our logging configuration
# This makes get_run_logger() use the same handlers
prefect_logger = get_logger("prefect")

# Also configure root logger for Prefect
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True
)

DATA_DIR = paths.data_dir
DB_PATH = str(paths.database)
SOURCE_FILE = str(paths.source_data)


@task(name="validate_source_file")
def validate_source_file(file_path: str) -> dict:
    """Validate that source file exists and is readable."""
    logger.info(f"Validating source file: {file_path}")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found: {file_path}")
    
    if not os.path.isfile(file_path):
        raise ValueError(f"Path is not a file: {file_path}")
    
    # Check file size
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        raise ValueError(f"Source file is empty: {file_path}")
    
    logger.info(f"Source file validated: {file_path} ({file_size} bytes)")
    
    return {
        "file_path": file_path,
        "file_size": file_size,
        "status": "valid"
    }


@task(name="create_target_table")
def create_target_table() -> dict:
    """Create the target table with strict schema (will cause errors with bad data)."""
    logger.info(f"Creating target table in DuckDB at {DB_PATH}")
    
    # This schema has NOT NULL constraints and type requirements
    # that will conflict with our bad data
    create_sql = """
    CREATE OR REPLACE TABLE users (
        id INTEGER NOT NULL,
        name VARCHAR NOT NULL,
        email VARCHAR NOT NULL,
        age INTEGER NOT NULL,
        join_date DATE NOT NULL,
        status VARCHAR NOT NULL,
        score FLOAT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
    )
    """
    
    conn = duckdb.connect(database=DB_PATH, read_only=False)
    try:
        conn.execute(create_sql)
        logger.info("Target table created successfully")
        
        # Also create a raw table for staging
        conn.execute("""
            CREATE OR REPLACE TABLE raw_users (
                id VARCHAR,
                name VARCHAR,
                email VARCHAR,
                age VARCHAR,
                join_date VARCHAR,
                status VARCHAR,
                score VARCHAR
            )
        """)
        logger.info("Raw staging table created")
        
        return {"status": "created", "tables": ["users", "raw_users"]}
    finally:
        conn.close()


@task(name="load_to_raw")
def load_to_raw(file_path: str) -> dict:
    """Load CSV into raw staging table (this should succeed)."""
    logger.info(f"Loading {file_path} into raw_users table")
    
    conn = duckdb.connect(database=DB_PATH, read_only=False)
    try:
        # Load into raw table - should work even with bad data
        conn.execute(f"""
            COPY raw_users FROM '{file_path}' 
            (AUTO_DETECT TRUE, HEADER TRUE, DELIMITER ',')
        """)
        
        # Get row count
        result = conn.execute("SELECT COUNT(*) FROM raw_users").fetchone()
        row_count = result[0]
        
        logger.info(f"Loaded {row_count} rows into raw_users")
        
        return {
            "rows_loaded": row_count,
            "status": "success"
        }
    finally:
        conn.close()


@task(name="transform_and_load")
def transform_and_load() -> dict:
    """
    Transform data from raw_users to users table.
    This will INTENTIONALLY FAIL due to data quality issues.
    """
    logger.info("Starting transformation from raw_users to users")
    
    conn = duckdb.connect(database=DB_PATH, read_only=False)
    try:
        # First, check what we're working with
        logger.info("Checking raw data quality...")
        
        # Count NULL emails
        null_emails = conn.execute("SELECT COUNT(*) FROM raw_users WHERE email IS NULL OR email = ''").fetchone()[0]
        logger.warning(f"Found {null_emails} rows with NULL/empty email")
        
        # Count invalid ages
        invalid_ages = conn.execute("SELECT COUNT(*) FROM raw_users WHERE age NOT LIKE '%[0-9]%'").fetchone()[0]
        logger.warning(f"Found {invalid_ages} rows with non-numeric age")
        
        # Now try to insert - THIS WILL FAIL
        logger.info("Attempting to insert into users table (this will fail)...")
        
        insert_sql = """
        INSERT INTO users (id, name, email, age, join_date, status, score)
        SELECT 
            CAST(id AS INTEGER),
            name,
            email,
            CAST(age AS INTEGER),  -- This will fail on 'N/A'
            CAST(join_date AS DATE),
            status,
            CAST(score AS FLOAT)
        FROM raw_users
        """
        
        # This will throw an exception due to:
        # 1. NULL email values (NOT NULL constraint)
        # 2. Non-numeric age ('N/A' can't be cast to INTEGER)
        # 3. Potentially malformed email
        conn.execute(insert_sql)
        
        rows_inserted = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        logger.info(f"Successfully inserted {rows_inserted} rows")
        
        return {
            "rows_inserted": rows_inserted,
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Transformation failed: {type(e).__name__}: {e}")
        
        # Log detailed error context
        logger.error("--- ERROR CONTEXT ---")
        logger.error(f"Database: {DB_PATH}")
        logger.error(f"Source: {SOURCE_FILE}")
        logger.error(f"Timestamp: {datetime.now().isoformat()}")
        
        # Get sample of problematic data
        try:
            bad_rows = conn.execute("""
                SELECT * FROM raw_users 
                WHERE email IS NULL OR email = '' OR age NOT LIKE '%[0-9]%'
                LIMIT 5
            """).fetchall()
            logger.error(f"Sample bad rows: {bad_rows}")
        except Exception as e2:
            logger.error(f"Could not fetch bad rows: {e2}")
        
        raise  # Re-raise to fail the task
        
    finally:
        conn.close()


def run_pipeline(file_path: str = SOURCE_FILE) -> dict:
    """
    Run the ingestion pipeline manually (without Prefect orchestration).
    This will fail due to data quality issues.
    """
    logger.info("=" * 80)
    logger.info("Starting data ingestion pipeline")
    logger.info(f"Source file: {file_path}")
    logger.info(f"Database: {DB_PATH}")
    
    result = {"status": "failed", "error": None}
    
    try:
        # Step 1: Validate source
        logger.info("\n--- Step 1: Validating source file ---")
        validation = validate_source_file(file_path)
        logger.info(f"Validation result: {validation}")
        result["validation"] = validation
        
        # Step 2: Create target tables
        logger.info("\n--- Step 2: Creating target tables ---")
        tables = create_target_table()
        logger.info(f"Tables created: {tables}")
        result["tables"] = tables
        
        # Step 3: Load to raw staging
        logger.info("\n--- Step 3: Loading to raw staging table ---")
        raw_load = load_to_raw(file_path)
        logger.info(f"Raw load result: {raw_load}")
        result["raw_load"] = raw_load
        
        # Step 4: Transform and load - THIS WILL FAIL
        logger.info("\n--- Step 4: Transforming and loading to users table ---")
        logger.warning("This step is expected to FAIL due to data quality issues!")
        transform_result = transform_and_load()
        logger.info(f"Transform result: {transform_result}")
        result["transform"] = transform_result
        
        result["status"] = "success"
        logger.info("\nData ingestion pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"\nPipeline failed with error: {type(e).__name__}: {e}")
        result["error"] = str(e)
        result["error_type"] = type(e).__name__
        raise
    
    return result


@flow(name="data_ingestion_pipeline", log_prints=True)
def data_ingestion_pipeline(file_path: str = SOURCE_FILE):
    """
    Prefect flow wrapper for the ingestion pipeline.
    """
    logger = get_run_logger()
    return run_pipeline(file_path)


if __name__ == "__main__":
    # Run the pipeline directly (without Prefect orchestration)
    try:
        result = run_pipeline()
        print(f"\nPipeline result: {result}")
    except Exception as e:
        print(f"\nPipeline failed (as expected): {type(e).__name__}: {e}")
        import sys
        sys.exit(1)
