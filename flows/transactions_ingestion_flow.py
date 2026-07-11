"""
Prefect flow for transactions data ingestion with intentional errors for troubleshooting demo.
This flow will fail due to data quality issues in the source CSV.
"""

from prefect import flow, task, get_run_logger
import duckdb
import os
from pathlib import Path
import logging

# Import path configuration
try:
    from utils.paths import paths
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.paths import paths

# Configure logging
transactions_logger = logging.getLogger('transactions_ingestion')
transactions_logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(str(paths.transactions_ingestion_log))
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
transactions_logger.addHandler(file_handler)

logger = transactions_logger

DATA_DIR = paths.data_dir
DB_PATH = str(paths.database)
SOURCE_FILE = str(paths.transactions_data)


@task(name="validate_transactions_source_file")
def validate_source_file(file_path: str) -> dict:
    """Validate that source file exists and is readable."""
    logger.info(f"Validating source file: {file_path}")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found: {file_path}")
    
    if not os.path.isfile(file_path):
        raise ValueError(f"Path is not a file: {file_path}")
    
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        raise ValueError(f"Source file is empty: {file_path}")
    
    logger.info(f"Source file validated: {file_path} ({file_size} bytes)")
    
    return {
        "file_path": file_path,
        "file_size": file_size,
        "status": "valid"
    }


@task(name="create_transactions_tables")
def create_target_tables() -> dict:
    """Create the transactions tables with strict schema (will cause errors with bad data)."""
    logger.info(f"Creating transactions tables in DuckDB at {DB_PATH}")
    
    # Target table with NOT NULL constraints and type requirements
    create_transactions_sql = """
    CREATE OR REPLACE TABLE transactions (
        transaction_id INTEGER NOT NULL,
        customer_id INTEGER NOT NULL,
        amount FLOAT NOT NULL,
        date DATE NOT NULL,
        category VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (transaction_id)
    )
    """
    
    # Raw staging table
    create_raw_sql = """
    CREATE OR REPLACE TABLE raw_transactions (
        transaction_id VARCHAR,
        customer_id VARCHAR,
        amount VARCHAR,
        date VARCHAR,
        category VARCHAR,
        status VARCHAR
    )
    """
    
    conn = duckdb.connect(database=DB_PATH, read_only=False)
    try:
        conn.execute(create_transactions_sql)
        logger.info("Transactions table created successfully")
        
        conn.execute(create_raw_sql)
        logger.info("Raw transactions staging table created")
        
        return {"status": "created", "tables": ["transactions", "raw_transactions"]}
    finally:
        conn.close()


@task(name="load_transactions_to_raw")
def load_to_raw(file_path: str) -> dict:
    """Load CSV into raw staging table."""
    logger.info(f"Loading {file_path} into raw_transactions table")
    
    conn = duckdb.connect(database=DB_PATH, read_only=False)
    try:
        conn.execute(f"""
            COPY raw_transactions FROM '{file_path}' 
            (AUTO_DETECT TRUE, HEADER TRUE, DELIMITER ',')
        """)
        
        result = conn.execute("SELECT COUNT(*) FROM raw_transactions").fetchone()
        row_count = result[0]
        
        logger.info(f"Loaded {row_count} rows into raw_transactions")
        
        return {
            "rows_loaded": row_count,
            "status": "success"
        }
    finally:
        conn.close()


@task(name="transform_and_load_transactions")
def transform_and_load() -> dict:
    """
    Transform data from raw_transactions to transactions table.
    This will INTENTIONALLY FAIL due to data quality issues.
    """
    logger.info("Starting transformation from raw_transactions to transactions")
    
    conn = duckdb.connect(database=DB_PATH, read_only=False)
    try:
        # Check data quality issues
        logger.info("Checking raw data quality...")
        
        # Count NULL categories
        null_categories = conn.execute("SELECT COUNT(*) FROM raw_transactions WHERE category IS NULL OR category = ''").fetchone()[0]
        logger.warning(f"Found {null_categories} rows with NULL/empty category")
        
        # Count invalid amounts
        invalid_amounts = conn.execute("SELECT COUNT(*) FROM raw_transactions WHERE amount = 'INVALID'").fetchone()[0]
        logger.warning(f"Found {invalid_amounts} rows with invalid amount")
        
        # Count negative amounts
        neg_amounts = conn.execute("SELECT COUNT(*) FROM raw_transactions WHERE amount LIKE '-%' OR (amount != 'INVALID' AND amount != '' AND CAST(amount AS FLOAT) < 0)").fetchone()[0]
        logger.warning(f"Found {neg_amounts} rows with negative amount")
        
        # Count invalid dates
        invalid_dates = conn.execute("SELECT COUNT(*) FROM raw_transactions WHERE date NOT LIKE '%-%-%'").fetchone()[0]
        logger.warning(f"Found {invalid_dates} rows with invalid date")
        
        # Count invalid statuses
        invalid_statuses = conn.execute("SELECT COUNT(*) FROM raw_transactions WHERE status NOT IN ('completed', 'pending', 'cancelled', 'refunded')").fetchone()[0]
        logger.warning(f"Found {invalid_statuses} rows with invalid status")
        
        # Now try to insert - THIS WILL FAIL
        logger.info("Attempting to insert into transactions table (this will fail)...")
        
        insert_sql = """
        INSERT INTO transactions (transaction_id, customer_id, amount, date, category, status)
        SELECT 
            CAST(transaction_id AS INTEGER),
            CAST(customer_id AS INTEGER),
            CAST(amount AS FLOAT),        -- This will fail on 'INVALID' and negative values
            CAST(date AS DATE),           -- This will fail on 'not-a-date'
            category,                    -- This will fail on NULL values
            status                      -- This will fail on 'invalid_status'
        FROM raw_transactions
        """
        
        # This will throw an exception due to:
        # 1. NULL category values (NOT NULL constraint)
        # 2. Invalid amount values ('INVALID')
        # 3. Invalid date values ('not-a-date')
        # 4. Invalid status values ('invalid_status')
        conn.execute(insert_sql)
        
        rows_inserted = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
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
        
        raise
        
    finally:
        conn.close()


def run_transactions_pipeline(file_path: str = SOURCE_FILE) -> dict:
    """Run the transactions ingestion pipeline manually."""
    logger.info("=" * 80)
    logger.info("Starting transactions data ingestion pipeline")
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
        tables = create_target_tables()
        logger.info(f"Tables created: {tables}")
        result["tables"] = tables
        
        # Step 3: Load to raw staging
        logger.info("\n--- Step 3: Loading to raw staging table ---")
        raw_load = load_to_raw(file_path)
        logger.info(f"Raw load result: {raw_load}")
        result["raw_load"] = raw_load
        
        # Step 4: Transform and load - THIS WILL FAIL
        logger.info("\n--- Step 4: Transforming and loading to transactions table ---")
        logger.warning("This step is expected to FAIL due to data quality issues!")
        transform_result = transform_and_load()
        logger.info(f"Transform result: {transform_result}")
        result["transform"] = transform_result
        
        result["status"] = "success"
        logger.info("\nTransactions ingestion pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"\nPipeline failed with error: {type(e).__name__}: {e}")
        result["error"] = str(e)
        result["error_type"] = type(e).__name__
        raise
    
    return result


@flow(name="transactions_ingestion_pipeline", log_prints=True)
def transactions_ingestion_pipeline(file_path: str = SOURCE_FILE):
    """Prefect flow wrapper for the transactions ingestion pipeline."""
    logger = get_run_logger()
    return run_transactions_pipeline(file_path)


if __name__ == "__main__":
    # Run the pipeline directly (without Prefect orchestration)
    try:
        result = run_transactions_pipeline()
        print(f"\nPipeline result: {result}")
    except Exception as e:
        print(f"\nPipeline failed (as expected): {type(e).__name__}: {e}")
        import sys
        sys.exit(1)
