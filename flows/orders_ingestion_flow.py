"""
Prefect flow for orders data ingestion with intentional errors for troubleshooting demo.
This flow will fail due to data quality issues in the source CSV.
"""

from prefect import flow, task, get_run_logger
import duckdb
import os
from pathlib import Path
import logging

from utils.paths import paths

# Configure logging
orders_logger = logging.getLogger('orders_ingestion')
orders_logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(str(paths.orders_ingestion_log))
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
orders_logger.addHandler(file_handler)

logger = orders_logger

DATA_DIR = paths.data_dir
DB_PATH = str(paths.database)
SOURCE_FILE = str(paths.orders_data)


@task(name="validate_orders_source_file")
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


@task(name="create_orders_tables")
def create_target_tables() -> dict:
    """Create the orders tables with strict schema (will cause errors with bad data)."""
    logger.info(f"Creating orders tables in DuckDB at {DB_PATH}")
    
    # Target table with NOT NULL constraints and type requirements
    create_orders_sql = """
    CREATE OR REPLACE TABLE orders (
        order_id INTEGER NOT NULL,
        customer_id INTEGER NOT NULL,
        order_date DATE NOT NULL,
        total_amount FLOAT NOT NULL,
        shipping_address VARCHAR NOT NULL,
        item_count INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (order_id)
    )
    """
    
    # Raw staging table
    create_raw_sql = """
    CREATE OR REPLACE TABLE raw_orders (
        order_id VARCHAR,
        customer_id VARCHAR,
        order_date VARCHAR,
        total_amount VARCHAR,
        shipping_address VARCHAR,
        item_count VARCHAR
    )
    """
    
    conn = duckdb.connect(database=DB_PATH, read_only=False)
    try:
        conn.execute(create_orders_sql)
        logger.info("Orders table created successfully")
        
        conn.execute(create_raw_sql)
        logger.info("Raw orders staging table created")
        
        return {"status": "created", "tables": ["orders", "raw_orders"]}
    finally:
        conn.close()


@task(name="load_orders_to_raw")
def load_to_raw(file_path: str) -> dict:
    """Load CSV into raw staging table."""
    logger.info(f"Loading {file_path} into raw_orders table")
    
    conn = duckdb.connect(database=DB_PATH, read_only=False)
    try:
        conn.execute(f"""
            COPY raw_orders FROM '{file_path}' 
            (AUTO_DETECT TRUE, HEADER TRUE, DELIMITER ',')
        """)
        
        result = conn.execute("SELECT COUNT(*) FROM raw_orders").fetchone()
        row_count = result[0]
        
        logger.info(f"Loaded {row_count} rows into raw_orders")
        
        return {
            "rows_loaded": row_count,
            "status": "success"
        }
    finally:
        conn.close()


@task(name="transform_and_load_orders")
def transform_and_load() -> dict:
    """
    Transform data from raw_orders to orders table.
    This will INTENTIONALLY FAIL due to data quality issues.
    """
    logger.info("Starting transformation from raw_orders to orders")
    
    conn = duckdb.connect(database=DB_PATH, read_only=False)
    try:
        # Check data quality issues
        logger.info("Checking raw data quality...")
        
        # Count NULL shipping addresses
        null_addresses = conn.execute("SELECT COUNT(*) FROM raw_orders WHERE shipping_address IS NULL OR shipping_address = ''").fetchone()[0]
        logger.warning(f"Found {null_addresses} rows with NULL/empty shipping_address")
        
        # Count NULL item_counts
        null_items = conn.execute("SELECT COUNT(*) FROM raw_orders WHERE item_count IS NULL OR item_count = ''").fetchone()[0]
        logger.warning(f"Found {null_items} rows with NULL/empty item_count")
        
        # Count negative amounts
        neg_amounts = conn.execute("SELECT COUNT(*) FROM raw_orders WHERE total_amount LIKE '-%' OR (total_amount != 'INVALID' AND total_amount != '' AND CAST(total_amount AS FLOAT) < 0)").fetchone()[0]
        logger.warning(f"Found {neg_amounts} rows with negative total_amount")
        
        # Count invalid dates
        invalid_dates = conn.execute("SELECT COUNT(*) FROM raw_orders WHERE order_date NOT LIKE '%-%-%'").fetchone()[0]
        logger.warning(f"Found {invalid_dates} rows with invalid order_date")
        
        # Now try to insert - THIS WILL FAIL
        logger.info("Attempting to insert into orders table (this will fail)...")
        
        insert_sql = """
        INSERT INTO orders (order_id, customer_id, order_date, total_amount, shipping_address, item_count)
        SELECT 
            CAST(order_id AS INTEGER),
            CAST(customer_id AS INTEGER),
            CAST(order_date AS DATE),        -- This will fail on 'bad-date' and 'INVALID'
            CAST(total_amount AS FLOAT),     -- This will fail on negative values if constraints were enforced
            shipping_address,                -- This will fail on NULL values
            CAST(item_count AS INTEGER)       -- This will fail on NULL values
        FROM raw_orders
        """
        
        # This will throw an exception due to:
        # 1. NULL shipping_address values (NOT NULL constraint)
        # 2. NULL item_count values (NOT NULL constraint)
        # 3. Invalid order_date values ('bad-date', 'INVALID')
        conn.execute(insert_sql)
        
        rows_inserted = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
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


def run_orders_pipeline(file_path: str = SOURCE_FILE) -> dict:
    """Run the orders ingestion pipeline manually."""
    logger.info("=" * 80)
    logger.info("Starting orders data ingestion pipeline")
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
        logger.info("\n--- Step 4: Transforming and loading to orders table ---")
        logger.warning("This step is expected to FAIL due to data quality issues!")
        transform_result = transform_and_load()
        logger.info(f"Transform result: {transform_result}")
        result["transform"] = transform_result
        
        result["status"] = "success"
        logger.info("\nOrders ingestion pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"\nPipeline failed with error: {type(e).__name__}: {e}")
        result["error"] = str(e)
        result["error_type"] = type(e).__name__
        raise
    
    return result


@flow(name="orders_ingestion_pipeline", log_prints=True)
def orders_ingestion_pipeline(file_path: str = SOURCE_FILE):
    """Prefect flow wrapper for the orders ingestion pipeline."""
    logger = get_run_logger()
    return run_orders_pipeline(file_path)


if __name__ == "__main__":
    # Run the pipeline directly (without Prefect orchestration)
    try:
        result = run_orders_pipeline()
        print(f"\nPipeline result: {result}")
    except Exception as e:
        print(f"\nPipeline failed (as expected): {type(e).__name__}: {e}")
        import sys
        sys.exit(1)
