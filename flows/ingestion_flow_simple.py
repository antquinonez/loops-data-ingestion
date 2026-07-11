"""
Simple data ingestion script with intentional errors for troubleshooting demo.
This script will fail due to data quality issues in the source CSV.
Uses plain Python without Prefect to avoid server requirements.
"""

import duckdb
import csv
import os
import json
import sys
import logging
from datetime import datetime
from pathlib import Path

# Import path configuration
try:
    from utils.paths import paths
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.paths import paths

# Get run ID from environment for unique log files
RUN_ID = os.environ.get('RUN_ID', datetime.now().strftime('%Y%m%d_%H%M%S'))

# Setup simple logging
logs_dir = paths.logs_dir
logs_dir.mkdir(parents=True, exist_ok=True)
log_path = logs_dir / f"ingestion_{RUN_ID}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(str(log_path)),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

DATA_DIR = paths.data_dir
DB_PATH = str(paths.database)
SOURCE_FILE = str(paths.source_data)

logger.info("=" * 80)
logger.info("Starting data ingestion pipeline")
logger.info("=" * 80)
logger.info(f"Source file: {SOURCE_FILE}")
logger.info(f"Database: {DB_PATH}")

try:
    # Step 1: Validate source file
    logger.info("\n--- Step 1: Validating source file ---")
    with open(SOURCE_FILE, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        file_size = os.path.getsize(SOURCE_FILE)
    
    logger.info(f"Source file validated: {SOURCE_FILE} ({file_size} bytes)")
    logger.info(f"Validation result: {{'file_path': '{SOURCE_FILE}', 'file_size': {file_size}, 'status': 'valid'}}")
    
    # Step 2: Create target tables
    logger.info("\n--- Step 2: Creating target tables ---")
    conn = duckdb.connect(database=DB_PATH, read_only=False)
    
    try:
        # Create users table with strict schema
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER NOT NULL PRIMARY KEY,
                name VARCHAR NOT NULL,
                email VARCHAR NOT NULL,
                age INTEGER NOT NULL,
                join_date DATE NOT NULL,
                status VARCHAR NOT NULL,
                score FLOAT NOT NULL
            )
        """)
        
        # Create raw staging table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS raw_users AS 
            SELECT * FROM read_csv_auto('{SOURCE_FILE}')
        """)
        
        logger.info("Target table created successfully")
        logger.info("Raw staging table created")
        logger.info("Tables created: {'status': 'created', 'tables': ['users', 'raw_users']}")
        
        # Step 3: Load to raw staging table
        logger.info("\n--- Step 3: Loading to raw staging table ---")
        row_count = conn.execute("SELECT COUNT(*) FROM raw_users").fetchone()[0]
        logger.info(f"Loaded {row_count} rows into raw_users")
        logger.info(f"Raw load result: {{'rows_loaded': {row_count}, 'status': 'success'}}")
        
        # Step 4: Transform and load to users table (this will fail)
        logger.info("\n--- Step 4: Transforming and loading to users table ---")
        logger.warning("This step is expected to FAIL due to data quality issues!")
        
        logger.info("Checking raw data quality...")
        # Check for NULL emails
        null_emails = conn.execute("SELECT COUNT(*) FROM raw_users WHERE email IS NULL OR email = ''").fetchone()[0]
        if null_emails > 0:
            logger.warning(f"Found {null_emails} rows with NULL/empty email")
        
        # Check for non-numeric age - use TRY_CAST to avoid errors
        non_numeric_age = conn.execute("SELECT COUNT(*) FROM raw_users WHERE TRY_CAST(age AS INTEGER) IS NULL AND age IS NOT NULL AND age != ''").fetchone()[0]
        if non_numeric_age > 0:
            logger.warning(f"Found {non_numeric_age} rows with non-numeric age")
        
        logger.info("Attempting to insert into users table (this will fail)...")
        
        # This will fail due to 'N/A' in age column
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
        
        conn.execute(insert_sql)
        
        logger.info("Transformation succeeded!")
        
    except Exception as e:
        logger.error(f"Transformation failed: {type(e).__name__}: {e}")
        logger.error("\n--- ERROR CONTEXT ---")
        logger.error(f"Database: {DB_PATH}")
        logger.error(f"Source: {SOURCE_FILE}")
        logger.error(f"Timestamp: {datetime.now().isoformat()}")
        
        # Get sample bad rows
        try:
            sample_rows = conn.execute("SELECT * FROM raw_users LIMIT 5").fetchall()
            logger.error(f"Sample bad rows: {sample_rows}")
        except:
            pass
        
        raise
    finally:
        conn.close()
        
except Exception as e:
    logger.error(f"\nPipeline failed with error: {type(e).__name__}: {e}")
    sys.exit(1)

logger.info("\nPipeline completed successfully!")
sys.exit(0)
