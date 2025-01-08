import duckdb
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('duckdb_query.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


try:
    # Connect to DuckDB
    logger.info("Connecting to DuckDB...")
    conn = duckdb.connect()
    
    # Configure S3
    s3_url = "s3://sim-api-data/simulated_api_data.json"
    conn.execute("""
        SET s3_access_key_id='admin';
        SET s3_url_style='path';
        SET s3_secret_access_key='password123';
        SET s3_endpoint='localhost:9000';
        SET s3_use_ssl=false;
    """)
    logger.info("S3 settings configured")

    # Execute query
    logger.info(f"Querying file: {s3_url}")
    result = conn.execute(f"SELECT * FROM read_json_auto('{s3_url}')").df()
    logger.info("Query executed successfully")
    print(result.head())

except Exception as e:
    logger.error(f"Error occurred: {str(e)}")
    sys.exit(1)

finally:
    conn.close()
    logger.info("Connection closed")
