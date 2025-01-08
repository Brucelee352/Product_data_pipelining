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
log = logging.getLogger(__name__)


try:
    # Connect to DuckDB
    log.info("Connecting to DuckDB...")
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
    log.info("S3 settings configured")

    # Execute query
    log.info("Querying file: %s", s3_url)
    result = conn.execute(f"SELECT * FROM read_json_auto('{s3_url}')").df()
    log.info("Query executed successfully")
    print(result.head())

except Exception as e:
    log.error("Error occurred: %s", str(e))
    sys.exit(1)

finally:
    conn.close()
    log.info("Connection closed")
