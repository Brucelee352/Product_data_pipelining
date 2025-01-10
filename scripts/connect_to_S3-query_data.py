import os
import sys
from pathlib import Path
import logging
import minio
import duckdb
import generate_fake_data # type: ignore

# Configure logging

log_dir = Path('logs')
log_dir.mkdir(parents=True, exist_ok=True)

log_file_path = log_dir / 'connect_to_S3-query_data.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# S3 upload function, uploads JSON and Parquet files to MinIO


def s3_upload():
    """Upload JSON and Parquet files to MinIO S3 storage.
    
    Connects to MinIO, creates bucket if needed, and uploads the generated data files.
    Handles MinIO connection and upload errors with appropriate logging.
    """
    try:
        log.info("Attempting to connect to MinIO...")
        client = minio.Minio("localhost:9000",
                             access_key="admin",
                             secret_key="password123",
                             secure=False)
        log.info("Connected to MinIO")
    except minio.error.MinioException as e:
        log.error("Error connecting to MinIO: %s", str(e))
        exit(1)

    script_dir = os.path.dirname(os.path.abspath(__file__))

    api_data_json = os.path.join(
        script_dir, "..", "data", "simulated_api_data.json")
    api_data_parquet = os.path.join(
        script_dir, "..", "data", "simulated_api_data.parquet")

    bucket = "sim-api-data"
    destination_name_a = "simulated_api_data.json"
    destination_name_b = "simulated_api_data.parquet"

    found = client.bucket_exists(bucket)

    if not found:
        try:
            client.make_bucket(bucket)
            log.info("Bucket %s created", bucket)
        except minio.error.MinioException as e:
            log.error("Error creating bucket %s: %s", bucket, e)
    else:
        log.info("Bucket %s already exists", bucket)

    for source, destination in [(api_data_json, destination_name_a),
                                (api_data_parquet, destination_name_b)]:
        try:
            client.fput_object(bucket, destination, source)
            log.info("File %s uploaded to %s as %s",
                     source, bucket, destination)
        except (minio.error.MinioException, IOError) as e:
            log.error("Error uploading file %s to %s: %s",
                      source, bucket, str(e))
            exit(1)


def query_data():
    """Query data from S3 using DuckDB.
    
    Connects to DuckDB, configures S3 settings, and executes a query on the JSON data.
    Prints the first 5 rows of results and handles database errors.
    """
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

        # Query execution from S3 file:
        log.info("Querying file: %s", s3_url)

        # Export the results from the query to a separate dataframe:
        results = conn.execute(
            f"SELECT * FROM read_json_auto('{s3_url}')").df()
        
        df = results
        
        # Data cleaning: 
        
        # Queries for transforming the data:
        
        

        # Log query result, and print first 5 rows of results to console:
        log.info("Query executed successfully")
        log.info(results.head())

    except (duckdb.Error, IOError, ConnectionError) as e:
        log.error("Error occurred: %s", str(e))
        sys.exit(1)

    finally:
        conn.close()
        log.info("Connection closed")


def main():
    """Execute the main 'def main()' data pipeline workflow.
    
    Orchestrates the data pipeline:
    1. Generates simulated data
    2. Uploads data to MinIO S3
    3. Queries the uploaded data using DuckDB
    
    Handles errors from MinIO, DuckDB, and I/O operations.
    """
    try:
        # Generate fresh data
        log.info("Generating fake data...")
        generate_fake_data.generate_data()
        log.info("Fake data generated successfully")

        # Upload to S3
        s3_upload()

        # Query the data
        query_data()

    except (minio.error.MinioException, duckdb.Error, IOError, ConnectionError) as e:
        log.error("Pipeline failed: %s", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
