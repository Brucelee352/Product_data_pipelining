import os
import sys
from pathlib import Path
import logging
import minio
import duckdb
from generate_fake_data import generate_data
from queries_cleaning import clean_data

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
    Prints the first 5 rows of results and handles database errors, turns the results into a 
    dataframe, then saves it as a .csv.
    """

    try:
        # Get the project root directory and db path
        project_root = Path(__file__).parents[1]
        db_dir = project_root / 'dbt_pipeline_demo' / 'databases'
        db_path = db_dir / 'dbt_pipeline_demo.duckdb'
        
        # Connect to DuckDB
        log.info("Connecting to DuckDB...")
        conn = duckdb.connect(str(db_path))

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
        
        ## 1. Customer Lifecycle Analysis
        lifecycle_analysis = conn.execute("""
            SELECT
                COUNT(*) as total_accounts,
                SUM(is_active) as active_accounts,
                ROUND(AVG(is_active) * 100, 2) as active_percentage,
                COUNT(CASE WHEN account_deleted IS NOT NULL THEN 1 END) as churned_accounts,
                ROUND(AVG(CASE WHEN account_deleted IS NOT NULL THEN 1 ELSE 0 END) * 100, 2) as churn_rate,
                ROUND(AVG(DATEDIFF('days', account_created, COALESCE(account_deleted, CURRENT_DATE))), 2) as avg_account_lifetime_days
            FROM user_activity;
        """).fetchdf()
        log.info("\nCustomer Lifecycle Analysis:")
        log.info(lifecycle_analysis)

        ## 2. Purchase Analysis by Product and Status
        purchase_analysis = conn.execute("""
            SELECT
                product_name,
                COUNT(*) as total_views,
                SUM(CASE WHEN purchase_status = 'completed' THEN 1 ELSE 0 END) as completed_purchases,
                ROUND(AVG(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END), 2) as avg_purchase_value,
                ROUND(SUM(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END), 2) as total_revenue,
                ROUND(SUM(CASE WHEN purchase_status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as conversion_rate
            FROM user_activity
            GROUP BY product_name
            ORDER BY total_revenue DESC;
        """).fetchdf()
        log.info("\nProduct Performance Analysis:")
        log.info(purchase_analysis)

        ## 3. User Demographics and Behavior
        user_demographics = conn.execute("""
            SELECT
                device_type,
                os,
                browser,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as total_sessions,
                ROUND(COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) * 100.0 / COUNT(*), 2) as conversion_rate,
                ROUND(AVG(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END), 2) as avg_purchase_value
            FROM user_activity
            GROUP BY device_type, os, browser
            HAVING total_sessions > 10
            ORDER BY unique_users DESC;
        """).fetchdf()
        log.info("\nUser Platform Analysis:")
        log.info(user_demographics)

        ## 4. Company and Job Title Impact on Purchases
        business_analysis = conn.execute("""
            SELECT
                job_title,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(AVG(price), 2) as avg_cart_value,
                COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) as completed_purchases,
                ROUND(SUM(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END), 2) as total_revenue,
                ROUND(COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) * 100.0 / COUNT(*), 2) as conversion_rate
            FROM user_activity
            GROUP BY job_title
            HAVING unique_users > 5
            ORDER BY total_revenue DESC
            LIMIT 10;
        """).fetchdf()
        log.info("\nTop 10 Job Titles by Revenue:")
        log.info(business_analysis)

        ## 5. User Engagement Over Time
        time_analysis = conn.execute("""
            SELECT
                DATE_TRUNC('hour', login_time) as hour_of_day,
                COUNT(*) as total_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration,
                COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) as completed_purchases,
                ROUND(SUM(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END), 2) as revenue
            FROM user_activity
            GROUP BY DATE_TRUNC('hour', login_time)
            ORDER BY total_sessions DESC
            LIMIT 24;
        """).fetchdf()
        log.info("\nHourly User Engagement Patterns:")
        log.info(time_analysis)

        ## 6. Detailed Churn Analysis, refined to include more granular data
        
        ### 6a. Time-based Churn Analysis
        time_based_churn = conn.execute("""
            SELECT 
                DATE_TRUNC('month', account_created) as cohort_month,
                COUNT(*) as total_users,
                SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as churned_users,
                ROUND(AVG(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100, 2) as churn_rate,
                ROUND(AVG(CASE 
                    WHEN is_active = 0 THEN 
                        DATEDIFF('days', account_created, account_deleted)
                    END), 2) as avg_days_to_churn
            FROM user_activity
            GROUP BY DATE_TRUNC('month', account_created)
            ORDER BY cohort_month;
        """).fetchdf()

        ### 6b. Price Range Impact on Churn
        price_churn = conn.execute("""
            SELECT 
                CASE 
                    WHEN price < 500 THEN 'Low (<$500)'
                    WHEN price BETWEEN 500 AND 1000 THEN 'Medium ($500-$1000)'
                    WHEN price BETWEEN 1001 AND 2500 THEN 'High ($1001-$2500)'
                    ELSE 'Premium (>$2500)'
                END as price_tier,
                COUNT(*) as total_users,
                SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as churned_users,
                ROUND(AVG(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100, 2) as churn_rate,
                ROUND(AVG(price), 2) as avg_price
            FROM user_activity
            GROUP BY 
                CASE 
                    WHEN price < 500 THEN 'Low (<$500)'
                    WHEN price BETWEEN 500 AND 1000 THEN 'Medium ($500-$1000)'
                    WHEN price BETWEEN 1001 AND 2500 THEN 'High ($1001-$2500)'
                    ELSE 'Premium (>$2500)'
                END
            ORDER BY avg_price;
        """).fetchdf()

        ### 6c. Session Duration Impact
        engagement_churn = conn.execute("""
            SELECT 
                CASE 
                    WHEN session_duration_minutes < 30 THEN 'Very Low (<30 mins)'
                    WHEN session_duration_minutes BETWEEN 30 AND 60 THEN 'Low (30-60 mins)'
                    WHEN session_duration_minutes BETWEEN 61 AND 120 THEN 'Medium (1-2 hours)'
                    ELSE 'High (>2 hours)'
                END as engagement_level,
                COUNT(*) as total_users,
                SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as churned_users,
                ROUND(AVG(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100, 2) as churn_rate,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration
            FROM user_activity
            GROUP BY 
                CASE 
                    WHEN session_duration_minutes < 30 THEN 'Very Low (<30 mins)'
                    WHEN session_duration_minutes BETWEEN 30 AND 60 THEN 'Low (30-60 mins)'
                    WHEN session_duration_minutes BETWEEN 61 AND 120 THEN 'Medium (1-2 hours)'
                    ELSE 'High (>2 hours)'
                END
            ORDER BY avg_session_duration;
        """).fetchdf()

        ### 6d. Device and Platform Impact
        platform_churn = conn.execute("""
            SELECT 
                device_type,
                os,
                browser,
                COUNT(*) as total_users,
                SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as churned_users,
                ROUND(AVG(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100, 2) as churn_rate,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration
            FROM user_activity
            GROUP BY device_type, os, browser
            HAVING total_users > 5
            ORDER BY churn_rate DESC;
        """).fetchdf()

        ### 6e. Purchase Behavior Impact
        purchase_pattern_churn = conn.execute("""
            SELECT 
                purchase_status,
                COUNT(*) as total_users,
                SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as churned_users,
                ROUND(AVG(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) * 100, 2) as churn_rate,
                ROUND(AVG(price), 2) as avg_price,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration
            FROM user_activity
            GROUP BY purchase_status
            ORDER BY churn_rate DESC;
        """).fetchdf()
        
        ## 7. Calculate session duration and filter invalid timestamps
        session_duration = conn.execute("""
            SELECT concat(first_name, ' ', last_name) AS full_name, email, state, login_time, logout_time,
                (logout_time - login_time) AS session_duration,
                ROUND(EXTRACT(EPOCH FROM (logout_time - login_time)) / 3600, 1) AS session_duration_hours,
                EXTRACT(EPOCH FROM (logout_time - login_time)) / 60 AS session_duration_minutes
            FROM user_activity
            WHERE login_time <= logout_time
            ORDER BY state
            LIMIT 50;
            """).fetchdf()

        # Map analysis object and analysis names to dictionary, and log the head of each df via for loop
        analyses = {
            "Customer Lifecycle Analysis": lifecycle_analysis,
            "Purchase Analysis by Product and Status": purchase_analysis,
            "User Demographics and Behavior": user_demographics,
            "Company and Job Title Impact on Purchases": business_analysis,
            "User Engagement Over Time": time_analysis,
            "Churn Analysis by Time (Cohorts)": time_based_churn,
            "Churn Analysis by Price Tier": price_churn, 
            "Churn Analysis by Engagement Level": engagement_churn,
            "Churn Analysis by Platform": platform_churn,
            "Churn Analysis by Purchase Behavior": purchase_pattern_churn,
            "Session Duration Analysis": session_duration
        }

        # Log the head of each analysis df
        for name, df in analyses.items():
            log.info(f"\n{name}:")
            log.info(df.head())

        # Log successful query execution
        log.info("\nQueries executed successfully")

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
    2. Clean data
    2. Upload data to MinIO S3
    3. Query the uploaded data using DuckDB

    Handles errors from MinIO, DuckDB, and I/O operations.
    """
    try:
        # Generate fresh data
        log.info("Generating fake data...")
        generate_data()
        log.info("Fake data generated successfully")
        
        # Clean data
        log.info("Cleaning data...")
        clean_data()
        log.info("Data cleaned successfully")

        # Upload to S3
        log.info("Uploading data to S3...")
        s3_upload()
        log.info("Upload successful")

        # Query the data
        log.info("Querying data...")
        query_data()
        log.info("Query successful")

    except (minio.error.MinioException, duckdb.Error, IOError, ConnectionError) as e:
        log.error("Pipeline failed: %s", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
