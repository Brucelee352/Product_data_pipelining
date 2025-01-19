import os
import sys
from pathlib import Path
from datetime import datetime
import logging
import minio
import duckdb
import pandas as pd
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
        start_time = datetime.now()
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

        # Debug: Check the structure of the data
        log.info("Checking data structure...")
        conn.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'user_activity';
        """).fetchdf()

        # Create and populate user_activity table
        log.info("Creating user_activity table...")
        conn.execute(f"""
            -- First, let's see what columns we have
            CREATE TABLE IF NOT EXISTS user_activity AS
            SELECT
                *,
                -- Convert is_active to proper string values, with NULL handling
                CASE
                    WHEN is_active IS NULL THEN 'no'
                    WHEN CAST(is_active AS VARCHAR) = '1' THEN 'yes'
                    WHEN CAST(is_active AS VARCHAR) = 'true' THEN 'yes'
                    WHEN CAST(is_active AS VARCHAR) = 'True' THEN 'yes'
                    ELSE 'no'
                END as is_active_str,
                CASE
                    WHEN user_agent LIKE '%Mobile%' THEN 'Mobile'
                    WHEN user_agent LIKE '%Tablet%' THEN 'Tablet'
                    ELSE 'Desktop'
                END as device_type,
                CASE
                    WHEN user_agent LIKE '%Windows%' THEN 'Windows'
                    WHEN user_agent LIKE '%Mac%' THEN 'MacOS'
                    WHEN user_agent LIKE '%Linux%' THEN 'Linux'
                    WHEN user_agent LIKE '%Android%' THEN 'Android'
                    WHEN user_agent LIKE '%iOS%' THEN 'iOS'
                    ELSE 'Other'
                END as os,
                CASE
                    WHEN user_agent LIKE '%Chrome%' THEN 'Chrome'
                    WHEN user_agent LIKE '%Firefox%' THEN 'Firefox'
                    WHEN user_agent LIKE '%Safari%' THEN 'Safari'
                    WHEN user_agent LIKE '%Edge%' THEN 'Edge'
                    ELSE 'Other'
                END as browser
            FROM read_json_auto('{s3_url}')
        """)
        log.info("user_activity table created and populated")

        # Queries for insights on user activity
        # 1. Customer Lifecycle Analysis
        lifecycle_analysis = conn.execute("""
            SELECT
                COUNT(*) as total_accounts,
                COUNT(CASE WHEN is_active_str = 'yes' THEN 1 END) as active_accounts,
                ROUND(TRY_CAST(COUNT(CASE WHEN is_active_str = 'yes' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(10,2)), 2) as active_percentage,
                COUNT(CASE WHEN account_deleted IS NOT NULL THEN 1 END) as churned_accounts,
                ROUND(TRY_CAST(COUNT(CASE WHEN account_deleted IS NOT NULL THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(10,2)), 2) as churn_rate,
                ROUND(AVG(DATEDIFF('days',
                    TRY_CAST(account_created AS TIMESTAMP),
                    COALESCE(TRY_CAST(account_deleted AS TIMESTAMP), CURRENT_TIMESTAMP)
                )), 2) as avg_account_lifetime_days
            FROM user_activity;
        """).fetchdf()
        log.info("\nCustomer Lifecycle Analysis:")
        log.info(lifecycle_analysis)

        # 2. Purchase Analysis by Product and Status
        purchase_analysis = conn.execute("""
            SELECT
                product_name,
                COUNT(*) as total_views,
                SUM(CASE WHEN purchase_status = 'completed' THEN 1 ELSE 0 END) as completed_purchases,
                ROUND(TRY_CAST(AVG(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END) AS DECIMAL(10,2)), 2) as avg_purchase_value,
                ROUND(TRY_CAST(SUM(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END) AS DECIMAL(10,2)), 2) as total_revenue,
                ROUND(TRY_CAST(SUM(CASE WHEN purchase_status = 'completed' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(10,2)), 2) as conversion_rate
            FROM user_activity
            GROUP BY product_name
            ORDER BY total_revenue DESC;
        """).fetchdf()
        log.info("\nProduct Performance Analysis:")
        log.info(purchase_analysis)

        # 3. User Demographics and Behavior
        user_demographics = conn.execute("""
            SELECT
                device_type,
                os,
                browser,
                ROUND(TRY_CAST(AVG(session_duration_minutes) AS DECIMAL(10,2)), 2) as avg_session_duration,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as total_sessions,
                ROUND(TRY_CAST(COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(10,2)), 2) as conversion_rate,
                ROUND(TRY_CAST(AVG(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END) AS DECIMAL(10,2)), 2) as avg_purchase_value
            FROM user_activity
            GROUP BY device_type, os, browser
            HAVING COUNT(*) > 10
            ORDER BY unique_users DESC;
        """).fetchdf()
        log.info("\nUser Platform Analysis:")
        log.info(user_demographics)

        # 4. Company and Job Title Impact on Purchases
        business_analysis = conn.execute("""
            SELECT
                job_title,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(TRY_CAST(AVG(price) AS DECIMAL(10,2)), 2) as avg_cart_value,
                COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) as completed_purchases,
                ROUND(TRY_CAST(SUM(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END) AS DECIMAL(10,2)), 2) as total_revenue,
                ROUND(TRY_CAST(COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(10,2)), 2) as conversion_rate
            FROM user_activity
            GROUP BY job_title
            HAVING COUNT(DISTINCT user_id) > 5
            ORDER BY total_revenue DESC
            LIMIT 10;
        """).fetchdf()
        log.info("\nTop 10 Job Titles by Revenue:")
        log.info(business_analysis)

        # 5. User Engagement Over Time
        time_analysis = conn.execute("""
            SELECT
                DATE_TRUNC('hour', TRY_CAST(login_time AS TIMESTAMP)) as hour_of_day,
                COUNT(*) as total_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(TRY_CAST(AVG(session_duration_minutes) AS DECIMAL(10,2)), 2) as avg_session_duration,
                COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) as completed_purchases,
                ROUND(TRY_CAST(SUM(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END) AS DECIMAL(10,2)), 2) as revenue
            FROM user_activity
            GROUP BY DATE_TRUNC('hour', TRY_CAST(login_time AS TIMESTAMP))
            ORDER BY total_sessions DESC
            LIMIT 24;
        """).fetchdf()
        log.info("\nHourly User Engagement Patterns:")
        log.info(time_analysis)

        # 6. Detailed Churn Analysis, refined to include more granular data

        # 6a. Time-based Churn Analysis
        time_based_churn = conn.execute("""
            SELECT
                DATE_TRUNC('month', TRY_CAST(account_created AS TIMESTAMP)) as cohort_month,
                COUNT(*) as total_users,
                SUM(CASE WHEN is_active_str = 'no' THEN 1 ELSE 0 END) as churned_users,
                ROUND(TRY_CAST(AVG(CASE WHEN is_active_str = 'no' THEN 1 ELSE 0 END) * 100 AS DECIMAL(10,2)), 2) as churn_rate,
                ROUND(TRY_CAST(AVG(CASE
                    WHEN is_active_str = 'no' THEN
                        DATEDIFF('days', TRY_CAST(account_created AS TIMESTAMP), TRY_CAST(account_deleted AS TIMESTAMP))
                    END) AS DECIMAL(10,2)), 2) as avg_days_to_churn
            FROM user_activity
            GROUP BY DATE_TRUNC('month', TRY_CAST(account_created AS TIMESTAMP))
            ORDER BY cohort_month;
        """).fetchdf()

        # 6b. Price Range Impact on Churn
        price_churn = conn.execute("""
            SELECT
                CASE
                    WHEN price < 500 THEN 'Low (<$500)'
                    WHEN price BETWEEN 500 AND 1000 THEN 'Medium ($500-$1000)'
                    WHEN price BETWEEN 1001 AND 2500 THEN 'High ($1001-$2500)'
                    ELSE 'Premium (>$2500)'
                END as price_tier,
                COUNT(*) as total_users,
                SUM(CASE WHEN is_active_str = 'no' THEN 1 ELSE 0 END) as churned_users,
                ROUND(TRY_CAST(AVG(CASE WHEN is_active_str = 'no' THEN 1 ELSE 0 END) * 100 AS DECIMAL(10,2)), 2) as churn_rate,
                ROUND(TRY_CAST(AVG(price) AS DECIMAL(10,2)), 2) as avg_price
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

        # 6c. Session Duration Impact
        engagement_churn = conn.execute("""
            SELECT
                CASE
                    WHEN session_duration_minutes < 30 THEN 'Very Low (<30 mins)'
                    WHEN session_duration_minutes BETWEEN 30 AND 60 THEN 'Low (30-60 mins)'
                    WHEN session_duration_minutes BETWEEN 61 AND 120 THEN 'Medium (1-2 hours)'
                    ELSE 'High (>2 hours)'
                END as engagement_level,
                COUNT(*) as total_users,
                SUM(CASE WHEN is_active_str = 'no' THEN 1 ELSE 0 END) as churned_users,
                ROUND(TRY_CAST(AVG(CASE WHEN is_active_str = 'no' THEN 1 ELSE 0 END) * 100 AS DECIMAL(10,2)), 2) as churn_rate,
                ROUND(TRY_CAST(AVG(session_duration_minutes) AS DECIMAL(10,2)), 2) as avg_session_duration
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

        # 6d. Device and Platform Impact
        platform_churn = conn.execute("""
            SELECT
                device_type,
                os,
                browser,
                COUNT(*) as total_users,
                SUM(CASE WHEN is_active_str = 'no' THEN 1 ELSE 0 END) as churned_users,
                ROUND(TRY_CAST(AVG(CASE WHEN is_active_str = 'no' THEN 1 ELSE 0 END) * 100 AS DECIMAL(10,2)), 2) as churn_rate,
                ROUND(TRY_CAST(AVG(session_duration_minutes) AS DECIMAL(10,2)), 2) as avg_session_duration
            FROM user_activity
            GROUP BY device_type, os, browser
            HAVING COUNT(*) > 5
            ORDER BY churn_rate DESC;
        """).fetchdf()

        # 6e. Purchase Behavior Impact
        purchase_pattern_churn = conn.execute("""
            SELECT
                purchase_status,
                COUNT(*) as total_users,
                SUM(CASE WHEN is_active_str = 'no' THEN 1 ELSE 0 END) as churned_users,
                ROUND(TRY_CAST(AVG(CASE WHEN is_active_str = 'no' THEN 1 ELSE 0 END) * 100 AS DECIMAL(10,2)), 2) as churn_rate,
                ROUND(TRY_CAST(AVG(price) AS DECIMAL(10,2)), 2) as avg_price,
                ROUND(TRY_CAST(AVG(session_duration_minutes) AS DECIMAL(10,2)), 2) as avg_session_duration
            FROM user_activity
            GROUP BY purchase_status
            ORDER BY churn_rate DESC;
        """).fetchdf()

        # 7. Calculate session duration and filter invalid timestamps
        session_duration = conn.execute("""
            SELECT
                concat(first_name, ' ', last_name) AS full_name,
                email,
                state,
                TRY_CAST(login_time AS TIMESTAMP) as login_time,
                TRY_CAST(logout_time AS TIMESTAMP) as logout_time,
                (TRY_CAST(logout_time AS TIMESTAMP) - TRY_CAST(login_time AS TIMESTAMP)) AS session_duration,
                ROUND(EXTRACT(EPOCH FROM (TRY_CAST(logout_time AS TIMESTAMP) - TRY_CAST(login_time AS TIMESTAMP))) / 3600.0, 1) AS session_duration_hours,
                EXTRACT(EPOCH FROM (TRY_CAST(logout_time AS TIMESTAMP) - TRY_CAST(login_time AS TIMESTAMP))) / 60.0 AS session_duration_minutes
            FROM user_activity
            WHERE TRY_CAST(login_time AS TIMESTAMP) <= TRY_CAST(logout_time AS TIMESTAMP)
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
            log.info("%s", name)
            log.info(df.head())

        # Create a directory for KPI reports if it doesn't exist
        reports_dir = project_root / 'reports'
        reports_dir.mkdir(parents=True, exist_ok=True)

        # Save each analysis to CSV with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        for name, df in analyses.items():
            # Create safe filename from analysis name
            safe_name = name.lower().replace(' ', '_').replace('(', '').replace(')', '')
            filename = f"{safe_name}_{timestamp}.csv"
            filepath = reports_dir / filename
            df.to_csv(filepath, index=False)
            log.info(f"Saved {name} to {filepath}")

        # Add performance metrics
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        log.info(f"Total query execution time: {execution_time:.2f} seconds")

        performance_metrics = {
            'execution_timestamp': start_time,
            'execution_time_seconds': execution_time,
            'queries_executed': len(analyses)
        }

        # Save performance metrics
        performance_df = pd.DataFrame([performance_metrics])
        performance_file = reports_dir / f"query_performance_{timestamp}.csv"
        performance_df.to_csv(performance_file, index=False)

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

        # Query the data and make csvs for reports
        log.info("Querying data...")
        query_data()
        log.info("Query successful")

    except (minio.error.MinioException,
            duckdb.Error, IOError, ConnectionError) as e:
        log.error("Pipeline failed: %s", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
