import os
import sys
from pathlib import Path
from datetime import datetime
import json
import random
import logging
from datetime import datetime as dt
from datetime import timedelta
import minio
import duckdb
from faker import Faker
from user_agents import parse
import pandas as pd


log_dir = Path('logs')
log_dir.mkdir(parents=True, exist_ok=True)

log_file_path = log_dir / 'generate_fake_data.log'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/generate_fake_data.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# Constants
num_rows = 8000
start_datetime = dt(2022, 1, 1, 10, 30)
end_datetime = dt(2024, 12, 31, 23, 59)
valid_statuses = ['pending', 'completed', 'failed']

# Configure Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# S3 Configuration
S3_CONFIG = {
    'endpoint': 'localhost:9000',
    'access_key': 'admin',
    'secret_key': 'password123',
    'bucket': 'sim-api-data',
    'use_ssl': False,
    'url_style': 'path'
}

# S3 URL for cleaned data
S3_URL = f"s3://{S3_CONFIG['bucket']}/cleaned_data.json"


def generate_data():
    """Generate and save fake data to JSON, Parquet, and CSV files."""
    try:
        data = []
        # Add data validation during generation
        for _ in range(num_rows):
            # Persist user data for each record
            first_name = fake.first_name()
            last_name = fake.last_name()

            # Generates data around account creation, deletion and updates
            is_active = 1 if random.random() < 0.8 else 0
            account_created = fake.date_time_between(
                start_date=start_datetime, end_date=end_datetime)
            account_updated = fake.date_time_between(
                start_date=account_created, end_date=end_datetime)
            account_deleted = fake.date_time_between(
                start_date=account_updated, end_date=end_datetime) if is_active == 0 else None

            # Generates login and logout times for each record
            login_time = fake.date_time_between(
                start_date=account_created,
                end_date=account_deleted if account_deleted else end_datetime
            )

            # This guarantees that logout time is always .5 to 4 hours after login time
            logout_time = login_time + timedelta(hours=random.uniform(0.5, 4))

            # Calculates session duration in minutes
            session_duration = (logout_time - login_time).total_seconds() / 60

            record = {
                "user_id": fake.uuid4(),
                "first_name": first_name,
                "last_name": last_name,
                "email": f"{first_name}_{last_name}@example.com",
                "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=72).isoformat(),
                "phone_number": fake.phone_number(),
                "address": fake.address(),
                "city": fake.city(),
                "state": fake.state(),
                "postal_code": fake.postcode(),
                "country": fake.country(),
                "company": fake.company(),
                "job_title": fake.job(),
                "ip_address": fake.ipv4(),
                "is_active": is_active,
                "login_time": login_time.isoformat(),
                "logout_time": logout_time.isoformat(),
                "account_created": account_created.isoformat(),
                "account_updated": account_updated.isoformat(),
                "account_deleted": account_deleted.isoformat() if account_deleted else None,
                "session_duration_minutes": round(session_duration, 2),
                "product_id": fake.uuid4(),
                "product_name": fake.random_element(["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta", "Iota", "Kappa", "Lambda", "Mu", "Nu", "Xi", "Omicron", "Pi", "Rho", "Sigma", "Tau", "Upsilon", "Phi", "Chi", "Psi", "Omega"]),
                "price": round(fake.pyfloat(min_value=100, max_value=5000, right_digits=2), 2),
                "purchase_status": fake.random_element(["completed", "pending", "failed"]),
                "user_agent": fake.user_agent()
            }

            # Validate record before adding
            if not all([
                record['login_time'] < record['logout_time'],
                record['account_created'] <= record['account_updated'],
                record['price'] > 0
            ]):
                continue

            data.append(record)

        # Add basic statistics logging
        log.info("Generated %s records", num_rows)
        log.info("Active users: %s", sum(1 for r in data if r['is_active']))
        log.info("Average price: $%s", sum(r['price'] for r in data)/len(data))

        return data

    except Exception as e:
        log.error("Error generating data: %s", str(e))
        raise


def validate_timestamps(row):
    """Function validates the timestamps for the login and logout times, to ensure consistency."""
    try:
        login = pd.to_datetime(row['login_time'])
        logout = pd.to_datetime(row['logout_time'])
        return pd.notnull(login) and pd.notnull(logout) and login <= logout
    except Exception as e:
        log.error("Error validating timestamps for row %s: %s", row.name, str(e))
        return False


def save_data_formats(df, project_root):
    """Save cleaned data in multiple formats (CSV, JSON, Parquet)."""
    try:
        data_dir = project_root / 'data'
        data_dir.mkdir(parents=True, exist_ok=True)

        # Save in different formats
        csv_path = data_dir / 'cleaned_data.csv'
        json_path = data_dir / 'cleaned_data.json'
        parquet_path = data_dir / 'cleaned_data.parquet'

        df.to_csv(csv_path, index=False)
        df.to_json(json_path, orient='records', date_format='iso')
        df.to_parquet(parquet_path, index=False)

        log.info("Data saved in CSV, JSON, and Parquet formats")
        return csv_path, json_path, parquet_path
    except Exception as e:
        log.error("Error saving data formats: %s", str(e))
        raise


def prepare_data():
    """Function prepares the generated data for cleaning and loading."""
    try:
        # Get the project root directory and setup paths
        project_root = Path(__file__).parents[1]
        data_dir = project_root / 'data'
        db_dir = project_root / 'dbt_pipeline_demo' / 'databases'
        db_dir.mkdir(parents=True, exist_ok=True)

        # Load and process data
        df = pd.read_csv(data_dir / 'simulated_api_data.csv')
        df = process_basic_cleaning(df)
        df = process_advanced_cleaning(df)

        # Save metrics and data
        save_metrics(df, project_root)
        file_paths = save_data_formats(df, project_root)

        return df, file_paths

    except Exception as e:
        log.error("Error in data cleaning: %s", str(e))
        raise


def process_basic_cleaning(df):
    """Handle basic data cleaning operations."""
    # Generate transaction ID
    df['transact_id'] = df.apply(
        lambda row: f"txn_{row['user_id']}_{pd.to_datetime(
            row['login_time']).strftime('%Y%m%d%H%M%S')}"
        if pd.notnull(row['login_time']) else None,
        axis=1
    )

    # Basic cleaning steps
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    df = df.drop_duplicates(subset=['email'])

    # Remove irrelevant columns
    drop_columns = ['phone_number', 'email_address',
                    'city', 'postal_code', 'country', 'product_id']
    df = df.drop([col for col in drop_columns if col in df.columns], axis=1)

    df['country'] = 'United States'
    df['is_active'] = df['is_active'].replace({0: 'no', 1: 'yes'})

    return df[['user_id', 'transact_id', 'first_name', 'last_name', 'email',
              'date_of_birth', 'address', 'state', 'country',
               'company', 'job_title', 'ip_address', 'is_active', 'login_time',
               'logout_time', 'account_created', 'account_updated', 'account_deleted',
               'session_duration_minutes', 'product_name', 'price', 'purchase_status',
               'user_agent']]


def process_advanced_cleaning(df):
    """Handle advanced data processing and validation."""
    # Process user agent data
    df[['device_type', 'os', 'browser']] = df['user_agent'].apply(lambda ua: pd.Series({
        'device_type': parse(ua).device.family,
        'os': parse(ua).os.family,
        'browser': parse(ua).browser.family
    }))
    df['device_type'] = df['device_type'].str.replace('Other', 'Desktop')

    # Validate and filter data
    df = df[df.apply(validate_timestamps, axis=1)]
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df = df[df['price'] > 0]

    df['purchase_status'] = df['purchase_status'].str.lower()
    df = df[df['purchase_status'].isin(valid_statuses)]
    return df


def add_analysis_fields(df):
    """Add additional analysis fields to the dataframe."""
    # Ensure datetime columns are properly formatted
    date_columns = ['account_created', 'login_time',
                    'logout_time', 'account_updated', 'account_deleted']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Safe string format for cohort date
    df['cohort_date'] = df['account_created'].dt.strftime('%Y-%m')

    # Handle potential NaT in date differences
    df['user_age_days'] = (
        df['login_time'] - df['account_created']).dt.days.fillna(0)

    # Handle potential nulls in numeric cuts
    df['engagement_level'] = pd.cut(
        df['session_duration_minutes'].fillna(0),
        bins=[0, 30, 60, 120, float('inf')],
        labels=['Very Low', 'Low', 'Medium', 'High']
    )

    # Handle potential empty groups in qcut
    try:
        df['price_tier'] = pd.qcut(
            df['price'],
            q=4,
            labels=['Budget', 'Standard', 'Premium', 'Luxury']
        )
    except ValueError:
        # Fallback if not enough distinct values
        df['price_tier'] = 'Standard'

    # Safe CLV calculation
    user_purchases = df.groupby(
        'user_id')['price'].sum().fillna(0).reset_index()
    df = df.merge(user_purchases, on='user_id',
                  suffixes=('', '_total'), how='left')
    df['customer_lifetime_value'] = df['price_total'].fillna(0)
    df = df.drop('price_total', axis=1)

    return df


def save_metrics(df, project_root):
    """Save data quality metrics."""
    initial_count = len(df)
    metrics_dir = project_root / 'metrics'
    metrics_dir.mkdir(exist_ok=True)

    cleaning_metrics = {
        'initial_records': initial_count,
        'duplicate_emails_removed': initial_count - len(df.drop_duplicates(subset=['email'])),
        'invalid_sessions_removed': len(df[~df.apply(validate_timestamps, axis=1)]),
        'invalid_prices_removed': len(df[df['price'] <= 0]),
        'invalid_status_removed': len(df[~df['purchase_status'].isin(valid_statuses)])
    }

    quality_metrics = {
        'null_percentage': df.isnull().sum().to_dict(),
        'price_stats': df['price'].describe().to_dict(),
        'session_duration_stats': df['session_duration_minutes'].describe().to_dict(),
        'device_type_distribution': df['device_type'].value_counts().to_dict()
    }

    timestamp = dt.now().strftime("%Y%m%d")
    pd.DataFrame([cleaning_metrics]).to_csv(
        metrics_dir / f'cleaning_metrics_{timestamp}.csv')
    pd.DataFrame([quality_metrics]).to_csv(
        metrics_dir / f'quality_metrics_{timestamp}.csv')


def generate_reports(df):
    """Query data from DuckDB and generate reports."""
    try:
        start_time = datetime.now()
        project_root = Path(__file__).parents[1]
        db_dir = project_root / 'dbt_pipeline_demo' / 'databases'
        db_path = db_dir / 'dbt_pipeline_demo.duckdb'

        # Connect to DuckDB
        log.info("Connecting to DuckDB...")
        conn = duckdb.connect(str(db_path))

        # Ensure DataFrame has expected types before creating table
        type_mapping = {
            'user_id': 'string',
            'price': 'float64',
            'session_duration_minutes': 'float64',
            'is_active': 'string',
            'cohort_date': 'string',
            'user_age_days': 'int64',
            'customer_lifetime_value': 'float64'
        }

        for col, dtype in type_mapping.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype, errors='ignore')

        # Create the table with explicit column types
        conn.execute("""
            DROP TABLE IF EXISTS user_activity;
            CREATE TABLE user_activity AS SELECT * FROM df;
        """)
        log.info("user_activity table created and populated")

        # Debug: Check the structure of the data
        log.info("Checking data structure...")
        conn.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'user_activity';
        """).fetchdf()

        # Add indexes for frequently queried columns
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_id ON user_activity(user_id);
            CREATE INDEX IF NOT EXISTS idx_purchase_status ON user_activity(purchase_status);
        """)

        # Queries for insights on user activity
        # 1. Customer Lifecycle Analysis
        lifecycle_analysis = conn.execute("""
            SELECT
                COUNT(*) as total_accounts,
                COUNT(CASE WHEN is_active = 'yes' THEN 1 END) as active_accounts,
                ROUND(TRY_CAST(COUNT(CASE WHEN is_active = 'yes' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(10,2)), 2) as active_percentage,
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
                SUM(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) as churned_users,
                ROUND(TRY_CAST(AVG(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) * 100 AS DECIMAL(10,2)), 2) as churn_rate,
                ROUND(TRY_CAST(AVG(CASE
                    WHEN is_active = 'no' THEN
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
                SUM(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) as churned_users,
                ROUND(TRY_CAST(AVG(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) * 100 AS DECIMAL(10,2)), 2) as churn_rate,
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
                SUM(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) as churned_users,
                ROUND(TRY_CAST(AVG(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) * 100 AS DECIMAL(10,2)), 2) as churn_rate,
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
                SUM(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) as churned_users,
                ROUND(TRY_CAST(AVG(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) * 100 AS DECIMAL(10,2)), 2) as churn_rate,
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
                SUM(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) as churned_users,
                ROUND(TRY_CAST(AVG(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) * 100 AS DECIMAL(10,2)), 2) as churn_rate,
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
            log.info("Saved %s to %s", name, filepath)

        # Add performance metrics
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        log.info("Total query execution time: %s seconds", execution_time)

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


def upload_data(df, s3_bucket="sim-api-data"):
    """Upload cleaned data to S3.

    Args:
        df (pd.DataFrame): Cleaned DataFrame to upload
        s3_bucket (str): Name of the S3 bucket to upload to
    """
    try:
        log.info("Attempting to connect to MinIO...")
        client = minio.Minio("localhost:9000",
                             access_key="admin",
                             secret_key="password123",
                             secure=False)
        log.info("Connected to MinIO")

        # Get project root and create temporary directory for files
        project_root = Path(__file__).parents[1]
        temp_dir = project_root / 'temp'
        temp_dir.mkdir(parents=True, exist_ok=True)

        # Save files temporarily
        json_path = temp_dir / 'cleaned_data.json'
        parquet_path = temp_dir / 'cleaned_data.parquet'

        # Save in different formats
        df.to_json(json_path, orient='records', date_format='iso')
        df.to_parquet(parquet_path, index=False)

        # Ensure bucket exists
        if not client.bucket_exists(s3_bucket):
            client.make_bucket(s3_bucket)
            log.info(f"Created bucket: {s3_bucket}")

        # Upload files to S3
        for file_path, object_name in [
            (json_path, 'cleaned_data.json'),
            (parquet_path, 'cleaned_data.parquet')
        ]:
            client.fput_object(
                s3_bucket,
                object_name,
                str(file_path),
                content_type='application/json' if object_name.endswith(
                    '.json') else 'application/octet-stream'
            )
            log.info(f"Uploaded {object_name} to S3 bucket: {s3_bucket}")

        # Clean up temporary files
        json_path.unlink()
        parquet_path.unlink()
        temp_dir.rmdir()
        log.info("Temporary files cleaned up")

    except minio.error.MinioException as e:
        log.error("MinIO error: %s", str(e))
        raise
    except Exception as e:
        log.error("Error uploading data to S3: %s", str(e))
        raise


def main():
    """Execute the main data pipeline workflow."""
    try:
        # Generate fresh data
        generate_data()

        # Cleans data and prepares it for analysis
        df, _ = prepare_data()

        # Add analysis fields
        df = add_analysis_fields(df)

        # Generate reporting across various modalities
        generate_reports(df)

        # Upload cleaned data to DuckDB & S3
        upload_data(df)

    except (minio.error.MinioException,
            duckdb.Error, IOError, ConnectionError) as e:
        log.error("Pipeline failed: %s", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
