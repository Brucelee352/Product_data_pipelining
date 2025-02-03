"""
Product Data Pipeline v4.0

This script is designed to generate, clean, and store product data 
for a platform I made up.

Please install dependencies via CLI, before running:

pip install -r requirements.txt

requirements.txt file can be found in the root directory of the 
project on GitHub for download.

"""

# Standard library imports
import logging
import os
import random
import sys
from datetime import datetime as dt
from datetime import timedelta
from pathlib import Path
from typing import Dict, Union
import time
import subprocess
import pandas as pd

# Third-party imports
import duckdb
import minio
from dotenv import load_dotenv
from faker import Faker
from dbt.cli.main import dbtRunner
from user_agents import parse

# Local imports for analytics queries
from analytics_queries import (
    run_lifecycle_analysis,
    run_purchase_analysis,
    run_demographics_analysis,
    run_business_analysis,
    run_engagement_analysis,
    run_churn_analysis,
    run_session_analysis,
    run_funnel_analysis,
    save_analysis_results
)

# Configuration and constants

# Initialize paths and configuration
PROJECT_ROOT = Path(__file__).parents[1]
load_dotenv(dotenv_path=PROJECT_ROOT / 'pdp_config.env')
PRODUCT_SCHEMA = 'main.product_schema'

# Configure logging
log_dir = Path('logs')
log_dir.mkdir(parents=True, exist_ok=True)

# Set logging level
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            os.getenv('LOG_FILE', 'logs/generate_fake_data.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# Constants
DEFAULT_NUM_ROWS = int(os.getenv('DEFAULT_NUM_ROWS', 8000))
START_DATETIME = dt.strptime(
    os.getenv('START_DATETIME', '2022-01-01 10:30'), '%Y-%m-%d %H:%M')
END_DATETIME = dt.strptime(
    os.getenv('END_DATETIME', '2024-12-31 23:59'), '%Y-%m-%d %H:%M')
VALID_STATUSES = os.getenv(
    'VALID_STATUSES', 'pending,completed,failed').split(',')

# Random seed configuration
fake = Faker()
Faker.seed(int(os.getenv('FAKER_SEED', 42)))
random.seed(int(os.getenv('RANDOM_SEED', 42)))

# Database configuration
DBT_ROOT = PROJECT_ROOT / 'dbt_pipeline_demo'
db_dir = DBT_ROOT / 'databases'
db_dir.mkdir(parents=True, exist_ok=True)
db_path = db_dir / 'dbt_pipeline_demo.duckdb'

# S3 Configuration
S3_CONFIG: Dict[str, Union[str, bool]] = {
    'endpoint': os.getenv('MINIO_ENDPOINT'),
    'access_key': os.getenv('MINIO_ACCESS_KEY'),
    'secret_key': os.getenv('MINIO_SECRET_KEY'),
    'bucket': os.getenv('MINIO_BUCKET_NAME'),
    'use_ssl': os.getenv('MINIO_USE_SSL', 'False').lower() in {'true', '1', 'yes'},
}

# Functions


def ellipsis(process_name="Loading", num_dots=3, interval=20):
    """Prints static loading messages with trailing periods."""
    try:
        # Print the process name
        sys.stdout.write(process_name)
        sys.stdout.flush()

        # Prints trailing ellipses with a delay
        for _ in range(num_dots):
            sys.stdout.write(".")
            sys.stdout.flush()
            time.sleep(interval)

        # Move to the next line
        sys.stdout.write("\n")
    except Exception as e:
        log.error(f"Error in ellipsis function: {str(e)}")
        raise


def activate_venv():
    """
        Activates the project's virtual environment.

        Needed for dependencies to function for portability reasons.  

    """
    try:
        # Get the project root directory
        project_root = Path(__file__).parents[1]

        # Temporary print statement
        print(f"Project root path: {project_root}")

        # Path to virtual environment in project directory
        venv_path = project_root / '.venv'

        # Verify the virtual environment exists
        if not venv_path.exists():
            log.error("Virtual environment not found at %s", venv_path)
            log.info("Please create one with: python -m venv .venv")
            sys.exit(1)

        # Activate the virtual environment
        if sys.platform == 'win32':

            activate_script = venv_path / 'Scripts' / 'activate.bat'
            subprocess.run([str(activate_script)], shell=False, check=True)
        else:
            activate_script = venv_path / 'bin' / 'activate'
            subprocess.run(['source', str(activate_script)],
                           shell=False, check=True)

        log.info(
            f"Virtual environment activated successfully from {venv_path}")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        log.error(f"Failed to activate virtual environment: {str(e)}")
        sys.exit(1)


def check_dependencies():
    """
       Verifies if the required packages are installed.

       Exits the program if not.      

    """

    required = {'duckdb', 'minio', 'pandas', 'faker'}
    try:
        import pkg_resources

        installed = {pkg.key for pkg in pkg_resources.working_set}
        missing = required - installed
        if missing:
            log.error(f"Missing packages: {missing}")
            log.info("Please run: pip install -r requirements.txt")
            sys.exit(1)
    except ImportError:
        log.error("Could not verify package installations")
        sys.exit(1)


def generate_data():
    """Generates synthetic data for the pipeline."""

    try:
        data = []
        for _ in range(DEFAULT_NUM_ROWS):
            # Generate user data
            first_name = fake.first_name()
            last_name = fake.last_name()
            is_active = random.random() < 0.8

            # Generate timestamps
            account_created = fake.date_time_between(
                start_date=START_DATETIME, end_date=END_DATETIME)
            account_updated = fake.date_time_between(
                start_date=account_created, end_date=END_DATETIME)
            account_deleted = None if is_active else fake.date_time_between(
                start_date=account_updated, end_date=END_DATETIME)

            login_time = fake.date_time_between(
                start_date=account_created,
                end_date=account_deleted if account_deleted else END_DATETIME
            )
            logout_time = login_time + timedelta(hours=random.uniform(0.5, 4))

            # Create record
            record = {
                "user_id": fake.uuid4(),
                "first_name": first_name,
                "last_name": last_name,
                "email": f"{first_name.lower()}_{last_name.lower()}@{fake.domain_name()}",
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
                "is_active": "yes" if is_active else "no",
                "login_time": login_time.isoformat(),
                "logout_time": logout_time.isoformat(),
                "account_created": account_created.isoformat(),
                "account_updated": account_updated.isoformat(),
                "account_deleted": account_deleted.isoformat() if account_deleted else None,
                "session_duration_minutes": (logout_time - login_time).total_seconds() / 60,
                "product_id": fake.uuid4(),
                "product_name": fake.random_element(["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta", "Iota", "Kappa", "Lambda", "Omicron", "Sigma", "Tau", "Upsilon", "Phi", "Omega"]),
                "price": round(fake.pyfloat(min_value=100, max_value=5000, right_digits=2), 2),
                "purchase_status": fake.random_element(["completed", "pending", "failed"]),
                "user_agent": fake.user_agent()
            }
            data.append(record)

        return pd.DataFrame(data)
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
    drop_columns = ['phone_number', 'city', 'postal_code', 'product_id']
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
    df = df[df['purchase_status'].isin(VALID_STATUSES)]
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


def prepare_data(df):
    """Function prepares the generated data for cleaning and loading."""
    try:
        df = process_basic_cleaning(df)
        df = process_advanced_cleaning(df)
        df = add_analysis_fields(df)
        # Save metrics and data
        save_metrics(df, PROJECT_ROOT)
        save_data_formats(df, PROJECT_ROOT)
        return df  # Return only the DataFrame
    except Exception as e:
        log.error("Error in data cleaning: %s", str(e))
        raise


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
        'invalid_status_removed': len(df[~df['purchase_status'].isin(VALID_STATUSES)])
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


def save_data_formats(df, project_root):
    """Save cleaned data in multiple formats (CSV, JSON, Parquet)
       and saves to the data directory, locally.
    """
    try:
        # Create data directory if it doesn't exist
        data_dir = project_root / 'data'
        data_dir.mkdir(parents=True, exist_ok=True)

        # Define file paths
        csv_path = data_dir / 'cleaned_data.csv'
        json_path = data_dir / 'cleaned_data.json'
        parquet_path = data_dir / 'cleaned_data.parquet'

        # Save data in different formats
        df.to_csv(csv_path, index=False)
        df.to_json(json_path, orient='records', date_format='iso')
        df.to_parquet(parquet_path, index=False)

        # Log success
        log.info(f"Data saved in CSV, JSON, and Parquet formats at {data_dir}")

        # Return paths for further use
        return csv_path, json_path, parquet_path
    except Exception as e:
        log.error(f"Error saving data formats: {str(e)}")
        raise


def make_ua_table(df: pd.DataFrame) -> None:
    """Create the source user_activity table in DuckDB."""
    try:
        conn = duckdb.connect(str(db_path))

        # Drop existing table
        conn.execute("DROP TABLE IF EXISTS user_activity")

        # Create table from DataFrame
        conn.register('temp_df', df)
        conn.execute("""
            CREATE SCHEMA IF NOT EXISTS raw_data;
            CREATE TABLE user_activity AS
            SELECT * FROM temp_df;
        """)

        count = conn.execute(
            "SELECT COUNT(*) FROM user_activity").fetchone()[0]
        log.info(f"Created user_activity table with {count} records")

    except Exception as e:
        log.error("Error creating user_activity table: %s", str(e))
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def run_dbt_models() -> None:
    """Run dbt models to transform the data."""
    try:
        # Store original directory
        original_dir = os.getcwd()

        # Change to dbt project directory
        os.chdir(DBT_ROOT)
        log.info(f"Changed working directory to {DBT_ROOT}")

        # Run dbt commands
        dbt = dbtRunner()
        result = dbt.invoke([
            "run",
            "--target", "dev",
            "--full-refresh"
        ])

        if not result.success:
            log.error("Failed to run dbt models")
            raise Exception("Failed to run dbt models")
        else:
            log.info("Successfully ran dbt models")

        # Change back to original directory
        os.chdir(original_dir)
        log.info(f"Changed back to original directory: {original_dir}")

    except Exception as e:
        log.error("Error running dbt models: %s", str(e))
        raise


def generate_reports() -> None:
    """Generate analytics reports from transformed data."""
    try:
        reports_dir = PROJECT_ROOT / 'reports'
        reports_dir.mkdir(parents=True, exist_ok=True)

        conn = duckdb.connect(str(db_path))

        # Run analytics
        analysis_results = {
            "lifecycle_analysis": run_lifecycle_analysis(conn),
            "purchase_analysis": run_purchase_analysis(conn),
            "demographics_analysis": run_demographics_analysis(conn),
            "business_analysis": run_business_analysis(conn),
            "engagement_analysis": run_engagement_analysis(conn),
            "churn_analysis": run_churn_analysis(conn),
            "session_analysis": run_session_analysis(conn),
            "funnel_analysis": run_funnel_analysis(conn)
        }

        save_analysis_results(analysis_results, reports_dir)

    except Exception as e:
        log.error(f"Error generating reports: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def upload_data() -> str:
    """Upload transformed data to S3."""
    try:
        conn = duckdb.connect(str(db_path))

        # Get final data
        final_df = conn.sql(f"""
            SELECT * FROM {PRODUCT_SCHEMA}
        """).df()

        # Upload to S3
        client = minio.Minio(
            S3_CONFIG['endpoint'],
            access_key=S3_CONFIG['access_key'],
            secret_key=S3_CONFIG['secret_key'],
            secure=S3_CONFIG['use_ssl']
        )

        # Save data locally, temporarily
        final_df.to_json('temp_upload.json', orient='records')
        final_df.to_parquet('temp_upload.parquet', index=False)

        # Upload to S3
        client.fput_object(
            bucket_name=S3_CONFIG['bucket'],
            object_name='cleaned_data.json',
            file_path='temp_upload.json'
        )
        client.fput_object(
            bucket_name=S3_CONFIG['bucket'],
            object_name='cleaned_data.parquet',
            file_path='temp_upload.parquet'
        )

        # Clean up local files
        os.remove('temp_upload.json')
        os.remove('temp_upload.parquet')

        log.info(f"Data uploaded to S3 successfully: {len(final_df)} rows")
        return 'success'

    except Exception as e:
        log.error("Error uploading data: %s", str(e))
        return 'failed'
    finally:
        if 'conn' in locals():
            conn.close()


def main():
    try:
        # Activate virtual environment
        log.info("Pipeline initialized at %s",
                 dt.now().strftime('%Y-%m-%d %H:%M:%S'))
        ellipsis("Activating virtual environment")
        activate_venv()

        # Check dependencies
        ellipsis("Checking dependencies")
        check_dependencies()

        # Drops existing database (for reproducibility)
        ellipsis("Dropping existing database")

        if db_path.exists():
            os.remove(str(db_path))
            log.info(f"Removed existing database at {db_path}")

        # Initialize new database connection
        ellipsis("Initializing new database connection")
        conn = duckdb.connect(str(db_path))
        log.info(f"Created new database at {db_path}")

        # 1. Generate fresh data
        ellipsis("Generating fresh data")
        df = generate_data()

        # 2. Prepare data
        ellipsis("Preparing data")
        df = prepare_data(df)

        # 3. Create source table
        ellipsis("Creating source table")
        make_ua_table(df)

        # 4. Run dbt transformations
        print("Running dbt transformations!")
        run_dbt_models()

        # 5. Generate reports
        ellipsis("Generating reports")
        generate_reports()

        # 6. Upload data
        ellipsis("Uploading data")
        upload_status = upload_data()
        if upload_status == 'success':
            print("Data uploaded to S3 successfully!")
            print("Pipeline completed successfully at %s",
                  dt.now().strftime('%Y-%m-%d %H:%M:%S'))
            log.info("Pipeline completed successfully at %s",
                     dt.now().strftime('%Y-%m-%d %H:%M:%S'))
        else:
            print("Data upload failed!")
            log.error("S3 upload failed!")

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()
