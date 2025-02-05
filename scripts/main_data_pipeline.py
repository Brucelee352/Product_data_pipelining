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
import pandas as pd
import importlib.metadata as metadata
import importlib.resources as resources

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
os.environ['DBT_PROFILES_DIR'] = str(PROJECT_ROOT / '.dbt')
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

def ellipsis(process_name="Loading", num_dots=3, interval=20) -> None:
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


def check_dependencies() -> None:
    """
    Verifies if the required packages are installed.
    Exits the program if any are missing.
    
    This version uses:
      - importlib.metadata to list installed packages, and
      - importlib.resources to load a resource file containing dependency info.
    """
    try:
        # Use importlib.metadata to inspect installed distributions
        import importlib.metadata as metadata
        # Use importlib.resources to load dependency info bundled with your package
        import importlib.resources as resources
    except ImportError as e:
        log.error(f"Required module missing: {e}")
        sys.exit(1)

    # Attempt to load the dependency list from a resource file.
    # (Change 'your_package' to the package where the file resides;
    #  here we assume the file is named "dependencies.txt" and is packaged.)
    try:
        # Handle case where __package__ is None (script run directly)
        package_path = Path(__file__).parent if __package__ is None else resources.files(__package__)
        with (package_path / "dependencies.txt").open() as f:
            required_data = f.read()
        # Each line is assumed to be in the format "package==version" or a comment.
        required = {
            line.split("==")[0].strip() 
            for line in required_data.splitlines() 
            if line.strip() and not line.strip().startswith("#")
        }
    except (FileNotFoundError, TypeError):
        # Fallback to hardcoded required packages if no resource file is found.
        required = {
            'user-agents',
            'duckdb',
            'minio',
            'pandas',
            'faker',
            'dbt-core',
            'dbt-duckdb'
        }

    # Use importlib.metadata to get a set of installed package names (in lower case).
    installed = {
        dist.metadata.get('Name', '').lower()
        for dist in metadata.distributions()
        if dist.metadata.get('Name')
    }

    # Identify missing packages (case-insensitively).
    missing = {pkg for pkg in required if pkg.lower() not in installed}

    if missing:
        log.error(f"Missing packages: {missing}")
        log.info("Please ensure that the virtual environment is created, and then install dependencies:")
        log.info("  venv creation: python -m venv .venv")
        log.info("  venv activation: source .venv/bin/activate  # or .venv\\Scripts\\activate on Windows")
        log.info("  dependencies: pip install -r requirements.txt")
        sys.exit(1)

def generate_data() -> pd.DataFrame:
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
                "product_name": fake.random_element(
                    ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta", "Iota", 
                     "Kappa", "Lambda", "Omicron", "Sigma", "Tau", "Upsilon", "Phi", "Omega"]
                    ),
                "price": round(fake.pyfloat(min_value=100, max_value=5000, right_digits=2), 2),
                "purchase_status": fake.random_element(["completed", "pending", "failed"]),
                "user_agent": fake.user_agent()
            }
            data.append(record)

        return pd.DataFrame(data)
    except Exception as e:
        log.error("Error generating data: %s", str(e))
        raise


def validate_timestamps(row) -> bool:
    """Function validates the timestamps for the login and logout times, to ensure consistency."""
    try:
        login = pd.to_datetime(row['login_time'])
        logout = pd.to_datetime(row['logout_time'])
        return pd.notnull(login) and pd.notnull(logout) and login <= logout
    except Exception as e:
        log.error("Error validating timestamps for row %s: %s", row.name, str(e))
        return False


def process_basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
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


def process_advanced_cleaning(df: pd.DataFrame) -> pd.DataFrame:
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


def add_analysis_fields(df: pd.DataFrame) -> pd.DataFrame:
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


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Function prepares the generated data for cleaning and loading."""
    try:
        df = process_basic_cleaning(df)
        df = process_advanced_cleaning(df)
        df = add_analysis_fields(df)
        # Save metrics and data
        save_metrics(df, PROJECT_ROOT)
        save_data_formats(df, PROJECT_ROOT)
        return df  
    except Exception as e:
        log.error("Error in data cleaning: %s", str(e))
        raise


def save_metrics(df: pd.DataFrame, project_root: Path) -> None:
    """Saves data quality metrics."""
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


def save_data_formats(df: pd.DataFrame, project_root: Path
                      ) -> tuple[Path, Path, Path]:
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
            
def run_dbt_ops() -> None:
    """Runs dbt deps and models for data transformation."""
    try:
        # Store original directory
        original_dir = os.getcwd()

        # Change to dbt project directory
        os.chdir(DBT_ROOT)
        log.info(f"Changed working directory to {DBT_ROOT}")

        # Clear dbt cache
        dbt = dbtRunner()
        dbt.invoke(["clean"])
        
        # Run dbt deps
        deps_result = dbt.invoke(["deps"])
        if not deps_result.success:
            log.error("Failed to run dbt deps")
            raise Exception("Failed to run dbt deps")
        
        # Verify packages.yml exists
        if not (DBT_ROOT / 'packages.yml').exists():
            raise FileNotFoundError("packages.yml not found in dbt project")
        
        # Verify dbt_packages directory exists
        dbt_packages_dir = DBT_ROOT / 'dbt_packages'
        if not dbt_packages_dir.exists():
            raise FileNotFoundError(f"dbt_packages directory not found at {dbt_packages_dir}")

        # Run dbt commands
        result = dbt.invoke([
            "run",
            "--target", "dev",
            "--full-refresh"
        ])

        if not result.success or not deps_result.success:
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


def main() -> None:
    try:
        # 1. Verify virtual environment is active
        if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
            log.error("Virtual environment is not active. Please activate it before running the script.")
            log.info("Activation commands:")
            log.info("  Windows: .venv\\Scripts\\activate")
            log.info("  macOS/Linux: source .venv/bin/activate")
            sys.exit(1)

        log.info("Pipeline initialized at %s", dt.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        # 2. Check dependencies
        ellipsis("Checking dependencies")
        check_dependencies()
        
        # 3. Drop existing database for reproducibility
        ellipsis("Dropping existing database")

        if db_path.exists():
            os.remove(str(db_path))
            log.info(f"Removed existing database at {db_path}")

        # 4. Initialize new database connection
        ellipsis("Initializing new database connection")
        conn = duckdb.connect(str(db_path))
        log.info(f"Created new database at {db_path}")

        # 5. Generate fresh data
        ellipsis("Generating fresh data")
        df = generate_data()

        # 6. Prepare data
        ellipsis("Preparing data")
        df = prepare_data(df)

        # 7. Create source table
        ellipsis("Creating source table")
        make_ua_table(df)

        # 8. Run dbt transformations
        print("Running dbt transformations!")
        run_dbt_ops()

        # 9. Generate reports
        ellipsis("Generating reports")
        generate_reports()

        # 10. Upload data
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
