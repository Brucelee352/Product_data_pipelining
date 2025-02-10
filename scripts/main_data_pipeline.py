"""
#----------------------------------------------------------------#

Product Data Pipeline v4.0

This script is designed to generate, clean, and store product
data for a made up platform that emulates a real-world,
browser-based SaaS platform.

Please refer to README.md file for more information.

#----------------------------------------------------------------#

Install the needed dependencies via this command:

pip install -e . 

A requirements.txt file can be found in the root directory of the 
project on the GitHub repo for download. 

That is to be used for streamlit, and is optional for this script. 

This too can be installed with via cli.

pip install -r requirements.txt

#----------------------------------------------------------------#
"""

# Standard library imports
import os
import sys
from pathlib import Path
import random
from datetime import datetime as dt
from datetime import timedelta
from typing import Dict, Union
import importlib.metadata as metadata
import importlib.resources as resources
import time

# Third-party imports
import minio
import duckdb
from dotenv import load_dotenv
from dbt.cli.main import dbtRunner
from user_agents import parse
import pandas as pd
from faker import Faker

# Local imports for analytics queries
from scripts.analytics_queries import (
    run_lifecycle_analysis, run_purchase_analysis, run_demographics_analysis,
    run_business_analysis, run_engagement_analysis, run_churn_analysis,
    save_analysis_results
)

# Local imports for constants
from scripts.constants import (
    PROJECT_ROOT, FAKE, FAKER_SEED, START_DATETIME, END_DATETIME,
    PRODUCT_SCHEMA, DEFAULT_NUM_ROWS, MINIO_ENDPOINT, MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD, MINIO_BUCKET_NAME, MINIO_USE_SSL, LOG_DIR, DB_PATH,
    VALID_STATUSES, LOG, DBT_ROOT
)

# Initialize paths and configuration
os.environ['DBT_PROFILES_DIR'] = str(PROJECT_ROOT / '.dbt')
load_dotenv(dotenv_path=PROJECT_ROOT / 'pdp_config.env')

# Set logging level
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Random seed configuration
Faker.seed(int(FAKER_SEED))
random.seed(int(FAKER_SEED))


class PipelineState:
    """
    Class to manage the pipeline state.
    """

    def __init__(self):
        """
        Initialize the pipeline state.
        """
        self.cached_data = None

    def reset_state(self):
        """Reset the pipeline state."""
        self.cached_data = None


# Create a global instance (optional)
pipeline_state = PipelineState()


# Functions

def minio_client():
    """
    Initializes the Minio client.
    """
    try:
        client = minio.Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ROOT_USER,
            secret_key=MINIO_ROOT_PASSWORD,
            secure=MINIO_USE_SSL
        )
        LOG.info("Connected to %s", MINIO_ENDPOINT)
        return client
    except Exception as e:
        LOG.error("MinIO connection error: %s", str(e))
        raise


def ellipsis(process_name="Loading", num_dots=3, interval=0.5) -> None:
    """
    Prints static loading messages with trailing periods.
    Unnecessary, but for flare, why not? I like it.

    Args:
        process_name(str): The name of the process to display.
        num_dots(int): The number of dots to print.
        interval(int): The interval between dots in seconds.
    """
    try:
        # Print out the process name
        sys.stdout.write(process_name)
        sys.stdout.flush()

        # Prints out trailing ellipses with a delay
        for _ in range(num_dots):
            sys.stdout.write(".")
            sys.stdout.flush()
            time.sleep(interval)

        # Move to the next line
        sys.stdout.write("\n")
    except Exception as e:
        LOG.error("Error in ellipsis function: %s", str(e))
        raise


def check_dependencies() -> None:
    """
    Verifies if the required packages are installed.
    Exits the program if any are missing.

    This version uses:
      - importlib.metadata to list installed packages, and
      - importlib.resources to load a file containing dependency info.
 """

    try:
        # Handle case where __package__ is None (script run directly)
        package_path = Path(__file__).parent
        if __package__ is None:
            package_path = Path(__file__).parent
        else:
            package_path = Path(__file__).parent / resources.files(__package__)
        with (package_path / "dependencies.txt").open() as f:
            required_data = f.read()

        # Each line is assumed to be "package==version" or a comment.
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

    # Use importlib.metadata to get a set of installed package names.
    installed = {
        dist.metadata.get('Name', '').lower()
        for dist in metadata.distributions()
        if dist.metadata.get('Name')
    }

    # Identify missing packages (case-insensitively).
    missing = {pkg for pkg in required if pkg.lower() not in installed}

    if missing:
        LOG.error("Missing packages: %s", missing)
        LOG.info(
            "Please ensure that the virtual environment is created, "
            "and then install dependencies:"
        )
        LOG.info("  venv creation: python -m venv .venv")
        LOG.info(
            "  venv activation: source .venv/bin/activate  # or "
            ".venv\\Scripts\\activate on Windows")
        LOG.info("  dependencies: pip install -r requirements.txt")
        sys.exit(1)


def generate_data() -> pd.DataFrame:
    """
    Generates synthetic data for the pipeline.

    Args:
        DEFAULT_NUM_ROWS (int): The number of rows to generate,
        can be set in .env file, as well as START_DATETIME and END_DATETIME.
    """

    try:
        data = []
        for _ in range(DEFAULT_NUM_ROWS):
            # Generate user data
            first_name = FAKE.first_name()
            last_name = FAKE.last_name()
            base_price = round(FAKE.pyfloat(
                min_value=100,
                max_value=5000,
                right_digits=2), 2)
            price = (random.choice([base_price * 10, base_price / 10])
                     if random.random() < 0.01 else base_price)
            product_name = (random.choice(["Alpha", "Beta",
                                           "Gamma", "Delta",
                                           "Omicron", "Phi",
                                           "Epsilon", "Zeta",
                                           "Omega"])
                            if random.random() > 0.009 else
                            random.choice(["Legacy Product",
                                           "Beta Product"]))
            purchase_status = (
                random.choice(["completed", "pending", "failed"]
                              if random.random() > 0.005 else
                              random.choice(["refunded", "chargeback"]))
            )
            is_active = random.random() < 0.8
            # Generate timestamps
            account_created = FAKE.date_time_between(
                start_date=START_DATETIME, end_date=END_DATETIME)
            account_updated = FAKE.date_time_between(
                start_date=account_created, end_date=END_DATETIME)
            account_deleted = (
                None if is_active else FAKE.date_time_between(
                    start_date=account_updated, end_date=END_DATETIME
                )
            )
            login_time = FAKE.date_time_between(
                start_date=account_created,
                end_date=account_deleted
                if account_deleted else END_DATETIME
            )
            logout_time = (
                login_time + timedelta(hours=random.uniform(0.5, 4))
            )
            # Creates user records for each row
            record = {
                "user_id": FAKE.uuid4(),
                "first_name": first_name,
                "last_name": last_name,
                "email": (
                    f"{first_name.lower()}_{
                        last_name.lower()
                    }@{FAKE.domain_name()}"
                ),
                "date_of_birth": FAKE.date_of_birth(
                    minimum_age=18, maximum_age=72
                ).isoformat(),
                "phone_number": FAKE.phone_number(),
                "address": FAKE.address(),
                "city": FAKE.city(),
                "state": FAKE.state(),
                "postal_code": FAKE.postcode(),
                "country": FAKE.country(),
                "company": FAKE.company(),
                "job_title": FAKE.job(),
                "ip_address": FAKE.ipv4(),
                "is_active": "yes" if is_active else "no",
                "login_time": login_time.isoformat(),
                "logout_time": logout_time.isoformat(),
                "account_created": account_created.isoformat(),
                "account_updated": account_updated.isoformat(),
                "account_deleted": (
                    account_deleted.isoformat()
                    if account_deleted else None
                ),
                "session_duration_minutes": (
                    max(1, min(120, random.gauss(30, 10)))
                    if random.random() < 0.95 else
                    random.uniform(0.5, 5)
                ),
                "product_id": FAKE.uuid4(),
                "product_name": product_name,
                "price": (price
                          if purchase_status != "refunded" or
                          purchase_status != "chargeback"
                          else price * 0.25
                          ),
                "purchase_status": purchase_status,
                "user_agent": FAKE.user_agent()
            }
            data.append(record)

        return pd.DataFrame(data)
    except Exception as e:
        LOG.error("Error generating data: %s", str(e))
        raise


def validate_timestamps(row) -> bool:
    """
    Function validates the timestamps for the login
    and logout times, to ensure consistency.

    Args:
        row (pd.Series): The row to validate.
    """
    try:
        login = pd.to_datetime(row['login_time'])
        logout = pd.to_datetime(row['logout_time'])
        return (pd.notnull(login) and pd.notnull(logout)
                and login <= logout)
    except (ValueError, FileNotFoundError, ImportError) as e:
        LOG.error(
            "Error validating timestamps for row %s: %s",
            row.name, str(e)
        )
        return False


def basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle basic data cleaning operations.

    Args:
        df (pd.DataFrame): The dataframe to process.
    """
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

    return df[['user_id', 'transact_id', 'first_name', 'last_name',
               'email', 'date_of_birth', 'address', 'state', 'country',
               'company', 'job_title', 'ip_address', 'is_active',
               'login_time', 'logout_time', 'account_created',
               'account_updated', 'account_deleted',
               'session_duration_minutes', 'product_name', 'price',
               'purchase_status', 'user_agent']]


def advanced_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle advanced data processing and validation.

    Parses user_agents(str) into the following fields:
        - device_type(str)
        - os(str)
        - browser(str)
    """
    # Process user agent data
    df[['device_type', 'os', 'browser']] = df['user_agent'].apply(
        lambda ua: pd.Series(
            {
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
    """
    Add additional analysis fields to the dataframe.

    Produces an additive dataframe with the following fields:
        - cohort_date
        - user_age_days
        - engagement_level
        - price_tier
        - customer_lifetime_value

    Needed to make the data more useful for analysis.
    """

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
    """
    Function prepares the generated data for cleaning and loading,
    Then saves the data in multiple formats, and metrics.

    It runs the following functions:
        - basic_cleaning
        - advanced_cleaning
        - add_analysis_fields

    It saves the following metrics:
        - cleaning_metrics
        - quality_metrics
    """

    try:
        df = basic_cleaning(df)
        df = advanced_cleaning(df)
        df = add_analysis_fields(df)
        # Save metrics and data
        save_metrics(df, PROJECT_ROOT)
        save_data_formats(df, PROJECT_ROOT)
        return df
    except Exception as e:
        LOG.error("Error in data cleaning: %s", str(e))
        raise


def save_metrics(df: pd.DataFrame, project_root: Path) -> None:
    """
    Saves data quality metrics, locally for debugging purposes.

    Args:
        df (pd.DataFrame): The dataframe to save.
        project_root (Path): The root directory of the project.
    """
    initial_count = len(df)
    metrics_dir = project_root / 'metrics'
    metrics_dir.mkdir(exist_ok=True)

    cleaning_metrics = {
        'initial_records': initial_count,
        'duplicate_emails_removed': (
            initial_count - len(df.drop_duplicates(subset=['email']))
        ),
        'invalid_sessions_removed': (
            len(df[~df.apply(validate_timestamps, axis=1)])
        ),
        'invalid_prices_removed': (
            len(df[df['price'] <= 0])
        ),
        'invalid_status_removed': (
            len(df[~df['purchase_status'].isin(VALID_STATUSES)])
        )
    }
    quality_metrics = {
        'null_percentage': df.isnull().sum().to_dict(),
        'price_stats': df['price'].describe().to_dict(),
        'session_duration_stats': (
            df['session_duration_minutes'].describe().to_dict()
        ),
        'device_type_distribution': (
            df['device_type'].value_counts().to_dict()
        )
    }
    timestamp = dt.now().strftime("%Y%m%d")
    pd.DataFrame([cleaning_metrics]).to_csv(
        metrics_dir / f'cleaning_metrics_{timestamp}.csv')
    pd.DataFrame([quality_metrics]).to_csv(
        metrics_dir / f'quality_metrics_{timestamp}.csv')


def save_data_formats(df: pd.DataFrame, project_root: Path
                      ) -> tuple[Path, Path, Path]:
    """
    Save cleaned data in multiple formats (CSV, JSON, Parquet)
    and saves to the data directory, locally.

    Args:
        df (pd.DataFrame): The dataframe to save.
        project_root (Path): The root directory of the project.
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
        LOG.info("Data saved in CSV, JSON, "
                 "and Parquet formats at %s", data_dir)

        # Return paths for further use
        return csv_path, json_path, parquet_path
    except Exception as e:
        LOG.error("Error saving data formats: %s", str(e))
        raise


def make_ua_table(df: pd.DataFrame, con: duckdb.DuckDBPyConnection) -> None:
    """Create the source user_activity table in DuckDB."""
    try:
        # Drop existing table
        con.execute("DROP TABLE IF EXISTS user_activity")

        # Create table from DataFrame
        con.register('temp_df', df)
        con.execute("""
            CREATE SCHEMA IF NOT EXISTS raw_data;
            CREATE TABLE user_activity AS
            SELECT * FROM temp_df;
        """)

        count = con.execute(
            "SELECT COUNT(*) FROM user_activity").fetchone()[0]
        LOG.info("Created user_activity table with %s records", count)

    except Exception as e:
        LOG.error("Error creating user_activity table: %s", str(e))
        raise
    finally:
        if 'con' in locals():
            con.close()


def run_dbt_ops() -> None:
    """
    Runs dbt deps and build model tables for data transformation.

    Executes the following commands:
        - dbt deps
        - dbt run --target dev --full-refresh
    """
    try:
        # Store original directory
        original_dir = os.getcwd()

        # Change to dbt project directory
        os.chdir(DBT_ROOT)
        LOG.info("Changed working directory to %s", DBT_ROOT)

        # Clear dbt cache
        dbt = dbtRunner()
        dbt.invoke(["clean"])

        # Run dbt deps
        deps_result = dbt.invoke(["deps"])
        if not deps_result.success:
            LOG.error("Failed to run dbt deps")
            raise RuntimeError("Failed to run dbt deps")

        # Verify packages.yml exists
        if not (DBT_ROOT / 'packages.yml').exists():
            raise FileNotFoundError("packages.yml not "
                                    "found in dbt project")

        # Verify dbt_packages directory exists
        dbt_packages_dir = DBT_ROOT / 'dbt_packages'
        if not dbt_packages_dir.exists():
            raise FileNotFoundError(
                f"dbt_packages directory not found at {dbt_packages_dir}")

        # Run dbt commands
        result = dbt.invoke([
            "run",
            "--target", "dev",
            "--full-refresh"
        ])

        if not result.success or not deps_result.success:
            LOG.error("Failed to run dbt models")
            raise RuntimeError("Failed to run dbt models")
        else:
            LOG.info("Successfully ran dbt models")

        # Change back to original directory
        os.chdir(original_dir)
        LOG.info("Changed back to original directory: %s",
                 original_dir)

    except Exception as e:
        LOG.error("Error running dbt models: %s", str(e))
        raise


def generate_reports() -> None:
    """
    Generate analytics reports from transformed data.

    Runs all of the imported analytics queries from the
    analytics_queries.py file.

    Saves the reports to the reports directory.
    """
    try:
        reports_dir = PROJECT_ROOT / 'reports'
        reports_dir.mkdir(parents=True, exist_ok=True)

        con = duckdb.connect(str(DB_PATH))

        # Run analytics
        analysis_results = {
            "lifecycle_analysis": run_lifecycle_analysis(con),
            "purchase_analysis": run_purchase_analysis(con),
            "demographics_analysis": run_demographics_analysis(con),
            "business_analysis": run_business_analysis(con),
            "engagement_analysis": run_engagement_analysis(con),
            "churn_analysis": run_churn_analysis(con),
        }

        save_analysis_results(analysis_results, reports_dir)

    except Exception as e:
        LOG.error("Error generating reports: %s", str(e))
        raise


def upload_data() -> str:
    """
    Uploads transformed data to MinIO.

    Uses DuckDB to get the final data, and then uploads it to S3.

    Returns:
        str: 'success' if the upload is successful, 'failed' otherwise.
    """
    try:
        con = duckdb.connect(str(DB_PATH))
        # Get final data
        final_df = con.sql(f"""
            SELECT * FROM {PRODUCT_SCHEMA}
        """).df()

        # Upload to MinIO
        client = minio_client()

        # Save data locally, temporarily
        final_df.to_json('temp_upload.json', orient='records')
        final_df.to_parquet('temp_upload.parquet', index=False)

        # Upload to S3
        client.fput_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name='cleaned_data.json',
            file_path='temp_upload.json'
        )
        client.fput_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name='cleaned_data.parquet',
            file_path='temp_upload.parquet'
        )

        # Clean up local files
        os.remove('temp_upload.json')
        os.remove('temp_upload.parquet')

        LOG.info("Data uploaded to S3 successfully: "
                 "%s rows", len(final_df))
        return 'success'

    except (minio.error.S3Error, IOError, ValueError) as e:
        LOG.error("Error uploading data: %s", str(e))
        return 'failed'
    finally:
        if 'con' in locals():
            con.close()


def main() -> None:
    """
    Main function that orchestrates the data pipeline.
    """
    try:
        pipeline_state.reset_state()

        # 1. Verify virtual environment is active
        is_venv = hasattr(sys, 'real_prefix')
        is_venv_modern = (hasattr(sys, 'base_prefix')
                          and sys.base_prefix != sys.prefix)
        if not (is_venv or is_venv_modern):
            LOG.error(
                "Virtual environment is not active. "
                "Please activate it before running the script."
            )
            LOG.info("Activation commands:")
            LOG.info("  Windows: .venv\\Scripts\\activate")
            LOG.info("  macOS/Linux: source .venv/bin/activate")
            sys.exit(1)

        LOG.info("Pipeline initialized at %s",
                 dt.now().strftime('%Y-%m-%d %H:%M:%S'))

        # 2. Check dependencies
        ellipsis("Checking dependencies")
        check_dependencies()

        # 3. Drop existing database for reproducibility
        ellipsis("Dropping existing database")
        if DB_PATH.exists():
            os.remove(str(DB_PATH))
            LOG.info("Removed existing database at %s", DB_PATH)

        # 4. Initialize new database connection
        ellipsis("Initializing new database connection")
        con = duckdb.connect(str(DB_PATH))  # Recreate the connection
        LOG.info("Created new database at %s", DB_PATH)

        # 5. Generate fresh data
        ellipsis("Generating fresh data")
        df = generate_data()

        # 6. Prepare data
        ellipsis("Preparing data")
        df = prepare_data(df)

        # 7. Create source table
        ellipsis("Creating source table")
        make_ua_table(df, con)

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
            timestamp = dt.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"Pipeline completed successfully at {timestamp}")
            LOG.info("Pipeline completed successfully at %s", timestamp)
        else:
            LOG.error("Upload process failed!")

    except (RuntimeError,
            IOError,
            ValueError,
            duckdb.Error,
            minio.error.S3Error) as e:
        LOG.error("Pipeline failed: %s", str(e))
        sys.exit(1)
    finally:
        if 'con' in locals():
            con.close()


if __name__ == "__main__":
    main()
