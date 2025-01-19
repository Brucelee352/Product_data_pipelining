import logging
import sys
from pathlib import Path
from datetime import datetime as dt
import duckdb
import pandas as pd
from user_agents import parse
import yaml


log_dir = Path('logs')
log_dir.mkdir(parents=True, exist_ok=True)

log_file_path = log_dir / 'queries_cleaning.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# Load the configuration from the YAML file
config = yaml.safe_load(open('config/pipeline_config.yaml', encoding='utf-8'))

# Use the configuration values
valid_statuses = config['data_cleaning']['valid_statuses']
price_minimum = config['data_cleaning']['price_minimum']


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


def clean_data():
    """Function cleans the generated data and prepares it for loading."""
    try:
        # Get the project root directory
        project_root = Path(__file__).parents[1]

        # Get the project root directory (2 levels up from this script)
        data_dir = project_root / 'data'
        db_dir = project_root / 'dbt_pipeline_demo' / 'databases'

        # Create the db directoriy if it doesn't exist
        db_dir.mkdir(parents=True, exist_ok=True)

        db_path = db_dir / 'dbt_pipeline_demo.duckdb'

        # Load the CSV file into a DataFrame for cleanup using relative path
        df = pd.read_csv(data_dir / 'simulated_api_data.csv')

        # Generate transaction ID using user_id and login_time
        df['transact_id'] = df.apply(
            lambda row: f"txn_{row['user_id']}_{pd.to_datetime(
                row['login_time']).strftime('%Y%m%d%H%M%S')}"
            if pd.notnull(row['login_time'])
            else None,
            axis=1
        )

        # Data cleaning:

        # Check for null values
        log.info(df.isnull().sum())

        # Apply strip only to string columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip()

        # Remove duplicate email addresses
        df = df.drop_duplicates(subset=['email'])

        # Check for null values again
        log.info(df.isnull().sum())

        # Remove columns that are not relevant to the analysis
        for col in df:
            if col in ['phone_number', 'email_address', 'city', 'postal_code', 'country', 'product_id']:
                df = df.drop(col, axis=1)

        # Add a new column for country, defaulted to 'United States'
        df['country'] = 'United States'

        # Correct is_active column:
        df['is_active'] = df['is_active'].replace({0: 'no', 1: 'yes'})

        # Select and reorder columns to keep consistency with the database schema and dbt models
        df = df[['user_id', 'transact_id', 'first_name', 'last_name', 'email',
                'date_of_birth', 'address', 'state', 'country',
                 'company', 'job_title', 'ip_address', 'is_active', 'login_time',
                 'logout_time', 'account_created', 'account_updated', 'account_deleted',
                 'session_duration_minutes', 'product_name', 'price', 'purchase_status',
                 'user_agent']]

        # Parse user agent to extract device type, os, and browser
        df[['device_type', 'os', 'browser']] = df['user_agent'].apply(lambda ua: pd.Series({
            'device_type': parse(ua).device.family,
            'os': parse(ua).os.family,
            'browser': parse(ua).browser.family
        }))

        # Replace 'Other' with 'Desktop' in the device_type column for less ambiguity in data
        df['device_type'] = df['device_type'].str.replace('Other', 'Desktop')

        # Add data quality checks for KPI-critical fields

        try:
            # Validate session durations and timestamps
            df['is_valid_session'] = df.apply(validate_timestamps, axis=1)
            df = df[df['is_valid_session']].drop('is_valid_session', axis=1)

            # Ensure price is numeric and positive
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df = df[df['price'] > 0]

            # Standardize purchase status values
            df['purchase_status'] = df['purchase_status'].str.lower()
            valid_statuses = ['pending', 'completed', 'failed']
            df = df[df['purchase_status'].isin(valid_statuses)]

            # Add cohort analysis fields
            df['cohort_date'] = pd.to_datetime(
                df['account_created']).dt.to_period('M')
            df['user_age_days'] = (pd.to_datetime(df['login_time']) -
                                   pd.to_datetime(df['account_created'])).dt.days

            # Add engagement level categorization
            df['engagement_level'] = pd.cut(
                df['session_duration_minutes'],
                bins=[0, 30, 60, 120, float('inf')],
                labels=['Very Low', 'Low', 'Medium', 'High']
            )

            # Add price tier categorization
            df['price_tier'] = pd.qcut(
                df['price'],
                q=4,
                labels=['Budget', 'Standard', 'Premium', 'Luxury']
            )

            # Calculate customer lifetime value (CLV)
            user_purchases = df.groupby('user_id')['price'].sum().reset_index()
            df = df.merge(user_purchases, on='user_id',
                          suffixes=('', '_total'))
            df.rename(
                columns={'price_total': 'customer_lifetime_value'}, inplace=True)

            # Select and reorder columns
            df = df[[
                'user_id', 'transact_id', 'first_name', 'last_name', 'email',
                'date_of_birth', 'address', 'state', 'country',
                'company', 'job_title', 'ip_address', 'is_active', 'login_time',
                'logout_time', 'account_created', 'account_updated', 'account_deleted',
                'session_duration_minutes', 'engagement_level', 'product_name',
                'price', 'price_tier', 'purchase_status', 'customer_lifetime_value',
                'cohort_date', 'user_age_days', 'user_agent', 'device_type',
                'os', 'browser'
            ]]

            # Add data quality metrics logging
            initial_count = len(df)

            # Track cleaning impact
            cleaning_metrics = {
                'initial_records': initial_count,
                'duplicate_emails_removed': initial_count - len(df.drop_duplicates(subset=['email'])),
                'invalid_sessions_removed': len(df[~df.apply(validate_timestamps, axis=1)]),
                'invalid_prices_removed': len(df[df['price'] <= 0]),
                'invalid_status_removed': len(df[~df['purchase_status'].isin(valid_statuses)])
            }

            # Add data quality checks
            quality_metrics = {
                'null_percentage': df.isnull().sum().to_dict(),
                'price_stats': df['price'].describe().to_dict(),
                'session_duration_stats': df['session_duration_minutes'].describe().to_dict(),
                'device_type_distribution': df['device_type'].value_counts().to_dict()
            }

            # Save metrics
            metrics_dir = project_root / 'metrics'
            metrics_dir.mkdir(exist_ok=True)

            pd.DataFrame([cleaning_metrics]).to_csv(
                metrics_dir /
                f'cleaning_metrics_{dt.now().strftime("%Y%m%d")}.csv'
            )
            pd.DataFrame([quality_metrics]).to_csv(
                metrics_dir /
                f'quality_metrics_{dt.now().strftime("%Y%m%d")}.csv'
            )

            log.info("Cleaning metrics saved to metrics directory")

            # Save cleaned data in multiple formats
            file_paths = save_data_formats(df, project_root)

            return df, file_paths

        except Exception as e:
            log.error("Error in data cleaning: %s", str(e))
            raise

    except Exception as e:
        log.error("Error in data cleaning: %s", str(e))
        raise


def connect_to_duckdb(db_path):
    """Create a connection to DuckDB and return the connection object.

    Args:
        db_path (Path): Path to the DuckDB database file

    Returns:
        duckdb.DuckDBPyConnection: Connection to DuckDB database

    Raises:
        duckdb.Error: If connection fails
    """
    try:
        conn = duckdb.connect(str(db_path))
        log.info("Connected to DuckDB at %s", db_path)
        return conn
    except duckdb.Error as e:
        log.error("Error connecting to DuckDB at %s: %s", db_path, str(e))
        raise


def upload_data(df, db_path):
    """Load cleaned data into DuckDB and S3.

    Args:
        df (pd.DataFrame): Cleaned DataFrame to load
        db_path (Path): Path to the DuckDB database file
    """
    conn = None
    try:
        conn = connect_to_duckdb(db_path)

        # Save cleaned data to CSV first
        data_dir = Path(db_path).parents[1] / 'data'
        csv_path = data_dir / 'cleaned_data.csv'
        df.to_csv(csv_path, index=False)

        # Load into DuckDB
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS user_activity AS
            SELECT * FROM read_csv_auto('{csv_path}');
        """)
        log.info("Data successfully loaded into DuckDB at %s", db_path)

    except duckdb.Error as e:
        log.error("Error loading data into DuckDB at %s: %s", db_path, str(e))
        raise
    finally:
        if conn:
            conn.close()
            log.info("Database connection closed")


if __name__ == "__main__":
    clean_data()
