import duckdb
import pandas as pd
import logging
import sys
from pathlib import Path
from user_agents import parse

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


def clean_data():
    """Function cleans the generated data and loads it into the demo database."""

    # Get the project root directory (2 levels up from this script)
    project_root = Path(__file__).parents[1]
    data_dir = project_root / 'data'
    db_dir = project_root / 'dbt_pipeline_demo' / 'databases'

    # Create the db directoriy if it doesn't exist
    db_dir.mkdir(parents=True, exist_ok=True)

    db_path = db_dir / 'dbt_pipeline_demo.duckdb'

    # Load the CSV file into a DataFrame for cleanup using relative path
    df = pd.read_csv(data_dir / 'simulated_api_data.csv')

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

    # Select and reorder columns to keep consistency with the database schema and dbt models
    df = df[['user_id', 'first_name', 'last_name', 'email',
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

    # Save cleaned data using relative paths
    df.to_csv(data_dir / 'cleaned_data.csv', index=False)
    df.to_parquet(data_dir / 'cleaned_data.parquet')

    try:
        # Connect to DuckDB using the defined path
        conn = duckdb.connect(str(db_path))

        # Load the cleaned CSV into DuckDB
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS user_activity AS
            SELECT * FROM read_csv_auto('{data_dir / 'cleaned_data.csv'}');
        """)
        log.info("Data successfully loaded into DuckDB at %s", db_path)
    except duckdb.Error as e:
        log.error("Error loading data into DuckDB at %s: %s", db_path, str(e))
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            log.info("Database connection closed")


if __name__ == "__main__":
    clean_data()
