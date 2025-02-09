"""
Constants for project scripts and configurations.

"""
import os
import sys
import logging
from pathlib import Path
from datetime import datetime as dt
from faker import Faker


# MinIO/S3 Configuration
MINIO_ENDPOINT = 'minio.brucea-lee.com'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = 'code_earth420'
MINIO_BUCKET_NAME = 'sim-api-data'
MINIO_USE_SSL = True
MINIO_URL_STYLE = 'path'

# Path configuration
PROJECT_ROOT = Path(__file__).parents[1]
DBT_ROOT = PROJECT_ROOT / 'dbt_pipeline_demo'
DB_DIR = DBT_ROOT / 'databases'
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / 'dbt_pipeline_demo.duckdb'
REPORTS_DIR = PROJECT_ROOT / 'reports'
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Database Configuration
PRODUCT_SCHEMA = 'main.product_schema'

# Data Generation Settings
DEFAULT_NUM_ROWS = 10000
START_DATETIME = dt.strptime('2021-01-01 10:30', '%Y-%m-%d %H:%M')
END_DATETIME = dt.strptime('2024-12-31 23:59', '%Y-%m-%d %H:%M')
VALID_STATUSES = ['pending', 'completed', 'failed', 'chargeback', 'refunded']

# Logging Configuration
LOG = logging.getLogger(':')
LOG.setLevel(os.getenv('LOG_LEVEL', 'INFO'))
LOG_DIR = PROJECT_ROOT / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=LOG.level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Faker Random Seed Settings
FAKE = Faker()
FAKER_SEED = 42
RANDOM_SEED = 42


def main():
    """
    Main function to print all constants.
    """
    print(f"PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"DB_PATH: {DB_PATH}")
    print(f"REPORTS_DIR: {REPORTS_DIR}")
    print(f"DEFAULT_NUM_ROWS: {DEFAULT_NUM_ROWS}")
    print(f"START_DATETIME: {START_DATETIME}")
    print(f"END_DATETIME: {END_DATETIME}")
    print(f"VALID_STATUSES: {VALID_STATUSES}")
    print(f"LOG_DIR: {LOG_DIR}")
    print(f"LOG_LEVEL: {LOG.level}")
    print(f"FAKE: {FAKE}")
    print(f"FAKER_SEED: {FAKER_SEED}")
    print(f"RANDOM_SEED: {RANDOM_SEED}")
    print(f"MINIO_ENDPOINT: {MINIO_ENDPOINT}")
    print(f"MINIO_ACCESS_KEY: {MINIO_ACCESS_KEY}")
    print(f"MINIO_SECRET_KEY: {MINIO_SECRET_KEY}")
    print(f"MINIO_BUCKET_NAME: {MINIO_BUCKET_NAME}")
    print(f"MINIO_USE_SSL: {MINIO_USE_SSL}")
    print(f"MINIO_URL_STYLE: {MINIO_URL_STYLE}")


if __name__ == "__main__":
    main()
