# Product Data Pipeline: 

## Introduction
This project is an end-to-end data pipeline designed to generate, process, and analyze product data in a cloud environment. It demonstrates modern data engineering practices including:

1. Data generation and cleaning using Python
2. Data storage and transformation using DuckDB and dbt
3. Cloud storage integration with MinIO/S3
4. Interactive analytics dashboard with Streamlit

## Key Features
- **Cloud-Native Architecture**: Designed for deployment in cloud environments
- **Reproducible Data Pipeline**: Generates consistent synthetic data
- **Modern Data Stack**: Combines DuckDB, dbt, and MinIO
- **Interactive Analytics**: Streamlit-powered dashboard
- **CI/CD Ready**: Includes proper package management and configuration

## Prerequisites
### For Cloud usage: 
- Python 3.9+
- DuckDB
- GCP, AWS, or Azure account
- dbt Core
- Streamlit

### Local Development
- Docker (for local MinIO instance)
- Python 3.9+
- DuckDB
- dbt Core (for local development)
- streamlit (for local development)

## Project Structure
```
├── .dbt/                           # dbt profiles directory
│   └── profiles.yml                # dbt connection profiles
├── data/                           # Generated data files
├── dbt_pipeline_demo/              # Main dbt project
│   ├── models/                     # SQL models
│   ├── databases/                  # DuckDB databases 
│   ├── dbt_project.yml             # dbt project configuration
│   └── packages.yml                # dbt package dependencies
├── logs/                           # Pipeline logs
├── metrics/                        # Data quality reports
├── portfolio_app/                  # Streamlit application
│   ├── reports/                    # Reporting .csvs
│   ├── scripts/                    # Python scripts
│     ├── main_data_pipeline.py     # Main pipeline
│     ├── constants.py              # Configuration constants
│     └── analytics_queries.py      # SQL analytics queries
│   ├── app.py                      # Streamlit application
│   └── __init.py__                 # Package initialization
├── minio.tar.gz                    # Docker container for local MinIO
├── pyproject.toml                  # Project configuration
├── requirements.txt                # Streamlit dependencies
├── setup.py                        # Package setup
├── streamlit_app.py                # Streamlit entry point
└── README.md                       # This file
```

## Setup Instructions

### Cloud Environment Setup
1. Clone Repository
```bash
git clone https://github.com/brucelee352/Product_data_pipelining.git
cd Product_data_pipelining
```

2. Configure Environment Variables
Edit `portfolio_app/scripts/constants.py` with your cloud MinIO/S3 credentials:
```python
MINIO_ENDPOINT = 'your-cloud-endpoint'
MINIO_ROOT_USER = 'your-access-key'
MINIO_ROOT_PASSWORD = 'your-secret-key'
MINIO_BUCKET_NAME = 'your-bucket-name'
MINIO_USE_SSL = True
```

3. Install Dependencies
```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -e .
```

4. Run Pipeline
```bash
python portfolio_app/scripts/main_data_pipeline.py
```

### Local Development Setup
1. Start Local MinIO
```bash
docker-compose up -d
```

2. Configure Local Constants
Edit `portfolio_app/scripts/constants.py`:
```python
MINIO_ENDPOINT = 'localhost:9000'
MINIO_ROOT_USER = 'minioadmin'
MINIO_ROOT_PASSWORD = 'minioadmin'
MINIO_BUCKET_NAME = 'local-bucket'
MINIO_USE_SSL = False
```

3. Run Pipeline
```bash
python portfolio_app/scripts/main_data_pipeline.py
```

## Pipeline Workflow
1. **Data Generation**: Creates synthetic product data
2. **Data Cleaning**: Validates and transforms raw data
3. **Database Loading**: Stores data in DuckDB
4. **dbt Transformations**: Runs dbt models
5. **Analytics**: Generates business reports
6. **Cloud Storage**: Uploads processed data to S3
7. **Visualization**: Serves analytics via Streamlit

## Key Components

### Data Generation
- Generates realistic product data using Faker
- Includes user activity, purchases, and session data
- Configurable data volume and time range

### Data Transformation
- Uses dbt for SQL-based transformations
- Includes staging, fact, and dimension models
- Implements data quality checks

### Analytics
- Lifecycle analysis
- Purchase patterns
- User demographics
- Business metrics
- Engagement trends
- Churn analysis

### Cloud Integration
- MinIO/S3 for data storage
- Streamlit Cloud for deployment
- Environment-specific configuration

## Maintenance

### Updating dbt Packages
```bash
cd dbt_pipeline_demo
dbt deps
cd ..
```

### Running Tests
```bash
pytest tests/
```

### CI/CD Integration
The project is configured for CI/CD with:
- Proper package management
- Environment variable handling
- Logging and error tracking

## Environment Variables
| Variable              | Description                          | Default Value          |
|-----------------------|--------------------------------------|------------------------|
| `MINIO_ENDPOINT`      | MinIO server endpoint                | -                      |
| `MINIO_ROOT_USER`     | MinIO access key                     | -                      |
| `MINIO_ROOT_PASSWORD` | MinIO secret key                     | -                      |
| `MINIO_BUCKET_NAME`   | MinIO bucket name                    | -                      |
| `MINIO_USE_SSL`       | Use SSL for MinIO connection         | `False`                |
| `DEFAULT_NUM_ROWS`    | Number of rows to generate           | `10000`                |
| `START_DATETIME`      | Start date for generated data        | `2022-01-01 10:30`     |
| `END_DATETIME`        | End date for generated data          | `2024-12-31 23:59`     |
| `VALID_STATUSES`      | Valid purchase statuses              | `pending,completed,failed,chargeback,refunded`|
| `LOG_LEVEL`           | Logging level                        | `INFO`                 |

## Data Model
### Source Tables
- `user_activity`: Raw user activity data

### dbt Models
- Staging:
  - `stg_product_schema`
  - `stg_user_activity`
- Fact:
  - `fact_user_activity`
- Dimensions:
  - `dim_user`
  - `dim_platform`
  - `dim_product`

## Contributing
1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## License
MIT License
