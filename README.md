# Product Data Pipeline: 

## Overview
This project is a complete data pipeline that 
1. Generates synthetic product data
2. Cleans and transforms the data using Python
3. Models the data using dbt
4. Generates analytics reports
5. Uploads the final data to S3

## Prerequisites
- Python 3.9+
- DuckDB
- MinIO (or S3-compatible storage)
- dbt Core

## Project Structure
```
.
├── dbt_pipeline_demo/          # dbt project
│   ├── models/                 # dbt models
│   ├── databases/              # DuckDB database files
│   ├── dbt_project.yml         # dbt project configuration
│   └── packages.yml            # dbt package dependencies
├── scripts/                    # Pipeline scripts
│   └── main_data_pipeline.py   # Main pipeline script
├── .dbt/                       # dbt profiles directory
│   └── profiles.yml           # dbt connection profiles
├── data/                       # Processed data files
├── metrics/                    # Data quality metrics
├── reports/                    # Generated analytics reports
├── pdp_config.env             # Environment configuration
└── README.md                  # This file
```

## Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/Product_data_pipelining.git
cd Product_data_pipelining
```

### 2. Set Up Virtual Environment
```bash
python -m venv .venv
```

### 3. Activate Virtual Environment
- Windows:
  ```bash
  .venv\Scripts\activate
  ```
- macOS/Linux:
  ```bash
  source .venv/bin/activate
  ```

### 4. Install Python Dependencies
```bash
pip install -e .
```

### 5. Configure Environment Variables
1. Copy the example environment file:
   ```bash
   cp pdp_config.env.example pdp_config.env
   ```
2. Edit `pdp_config.env` with your specific configurations:
   ```env
   MINIO_ENDPOINT=your-minio-endpoint
   MINIO_ACCESS_KEY=your-access-key
   MINIO_SECRET_KEY=your-secret-key
   MINIO_BUCKET_NAME=your-bucket-name
   MINIO_USE_SSL=False
   ```

### 6. Set Up dbt Profile
1. Create the `.dbt` directory:
   ```bash
   mkdir .dbt
   ```
2. Copy the example profile:
   ```bash
   cp profiles.example.yml .dbt/profiles.yml
   ```
3. Edit `.dbt/profiles.yml` with your DuckDB configuration.

### 7. Install dbt Packages
```bash
cd dbt_pipeline_demo
dbt deps
cd ..
```

## Running the Pipeline

### Execute the Full Pipeline
```bash
python scripts/main_data_pipeline.py
```

### Pipeline Steps
1. **Environment Verification**: Checks for active virtual environment
2. **Dependency Check**: Verifies required packages are installed
3. **Database Setup**: Creates/clears the DuckDB database
4. **Data Generation**: Generates synthetic product data
5. **Data Preparation**: Cleans and transforms the raw data
6. **Database Loading**: Loads data into DuckDB
7. **dbt Transformations**: Runs dbt models to transform the data
8. **Report Generation**: Creates analytics reports
9. **Data Upload**: Uploads final data to S3

## Configuration

### Environment Variables
| Variable              | Description                          | Default Value          |
|-----------------------|--------------------------------------|------------------------|
| `MINIO_ENDPOINT`      | MinIO server endpoint                | -                      |
| `MINIO_ACCESS_KEY`    | MinIO access key                     | -                      |
| `MINIO_SECRET_KEY`    | MinIO secret key                     | -                      |
| `MINIO_BUCKET_NAME`   | MinIO bucket name                    | -                      |
| `MINIO_USE_SSL`       | Use SSL for MinIO connection         | `False`                |
| `DEFAULT_NUM_ROWS`    | Number of rows to generate           | `8000`                 |
| `START_DATETIME`      | Start date for generated data        | `2022-01-01 10:30`     |
| `END_DATETIME`        | End date for generated data          | `2024-12-31 23:59`     |
| `VALID_STATUSES`      | Valid purchase statuses              | `pending,completed,failed` |
| `LOG_LEVEL`           | Logging level                        | `INFO`                 |
| `LOG_FILE`            | Log file path                        | `logs/pipeline.log`    |

## Data Model

### Source Tables
- `user_activity`: Raw user activity data

### dbt Models
- `stg_user_activity`: Staging model for user activity
- `dim_user`: User dimension table
- `fact_user_activity`: Fact table for user activity

## Analytics Reports
The pipeline generates the following reports:
1. Lifecycle Analysis
2. Purchase Analysis
3. Demographics Analysis
4. Business Analysis
5. Engagement Analysis
6. Churn Analysis
7. Session Analysis
8. Funnel Analysis

Reports are saved in the `reports/` directory in CSV format.

## Maintenance

### Clearing Generated Files
```bash
# Clear database
rm -rf dbt_pipeline_demo/databases/*

# Clear reports
rm -rf reports/*

# Clear metrics
rm -rf metrics/*

# Clear data files
rm -rf data/*
```

### Updating dbt Packages
```bash
cd dbt_pipeline_demo
dbt deps
cd ..
```

## Troubleshooting

### Common Issues
1. **Virtual Environment Not Active**:
   - Ensure the virtual environment is activated before running the pipeline
   - Verify by checking `sys.prefix` in Python

2. **dbt Packages Not Installed**:
   - Run `dbt deps` in the `dbt_pipeline_demo`