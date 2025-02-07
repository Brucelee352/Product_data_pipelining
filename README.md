# Product Data Pipeline Ver. 4: 

## Introduction

This project serves as a guide on how to produce business-centric product data as JSON, for containerized data ingestion; and further orchestration using Docker, MinIO and Python. I'm looking to make this as simple as possible, while being applicable to real-world business scenarios and use-cases.

<<<<<<< Updated upstream
## Data Sourcing
=======
## Project Structure
```
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
>>>>>>> Stashed changes

The central piece of this project is the Python script that produces the data itself. I'm specifically generating columns that reflect how real-world users interact with transactional databases; relying on the following assumptions:

- Their data has comprehensive metadata, including the date and time of the transaction, the user's ID, the product's ID, and the price of the product.

## Recent Changes:

1/30/2025:
  1. Extensive rewrites of the pipeline's functions, analytical query logic and dbt environmental models, their configurations, and references in joins.
  2. Established proper file structure in dbt to prevent errors and vet for reproducibility for the repo.
  3. Pipeline is stable and working, with all models properly executed.

### To-Dos:
  1. ~~Design models in dbt and launch~~
  2. ~~Make Docker image file and host for portability of project~~
  3. ~~Make requirements and redo scripts for porting .env files~~
  4. PEP-8 Styling
  5. Merge new branch with main without losing everything. 
  6. Further test for portability
  7. Redo pipeline app for portfolio in Streamlit or Python-Dash
  8. Re-write this readme with comprehensive narrative outlining the process
  9. Edit scripts with comments emphasizing steps in the readme that are imperative to the pipeline working or not 
  10. Share 
