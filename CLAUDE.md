# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies (use this, not pip install -r requirements.txt)
python -m venv .venv
.venv\Scripts\activate           # Windows
source .venv/bin/activate        # macOS/Linux
pip install -e .

# Run the full data pipeline
python portfolio_app/scripts/main_data_pipeline.py

# Launch the Dash analytics dashboard (requires pipeline to have run first)
python portfolio_app/app.py
# Dashboard runs at http://127.0.0.1:8050

# dbt operations (run from repo root — the pipeline handles chdir internally)
cd dbt_pipeline_demo
dbt deps
dbt run --target dev --full-refresh
dbt test
cd ..
```

The above is outdated, I initialized uv for this repo, so an end user just needs to use the .venv contained within this repo's lock file. 

The pipeline itself must invoke dbt in place and re-target the duckdb database only. I could use S3 again, but I don't see the need for that level of complexity, I will leave it to the agent to determine if the amount of records we have requires the use of S3 for data storage. 

Python must be **exactly 3.12.x** — 3.13+ breaks mashumaro (a transitive dependency) for dbt.

## Architecture

The pipeline is a sequential, single-process ETL that runs entirely within one script call, however because this is a refactoring project, the steps below are merely a reference point and not anything that is needed to be adhered to:

```
generate_data() → prepare_data() → make_ua_table() → run_dbt_ops() → generate_reports() → upload_data()
```

1. **Data generation** (`main_data_pipeline.py`): Faker generates 10,000 synthetic SaaS user records. `basic_cleaning` → `advanced_cleaning` (parses user-agent strings into `device_type`, `os`, `browser`) → `add_analysis_fields` (adds `cohort_date`, `price_tier`, `engagement_level`, `customer_lifetime_value`). Cleaned data is saved as CSV/JSON/Parquet under `data/` and quality metrics under `metrics/`.

2. **DuckDB loading**: The cleaned DataFrame is written to `dbt_pipeline_demo/databases/dbt_pipeline_demo.duckdb` as the `user_activity` source table. The database is fully dropped and recreated on each pipeline run for reproducibility.

3. **dbt transformations** (`dbt_pipeline_demo/`): `run_dbt_ops()` calls `os.chdir()` to the dbt project root before invoking dbt — the dbt profile uses a relative path (`databases/dbt_pipeline_demo.duckdb`) so this is required. The model DAG:
   - Source: `raw_data.user_activity`
   - Staging (views): `stg_product_schema`, `stg_user_activity`
   - Dimensions (tables in `dimensions` schema): `dim_user`, `dim_product`, `dim_platform`
   - Fact (table in `fact` schema): `fact_user_activity`
   - Final queryable table: `main.product_schema` — this is what all analytics queries hit

4. **Analytics** (`analytics_queries.py`): Six SQL queries run against `main.product_schema` and save CSV reports to `reports/`. The same query functions are called by the Dash app at render time.

5. **MinIO upload**: Uploads the final `main.product_schema` data to S3-compatible MinIO. Credentials are hardcoded in `constants.py` (not from environment variables at runtime, despite the env var table in the README).

6. **Dash app** (`portfolio_app/app.py`): Reads the DuckDB file directly for chart queries and also pulls Parquet from MinIO for the download feature. The "Refresh" button re-runs dbt on the data currently in MinIO. The app requires the DuckDB file to exist before startup — run the pipeline first.

## Configuration

All paths and settings live in `portfolio_app/scripts/constants.py`. To switch environments, edit the MinIO constants there directly:
- `MINIO_ENDPOINT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_BUCKET_NAME`, `MINIO_USE_SSL`
- For local dev: set `MINIO_ENDPOINT = 'localhost:9000'` and `MINIO_USE_SSL = False`; start MinIO with `docker-compose up -d`

`DEFAULT_NUM_ROWS`, `START_DATETIME`, `END_DATETIME`, and `FAKER_SEED` in `constants.py` control data generation volume and reproducibility.

The `LOG_LEVEL` environment variable overrides the default `INFO` log level. Logs are written to `logs/pipeline.log`.

If possible, do not have the agents employ a constants.py if needed. Stick only to env files that I can choose to have .gitignored 

## dbt Profile Note

The dbt profile (`.dbt/profiles.yml`) uses a relative path `databases/dbt_pipeline_demo.duckdb`. This only resolves correctly when dbt is invoked from inside `dbt_pipeline_demo/`. The `run_dbt_ops()` function handles this via `os.chdir()`, but if running dbt manually from the repo root it will fail to find the database.

---

The pipeline as it exists now is broken, it needs a new data source, its dbt models need to be redone for that new data source and the Dash conversion from Streamlit was botched. I need the agents and subagents to work through refactoring the old code to use Dash properly using a new data source that I have given the agents. 

Use the hook I defined in /hooks/block-env-read.sh