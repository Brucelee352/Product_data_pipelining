# Kroger Product Analysis

A Python data pipeline that fetches live product, pricing, and store-location data from the Kroger Public API, transforms it with dbt + DuckDB, and serves an interactive Dash analytics dashboard showing fulfillment KPIs and price trends across Kroger store locations.

![Python 3.12](https://img.shields.io/badge/python-3.12-blue)

---

## Overview

The pipeline authenticates against the Kroger OAuth2 endpoint, discovers store locations near ten representative US zip codes, fetches live product catalogs and pricing across eight grocery categories, and writes everything to a local DuckDB database. 

A dbt project then transforms the raw tables into a clean mart layer. A Dash dashboard reads the mart layer directly and renders five static charts covering pricing distributions, stock availability, promotional discounts, and fulfillment channel coverage.

All credentials and paths are configured through a single `.env` file — there are no hardcoded secrets in the source code.

---

## Architecture

### Pipeline DAG

```
get_access_token()
    └─► fetch_locations()          # 10 zip codes × up to 10 stores each
            └─► load_locations()   # upsert → raw.locations
    └─► fetch_products()           # 8 category terms × each discovered location
            └─► load_products_and_prices()
                    ├─► raw.products
                    └─► raw.product_prices
run_dbt_ops()                      # dbt deps → dbt run --full-refresh
```

**Category search terms:** `produce`, `dairy`, `bakery`, `meat`, `frozen`, `snacks`, `beverages`, `household`

**Zip codes searched:** `10001` (New York), `60601` (Chicago), `77001` (Houston), `90001` (Los Angeles), `30301` (Atlanta), `85001` (Phoenix), `98101` (Seattle), `78201` (San Antonio), `33101` (Miami), `02101` (Boston)

The pipeline makes approximately 165 API calls per full run and stays well within the 10,000-call/day rate limit.

### dbt Model DAG

```
Source (raw schema)
├── raw.locations
├── raw.products
└── raw.product_prices
        │
        ▼
Staging (views — main_staging schema)
├── stg_locations      ← filters nulls, passes through address + geo fields
├── stg_products       ← extracts primary_category from JSON categories array
└── stg_prices         ← computes discount_amount, discount_pct; casts dates
        │
        ▼
Dimensions (tables — main_marts schema)
├── dim_locations      ← location_id, name, chain, city, state, zip_code, lat/lon
└── dim_products       ← product_id, description, brand, category
        │
        ▼
Fact (table — main_marts schema)
└── fact_prices        ← joined price + product + location grain
        │
        ▼
Mart aggregates (tables — main_marts schema)
├── mart_location_sales         ← per-store: avg_price, total_products, physical/online counts
├── mart_category_distribution  ← per-category: product_count, avg_regular/promo price
└── mart_price_by_category      ← per-category per date: percentiles, min/max, avg discount
```

The dashboard queries `main_marts.fact_prices` and `main_marts.mart_location_sales` directly.

---

## Prerequisites

- **Python 3.12.x** — exactly 3.12; 3.13+ breaks mashumaro, a transitive dbt dependency
- **uv** — package manager ([install instructions](https://docs.astral.sh/uv/getting-started/installation/))
- **Kroger Developer account** — register at [https://developer.kroger.com](https://developer.kroger.com) to obtain `KROGER_CLIENT_ID` and `KROGER_CLIENT_SECRET`; the free tier supports up to 10,000 API calls per day

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/Brucelee352/Product_data_pipelining.git
cd Product_data_pipelining
```

### 2. Install dependencies

```bash
uv sync
```

This creates a `.venv` and installs all locked dependencies from `uv.lock`. Do not use `pip install` directly.

### 3. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in your Kroger API credentials (see [Configuration](#configuration) below).

### 4. Run the pipeline

```bash
uv run run-pipeline
```

Or, with the virtual environment activated:

```bash
run-pipeline
```

The pipeline will authenticate, fetch data, populate DuckDB, and run dbt transformations automatically. Logs are written to both stdout and `logs/pipeline.log`. A full run takes two to five minutes depending on network latency.

### 5. Launch the dashboard

```bash
uv run dashboard
```

Or, with the virtual environment activated:

```bash
dashboard
```

Open [http://127.0.0.1:8050](http://127.0.0.1:8050) in your browser. The dashboard requires the pipeline to have run at least once so that `dbt_pipeline_demo/databases/kroger_pipeline.duckdb` exists.

---

## Configuration

All configuration is read from the `.env` file in the project root. Copy `.env.example` to `.env` and set each value.

| Variable | Required | Default | Description |
|---|---|---|---|
| `KROGER_CLIENT_ID` | Yes | — | OAuth2 client ID from https://developer.kroger.com |
| `KROGER_CLIENT_SECRET` | Yes | — | OAuth2 client secret from https://developer.kroger.com |
| `KROGER_BASE_URL` | No | `https://api.kroger.com` | Kroger API base URL |
| `DB_PATH` | No | `dbt_pipeline_demo/databases/kroger_pipeline.duckdb` | Path to the DuckDB file, relative to the project root |
| `LOG_LEVEL` | No | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

Kroger OAuth2 tokens expire after approximately 30 minutes. The pipeline fetches a single token at startup, which is sufficient for a full run.

---

## Dashboard

The dashboard is a single static page with no controls. All five charts are rendered from the mart tables in DuckDB at startup.

### Chart 1 — Product Price Distribution (Box & Whisker)

Box plot of regular prices grouped by product category. Categories are sorted by median price descending. If Q1 2026 price data is present the chart is scoped to that quarter; otherwise it falls back to the full date range available in the database.

### Chart 2 — Store Fulfillment KPIs & Avg Price by Location (Choropleth + Scatter Map)

US map combining two overlapping layers. The choropleth shades each state by average catalog price across all stores in that state. A scatter overlay places a bubble at each discovered store location, sized by the number of unique products in its catalog and colored by average price. Hovering a bubble shows the store name, city, zip code, average price, total product count, and a breakdown of physical versus online fulfillment counts.

### Chart 3 — Stock Availability by Category (Stacked Bar)

Horizontal stacked bar chart showing the share of items in each stock status (`In stock (high)`, `Low stock`, `Out of stock`) per category, normalized to 100 %. Categories are ordered from highest to lowest out-of-stock rate.

### Chart 4 — Average Promotional Discount Depth by Category (Bar)

Horizontal bar chart of the average promotional discount percentage per category, restricted to categories with at least five promotional items. Ordered from deepest to shallowest discount. Hover shows average regular price and promotional item count alongside the discount percentage.

### Chart 5 — Category Value Map (Bubble Scatter)

Scatter plot with average regular price on the X axis and average promotional discount depth on the Y axis. Each bubble represents one category and is sized by the number of distinct products. Median reference lines divide the plot into four quadrants for quick identification of high-price/high-discount versus low-price/low-discount categories.

---

## Project Structure

```
.
├── .dbt/
│   └── profiles.yml               # dbt connection profile (DuckDB, relative path)
├── .env.example                   # environment variable template
├── dbt_pipeline_demo/
│   ├── databases/
│   │   └── kroger_pipeline.duckdb # generated on first pipeline run
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_locations.sql
│   │   │   ├── stg_products.sql
│   │   │   └── stg_prices.sql
│   │   └── marts/
│   │       ├── dim_locations.sql
│   │       ├── dim_products.sql
│   │       ├── fact_prices.sql
│   │       ├── mart_category_distribution.sql
│   │       ├── mart_location_sales.sql
│   │       └── mart_price_by_category.sql
│   ├── dbt_project.yml
│   └── packages.yml
├── logs/
│   └── pipeline.log               # generated at runtime
├── portfolio_app/
│   ├── app.py                     # Dash dashboard entry point
│   └── scripts/
│       └── main_pipeline.py       # ETL entry point
├── pyproject.toml                 # package metadata and dependencies
└── uv.lock                        # locked dependency graph
```

---

## Troubleshooting

### Wrong Python version

The package specifies `requires-python = ">= 3.12, < 3.13"`. If `uv sync` fails or dbt throws import errors, verify your Python version:

```bash
python --version
```

Install Python 3.12 via [python.org](https://www.python.org/downloads/) or `pyenv`, then re-run `uv sync`.

### dbt cannot find the database

The dbt profile in `.dbt/profiles.yml` uses the relative path `databases/kroger_pipeline.duckdb`. This path resolves correctly only when dbt is invoked from inside `dbt_pipeline_demo/`. The pipeline handles this automatically via an internal `os.chdir()`. If you run dbt manually from the project root it will fail to locate the database. Always invoke dbt from `dbt_pipeline_demo/`:

```bash
cd dbt_pipeline_demo
dbt run --target dev --full-refresh
cd ..
```

### Missing or invalid Kroger credentials

If the pipeline exits with `KROGER_CLIENT_ID and KROGER_CLIENT_SECRET must be set`, confirm that:

1. You have a registered application at [https://developer.kroger.com](https://developer.kroger.com)
2. Your `.env` file exists in the project root and contains the correct values
3. The virtual environment is active when you run the pipeline (the pipeline will print activation instructions and exit if no virtual environment is detected)

### Dashboard shows empty charts

The dashboard requires a populated DuckDB file. Run `uv run run-pipeline` first. If a specific chart is blank after the pipeline has run successfully, check `logs/pipeline.log` for dbt errors — a failed model will leave the corresponding mart table missing.

### Rate limit errors from the Kroger API

The free developer tier allows 10,000 API calls per day. A full run uses approximately 165 calls. If you receive HTTP 429 responses, wait until your daily quota resets (midnight UTC) before re-running.

---

## Author

Bruce Anthony Lee — [brucelee352@gmail.com](mailto:brucelee352@gmail.com)  
Portfolio: [kro.brucea-lee.com](https://kro.brucea-lee.com)  
LinkedIn: [linkedin.com/in/brucealee](https://www.linkedin.com/in/brucealee/)  
Repository: [github.com/Brucelee352/Product_data_pipelining](https://github.com/Brucelee352/Product_data_pipelining)
