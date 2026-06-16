FROM python:3.12-slim

# Pull uv binary from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install dependencies first (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy application source
COPY portfolio_app/ ./portfolio_app/

# Copy the pre-built DuckDB file (run the pipeline locally first)
COPY dbt_pipeline_demo/databases/kroger_pipeline.duckdb \
     ./dbt_pipeline_demo/databases/kroger_pipeline.duckdb

# Cloud Run injects PORT; default matches Cloud Run's expected port
ENV PORT=8080

CMD ["/app/.venv/bin/dashboard"]
