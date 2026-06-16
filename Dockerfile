FROM python:3.12-slim

WORKDIR /app

# Install uv from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# ── Layer 1: dependencies only ───────────────────────────────────────────────
# Copy manifests first so this layer is rebuilt only when deps change.
# --no-install-project skips the editable install of this repo (source not
# present yet). --python pins to the system interpreter already in the image.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --python /usr/local/bin/python3.12

# ── Layer 2: application source ──────────────────────────────────────────────
COPY README.md ./
COPY portfolio_app/ ./portfolio_app/
COPY dbt_pipeline_demo/ ./dbt_pipeline_demo/
COPY .dbt/ ./.dbt/

# Install the project itself now that source is present
RUN uv sync --frozen --python /usr/local/bin/python3.12

# Pre-fetch dbt packages so cold starts don't hit the network
RUN cd dbt_pipeline_demo && \
    /app/.venv/bin/dbt deps --profiles-dir ../.dbt --quiet

RUN mkdir -p dbt_pipeline_demo/databases logs

# Activate the venv for CMD
ENV VIRTUAL_ENV="/app/.venv"
ENV PATH="/app/.venv/bin:$PATH"

# Cloud Run injects PORT; default to 8050 for plain docker run
ENV PORT=8050

EXPOSE 8050

# ── Layer 3: bake pipeline output into the image ─────────────────────────────
# Build with: docker build --secret id=env,src=.env .
# The secret is mounted read-only for this step only — it never appears in any
# image layer. The resulting DuckDB file is baked in so cold starts are instant.
RUN --mount=type=secret,id=env,dst=/app/.env \
    python -m portfolio_app.scripts.main_pipeline

CMD exec gunicorn --bind ":${PORT}" --workers 1 --threads 8 --timeout 120 portfolio_app.app:server
