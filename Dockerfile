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
COPY docker-entrypoint.sh /docker-entrypoint.sh

# Install the project itself now that source is present
RUN uv sync --frozen --python /usr/local/bin/python3.12

# Pre-fetch dbt packages so cold starts don't hit the network
RUN cd dbt_pipeline_demo && \
    /app/.venv/bin/dbt deps --profiles-dir ../.dbt --quiet

RUN mkdir -p dbt_pipeline_demo/databases logs && \
    chmod +x /docker-entrypoint.sh

# Activate the venv for ENTRYPOINT / CMD
ENV VIRTUAL_ENV="/app/.venv"
ENV PATH="/app/.venv/bin:$PATH"

# Cloud Run injects PORT; default to 8050 for plain docker run
ENV PORT=8050

EXPOSE 8050

ENTRYPOINT ["/docker-entrypoint.sh"]
