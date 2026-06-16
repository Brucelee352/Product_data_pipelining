#!/bin/bash
set -euo pipefail

echo "=== Kroger pipeline: fetching data and running dbt (background) ==="
run-pipeline &

echo "=== Starting Dash dashboard on port ${PORT} ==="
exec gunicorn \
    --bind ":${PORT}" \
    --workers 1 \
    --threads 8 \
    --timeout 0 \
    portfolio_app.app:server
