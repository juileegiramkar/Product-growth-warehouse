#!/usr/bin/env bash
# Fast path: build all 90 days in one shot (~6 min) instead of letting Airflow
# catch up day by day. Runs inside the Airflow image:
#   docker compose run --rm pipeline
# Pause the DAG first (or set AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true)
# so the two don't write the warehouse at the same time.
set -euo pipefail
cd "${PULSE_HOME:-/opt/pulse}"
python -m generator.generate --all
spark-submit --master 'local[*]' --driver-memory "${SPARK_DRIVER_MEMORY:-4g}" spark/clean_events.py --full
mkdir -p "$(dirname "${PULSE_WAREHOUSE_PATH:-data/warehouse/pulse.duckdb}")"
cd dbt
[ -d dbt_packages/dbt_utils ] || dbt deps
dbt source freshness
dbt build --full-refresh
cd ..
python scripts/refresh_extracts.py
python scripts/register_trino_tables.py || echo "Trino not reachable - skipped table registration"
