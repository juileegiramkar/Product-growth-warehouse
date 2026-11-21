"""Canvas (collaboration app) pipeline: the same lake -> Spark -> dbt pattern on
a second event set (file edits, comments, invites). Paused by default; trigger
it manually from the Airflow UI to build the collab marts.

It shares the DuckDB warehouse with the Pulse DAG and DuckDB allows a single
writer, so trigger it when pulse_daily_pipeline isn't mid-run.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PULSE_HOME = os.getenv("PULSE_HOME", "/opt/pulse")
VARS = "--vars '{enable_collab: true}'"

with DAG(
    dag_id="collab_daily_pipeline",
    start_date=pendulum.parse(os.getenv("PULSE_SIM_START", "2026-08-21"), tz="UTC"),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1),
                  "retry_exponential_backoff": True},
    tags=["collab", "extension"],
    doc_md=__doc__,
) as dag:
    generate = BashOperator(
        task_id="generate_collab_events",
        bash_command=f"cd {PULSE_HOME} && python -m generator.collab --all",
    )
    spark = BashOperator(
        task_id="spark_clean_collab",
        bash_command=f"cd {PULSE_HOME}/spark && spark-submit --master 'local[*]' clean_collab_events.py",
    )
    dbt = BashOperator(
        task_id="dbt_build_collab",
        bash_command=(f"mkdir -p {PULSE_HOME}/data/warehouse && cd {PULSE_HOME}/dbt && ([ -d dbt_packages/dbt_utils ] || dbt deps --quiet) && "
                      f"dbt source freshness --select source:collab {VARS} && "
                      f"dbt build --select path:models/collab {VARS}"),
    )
    generate >> spark >> dbt
