"""Pulse product-growth pipeline, one run per day of data.

generate_events -> spark_clean_and_sessionize -> dbt_source_freshness
    -> dbt_run -> dbt_test -> refresh_dashboard_extract -> publish_to_trino

* catchup=True backfills the 90 days of history on first start
  (max_active_runs=1 because every run builds on the previous one).
* every task retries with exponential backoff.
* dbt_test failures fire an alert (task log + data/alerts/alerts.log, plus
  e-mail / Slack webhook when configured).
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PULSE_HOME = os.getenv("PULSE_HOME", "/opt/pulse")
SIM_START = os.getenv("PULSE_SIM_START", "2026-08-21")
ALERT_LOG = Path(PULSE_HOME) / "data" / "alerts" / "alerts.log"
log = logging.getLogger(__name__)


def alert_on_failure(context) -> None:
    """Failure alert: always logged + written to a file, optionally e-mail/Slack."""
    ti = context["task_instance"]
    msg = {
        "at": datetime.now(timezone.utc).isoformat(),
        "dag": ti.dag_id,
        "task": ti.task_id,
        "logical_date": context["ds"],
        "try": ti.try_number,
        "log_url": ti.log_url,
        "error": str(context.get("exception"))[:500],
    }
    log.error("PIPELINE ALERT %s", json.dumps(msg))
    ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with ALERT_LOG.open("a") as fh:
        fh.write(json.dumps(msg) + "\n")
    webhook = os.getenv("PULSE_SLACK_WEBHOOK")
    if webhook:
        body = json.dumps({"text": f":rotating_light: {ti.task_id} failed for {context['ds']}"
                                   f" - {msg['error']}"}).encode()
        req = urllib.request.Request(webhook, body, {"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=10)
        except Exception as exc:  # alerting must never mask the original failure
            log.warning("slack alert failed: %s", exc)


default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "email": [e for e in [os.getenv("PULSE_ALERT_EMAIL")] if e],
    "email_on_failure": bool(os.getenv("PULSE_ALERT_EMAIL")),
    "email_on_retry": False,
}

DBT = (f"mkdir -p \"$(dirname \"${{PULSE_WAREHOUSE_PATH:-{PULSE_HOME}/data/warehouse/pulse.duckdb}}\")\" && "
       f"cd {PULSE_HOME}/dbt && "
       "([ -d dbt_packages/dbt_utils ] || dbt deps --quiet) && ")

with DAG(
    dag_id="pulse_daily_pipeline",
    description="Pulse events -> PySpark staging -> dbt warehouse -> dashboard",
    start_date=pendulum.parse(SIM_START, tz="UTC"),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=1),
    tags=["pulse", "growth", "spark", "dbt"],
    doc_md=__doc__,
) as dag:

    generate_events = BashOperator(
        task_id="generate_events",
        bash_command=f"cd {PULSE_HOME} && python -m generator.generate --date {{{{ ds }}}}",
    )

    spark_clean_and_sessionize = BashOperator(
        task_id="spark_clean_and_sessionize",
        bash_command=(f"cd {PULSE_HOME} && spark-submit --master 'local[*]' "
                      "--driver-memory ${SPARK_DRIVER_MEMORY:-2g} "
                      "spark/clean_events.py --run-date {{ ds }}"),
        execution_timeout=timedelta(minutes=20),
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=DBT + "dbt source freshness",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=DBT + "dbt run",
        execution_timeout=timedelta(minutes=20),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT + "dbt test",
        on_failure_callback=alert_on_failure,
        retries=1,  # a failing data test will not fix itself; alert quickly
    )

    refresh_dashboard_extract = BashOperator(
        task_id="refresh_dashboard_extract",
        bash_command=f"cd {PULSE_HOME} && python scripts/refresh_extracts.py",
    )

    publish_to_trino = BashOperator(
        task_id="publish_to_trino",
        bash_command=f"cd {PULSE_HOME} && python scripts/register_trino_tables.py",
        retries=2,
    )

    (generate_events >> spark_clean_and_sessionize >> dbt_source_freshness
     >> dbt_run >> dbt_test >> refresh_dashboard_extract >> publish_to_trino)
