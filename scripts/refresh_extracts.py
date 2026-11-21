"""Export warehouse marts to Parquet (dashboard + Trino) and CSV (Tableau/Power BI).

The dashboard never opens the DuckDB file directly: DuckDB allows one writer
process, and dbt is that writer. Instead this task snapshots the marts into
data/extracts/<table>/data.parquet, swapping each directory atomically.
"""
from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path

import duckdb

HOME = Path(os.getenv("PULSE_HOME", Path(__file__).resolve().parents[1]))
WAREHOUSE = Path(os.getenv("PULSE_WAREHOUSE_PATH", HOME / "data/warehouse/pulse.duckdb"))
EXTRACTS = Path(os.getenv("PULSE_EXTRACTS_DIR", HOME / "data/extracts"))
STAGING = Path(os.getenv("PULSE_STAGING_DIR", HOME / "data/lake/staging"))

TABLES = {
    "core": ["dim_users", "dim_date", "dim_posts", "fct_user_activity", "fct_engagement",
             "fct_onboarding", "fct_user_retention"],
    "metrics": ["mtr_daily_active_users", "mtr_retention_weekly", "mtr_cohort_retention",
                "mtr_onboarding_funnel", "mtr_onboarding_retention"],
}
CSV_TABLES = {"fct_user_activity", "dim_users", *TABLES["metrics"]}  # BI-friendly sizes


def select_with_portable_types(con, fq: str) -> str:
    """HUGEINT/DECIMAL -> BIGINT/DOUBLE, TIMESTAMPTZ -> TIMESTAMP (Trino/Tableau friendly)."""
    cols = []
    for name, typ, *_ in con.execute(f"describe {fq}").fetchall():
        t = typ.upper()
        if t in ("HUGEINT", "UBIGINT"):
            cols.append(f'cast("{name}" as bigint) as "{name}"')
        elif t.startswith("DECIMAL"):
            cols.append(f'cast("{name}" as double) as "{name}"')
        elif t.startswith("TIMESTAMP WITH TIME ZONE"):
            cols.append(f'cast("{name}" as timestamp) as "{name}"')
        else:
            cols.append(f'"{name}"')
    return f"select {', '.join(cols)} from {fq}"


def main() -> None:
    t0 = time.time()
    EXTRACTS.mkdir(parents=True, exist_ok=True)
    (EXTRACTS / "csv").mkdir(exist_ok=True)
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    con.execute("set TimeZone = 'UTC'")
    summary = {}
    for schema, tables in TABLES.items():
        for t in tables:
            sql = select_with_portable_types(con, f"{schema}.{t}")
            tmp = EXTRACTS / f".{t}.tmp"
            shutil.rmtree(tmp, ignore_errors=True)
            tmp.mkdir()
            con.execute(f"copy ({sql}) to '{(tmp / 'data.parquet').as_posix()}' (format parquet)")
            final = EXTRACTS / t
            old = EXTRACTS / f".{t}.old"
            shutil.rmtree(old, ignore_errors=True)
            if final.exists():
                final.rename(old)
            tmp.rename(final)
            shutil.rmtree(old, ignore_errors=True)
            if t in CSV_TABLES:
                con.execute(f"copy ({sql}) to '{(EXTRACTS / 'csv' / f'{t}.csv').as_posix()}' (header)")
            summary[t] = con.execute(f"select count(*) from {schema}.{t}").fetchone()[0]

    # data-quality history from the Spark runs, for the dashboard DQ panel
    dq = []
    for f in sorted((STAGING / "_dq").glob("run=*.json")):
        dq.append(json.loads(f.read_text()))
    (EXTRACTS / "dq_runs.json").write_text(json.dumps(dq, indent=1))
    (EXTRACTS / "_manifest.json").write_text(json.dumps(
        {"refreshed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "rows": summary},
        indent=1))
    print(json.dumps({"tables": len(summary), "seconds": round(time.time() - t0, 1), "rows": summary}))


if __name__ == "__main__":
    main()
