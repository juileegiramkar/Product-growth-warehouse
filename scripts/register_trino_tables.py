"""Register the Parquet extracts as external tables in Trino (hive.pulse.*).

Trino's Hive connector uses a file-based metastore and the local file system,
so the same tables dbt built can be queried with Presto/Trino SQL:

    docker compose exec trino trino --catalog hive --schema pulse \
        --execute "select activity_date, dau from mtr_daily_active_users order by 1 desc limit 7"
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pyarrow.parquet as pq
import trino

HOME = Path(os.getenv("PULSE_HOME", Path(__file__).resolve().parents[1]))
EXTRACTS = Path(os.getenv("PULSE_EXTRACTS_DIR", HOME / "data/extracts"))
HOST = os.getenv("TRINO_HOST", "localhost")
PORT = int(os.getenv("TRINO_PORT", "8085"))
# path of the extracts dir *inside the Trino container*, relative to local.location
TRINO_ROOT = os.getenv("TRINO_EXTRACTS_URI", "local:///extracts")


def trino_type(t) -> str:
    import pyarrow as pa
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "varchar"
    if pa.types.is_int64(t):
        return "bigint"
    if pa.types.is_int32(t):
        return "integer"
    if pa.types.is_int16(t) or pa.types.is_int8(t):
        return "integer"
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_floating(t):
        return "double"
    if pa.types.is_date(t):
        return "date"
    if pa.types.is_timestamp(t):
        return "timestamp(6)"
    raise TypeError(f"unmapped arrow type {t}")


def main() -> None:
    conn = trino.dbapi.connect(host=HOST, port=PORT, user="pulse", catalog="hive")
    cur = conn.cursor()

    def run(sql: str):
        cur.execute(sql)
        return cur.fetchall()

    run(f"create schema if not exists hive.pulse with (location = '{TRINO_ROOT}/_trino_schema')")
    tables = sorted(p.parent.name for p in EXTRACTS.glob("*/data.parquet")
                    if not p.parent.name.startswith("."))
    for t in tables:
        schema = pq.read_schema(EXTRACTS / t / "data.parquet")
        cols = ",\n  ".join(f'"{f.name}" {trino_type(f.type)}' for f in schema)
        run(f"drop table if exists hive.pulse.{t}")  # external: data files are kept
        run(f"create table hive.pulse.{t} (\n  {cols}\n) with "
            f"(external_location = '{TRINO_ROOT}/{t}', format = 'PARQUET')")
        n = run(f"select count(*) from hive.pulse.{t}")[0][0]
        print(f"registered hive.pulse.{t}: {n} rows")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Trino registration failed: {exc}", file=sys.stderr)
        raise
