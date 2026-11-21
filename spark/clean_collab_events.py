"""PySpark job for the Canvas (collaboration app) event set.

Reuses the Pulse cleaning building blocks: mixed-timestamp parsing,
event_id dedup, null handling and 30-minute sessionisation.

    spark-submit spark/clean_collab_events.py
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from clean_events import SESSION_GAP_SECONDS, build_spark, parse_mixed_ts, remove_stale_temp_dirs


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", type=Path, default=Path(os.getenv("PULSE_RAW_DIR", "data/lake/raw")))
    ap.add_argument("--staging-dir", type=Path,
                    default=Path(os.getenv("PULSE_STAGING_DIR", "data/lake/staging")))
    a = ap.parse_args()
    remove_stale_temp_dirs(a.staging_dir / "collab_events")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    base = a.raw_dir / "collab_events"
    df = (spark.read.option("basePath", str(base)).parquet(str(base))
          .withColumn("ts", parse_mixed_ts("ts"))
          .withColumn("received_at", parse_mixed_ts("received_at"))
          .withColumn("client", F.coalesce(F.lower(F.trim("client")), F.lit("unknown"))))
    raw = df.count()
    df = (df.withColumn("_rn", F.row_number().over(
              Window.partitionBy("event_id").orderBy("received_at"))).filter("_rn = 1").drop("_rn")
            .filter(F.col("ts").isNotNull()))
    w = Window.partitionBy("user_id").orderBy("ts", "event_id")
    df = (df.withColumn("gap", F.col("ts").cast("long") - F.lag("ts").over(w).cast("long"))
            .withColumn("session_num", F.sum((F.col("gap").isNull() | (F.col("gap") > SESSION_GAP_SECONDS))
                                             .cast("int")).over(w))
            .withColumn("session_id", F.substring(F.sha2(F.concat_ws(
                "-", "user_id", "session_num", F.lit("canvas")), 256), 1, 32))
            .withColumn("event_date", F.to_date("ts"))
            .withColumn("_loaded_at", F.current_timestamp())
            .drop("gap", "date"))
    out = df.select("event_id", "event_type", "user_id", "team_id", "file_id", "target_user_id",
                    "ts", "event_date", "session_id", "client", "received_at", "_loaded_at")
    out.repartition("event_date").write.mode("overwrite").partitionBy("event_date") \
        .parquet(str(a.staging_dir / "collab_events"))
    print("DQ_METRICS " + json.dumps({"dataset": "collab", "raw_rows": raw,
                                      "rows_written": spark.read.parquet(str(a.staging_dir / "collab_events")).count()}))
    spark.stop()


if __name__ == "__main__":
    main()
