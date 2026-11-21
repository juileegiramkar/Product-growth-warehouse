"""PySpark job: raw events/users  ->  clean, deduplicated, sessionised staging.

Daily (incremental) mode, used by Airflow:
    spark-submit spark/clean_events.py --run-date 2026-09-01

  * reads raw arrival partitions [run_date-3, run_date]
    - late events arrive at most 2 days after they happen, so event dates
      [run_date-3, run_date] are complete in that slice
  * re-sessionises event dates run_date-3..run_date (day -3 is context so a
    session crossing midnight isn't split) and overwrites staging partitions
    run_date-2..run_date  -> late events are picked up on the next 2 runs
  * writes data-quality metrics for the run to staging/_dq/

Full mode (bootstrap / rebuild):
    spark-submit spark/clean_events.py --full
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

SESSION_GAP_SECONDS = 30 * 60
LOOKBACK_DAYS = 3          # staging partitions rewritten each run (run_date-2..run_date)
VALID_DEVICES = ["ios", "android", "web"]
SIM_START = date.fromisoformat(os.getenv("PULSE_SIM_START", "2026-08-21"))


def build_spark() -> SparkSession:
    return (SparkSession.builder.appName("pulse-clean-and-sessionize")
            .master(os.getenv("SPARK_MASTER", "local[*]"))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
            .config("spark.sql.ansi.enabled", "false")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def parse_mixed_ts(col: str) -> F.Column:
    """Normalise every timestamp format our clients send to a UTC timestamp.

    Seen in the wild:
      1757000000123                      android legacy: epoch millis
      2026-09-04T18:13:20.123Z           android / ISO-8601 UTC
      2026-09-04T23:43:20.123+05:30      iOS: local wall clock + offset
      2026-09-04 18:13:20.123            web: UTC, no zone
      09/04/2026 18:13:20                US-formatted, UTC
    """
    c = F.trim(F.col(col))
    return F.coalesce(
        F.when(c.rlike(r"^\d{12,14}$"), F.timestamp_millis(c.cast("long"))),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(c, "MM/dd/yyyy HH:mm:ss"),
    )


def normalise_device(col: str) -> F.Column:
    d = F.lower(F.trim(F.col(col)))
    return F.when(d.isin(VALID_DEVICES), d).otherwise(F.lit("unknown"))


def existing_partitions(base: Path, days: list[date]) -> list[str]:
    return [str(base / f"date={d.isoformat()}") for d in days
            if (base / f"date={d.isoformat()}").exists()]


# --------------------------------------------------------------------------
def clean_events(spark: SparkSession, raw: Path, staging: Path, quarantine: Path,
                 run_date: date | None) -> dict:
    base = raw / "events"
    if run_date is None:
        paths = [str(base)]
        out_from, out_to = None, None
    else:
        window = [run_date - timedelta(days=i) for i in range(LOOKBACK_DAYS, -1, -1)]
        paths = existing_partitions(base, window)
        if not paths:
            raise FileNotFoundError(f"no raw event partitions for {window[0]}..{run_date}")
        out_from, out_to = run_date - timedelta(days=LOOKBACK_DAYS - 1), run_date

    df = (spark.read.option("basePath", str(base)).parquet(*paths)
          .withColumn("arrival_date", F.col("date").cast("date")).drop("date")
          .withColumn("ts_utc", parse_mixed_ts("ts"))
          .withColumn("received_at", parse_mixed_ts("received_at")))
    df = df.cache()
    m = {"raw_rows": df.count()}

    # 1. timestamps: unparseable or impossible (broken device clocks) -> quarantine
    upper = F.lit(datetime.now(timezone.utc) + timedelta(days=1)).cast("timestamp")
    bad_ts = (F.col("ts_utc").isNull() | (F.col("ts_utc") < F.lit(str(SIM_START)).cast("timestamp"))
              | (F.col("ts_utc") > upper))
    bad = df.filter(bad_ts).withColumn(
        "reason", F.when(F.col("ts_utc").isNull(), "unparseable_ts").otherwise("ts_out_of_range"))
    m["quarantined_bad_ts"] = bad.count()
    if m["quarantined_bad_ts"]:
        tag = run_date.isoformat() if run_date else "full"
        bad.write.mode("overwrite").parquet(str(quarantine / "events" / f"run={tag}"))
    df = df.filter(~bad_ts)

    # 2. devices: nulls and messy casing ("iOS", "Android ") -> canonical
    m["null_device_filled"] = df.filter(F.col("device").isNull()).count()
    m["device_case_fixed"] = df.filter(F.col("device").isNotNull()
                                       & ~F.col("device").isin(VALID_DEVICES)).count()
    df = df.withColumn("device", normalise_device("device"))

    # 3a. exact redeliveries: same event_id -> keep first received
    w_id = Window.partitionBy("event_id").orderBy(F.col("received_at").asc_nulls_last())
    before = df.count()
    df = df.withColumn("_rn", F.row_number().over(w_id)).filter("_rn = 1").drop("_rn")
    m["dup_event_id_removed"] = before - df.count()

    # 3b. client retries: new event_id but identical payload -> natural key
    nk = ["user_id", "event_type", "ts_utc",
          F.coalesce("post_id", F.lit("")), F.coalesce("target_user_id", F.lit(-1)),
          F.coalesce("step_name", F.lit(""))]
    w_nk = Window.partitionBy(*nk).orderBy(F.col("received_at").asc_nulls_last(), "event_id")
    before = df.count()
    df = df.withColumn("_rn", F.row_number().over(w_nk)).filter("_rn = 1").drop("_rn")
    m["dup_natural_key_removed"] = before - df.count()

    df = df.withColumn("event_date", F.to_date("ts_utc"))
    if run_date is not None:  # keep context day for sessionisation
        df = df.filter(F.col("event_date").between(F.lit(str(run_date - timedelta(days=LOOKBACK_DAYS))),
                                                   F.lit(str(run_date))))

    # 4. sessionise: new session after > 30 minutes of inactivity
    w = Window.partitionBy("user_id").orderBy("ts_utc", "event_id")
    df = (df.withColumn("prev_ts", F.lag("ts_utc").over(w))
            .withColumn("new_session",
                        F.col("prev_ts").isNull()
                        | ((F.col("ts_utc").cast("long") - F.col("prev_ts").cast("long"))
                           > SESSION_GAP_SECONDS))
            .withColumn("session_num", F.sum(F.col("new_session").cast("int")).over(w)))
    w_s = Window.partitionBy("user_id", "session_num")
    df = (df.withColumn("session_start", F.min("ts_utc").over(w_s))
            .withColumn("session_id", F.substring(F.sha2(F.concat_ws(
                "-", F.col("user_id").cast("string"),
                F.unix_millis("session_start").cast("string")), 256), 1, 32))
            .withColumn("seq_in_session", F.row_number().over(
                Window.partitionBy("user_id", "session_num").orderBy("ts_utc", "event_id"))))

    if run_date is not None:
        df = df.filter(F.col("event_date").between(F.lit(str(out_from)), F.lit(str(out_to))))

    df = df.withColumn("is_late", F.col("arrival_date") > F.col("event_date"))
    out = df.select(
        "event_id", "event_type", "user_id", F.col("ts_utc").alias("ts"), "event_date",
        "session_id", "session_start", "seq_in_session", "device", "app_version",
        "step_name", "post_id", "content_type", "target_user_id",
        "arrival_date", "received_at", "is_late",
        F.current_timestamp().alias("_loaded_at"),
        F.lit(run_date.isoformat() if run_date else "full").alias("_run"),
    ).repartition("event_date")

    out = out.cache()
    m["rows_written"] = out.count()
    m["late_events_in_output"] = out.filter("is_late").count()
    m["sessions_in_output"] = out.select("session_id").distinct().count()
    m["partitions_written"] = out.select("event_date").distinct().count()
    (out.write.mode("overwrite").partitionBy("event_date")
        .parquet(str(staging / "events")))
    df.unpersist()
    return m


def clean_users(spark: SparkSession, raw: Path, staging: Path) -> dict:
    base = raw / "users"
    df = (spark.read.option("basePath", str(base)).parquet(str(base))
          .withColumn("signup_ts", parse_mixed_ts("signup_ts"))
          .withColumn("received_at", parse_mixed_ts("received_at")))
    df = df.cache()
    m = {"users_raw_rows": df.count(),
         "users_null_device": df.filter(F.col("device").isNull()).count()}
    # profile re-sends: latest received version wins (SCD type 1)
    w = Window.partitionBy("user_id").orderBy(F.col("received_at").desc())
    out = (df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
             .select("user_id", "username", "signup_ts",
                     F.to_date("signup_ts").alias("signup_date"),
                     F.upper(F.trim("country")).alias("country"),
                     normalise_device("device").alias("device"),
                     F.lower(F.trim("acquisition_channel")).alias("acquisition_channel"),
                     "age_bucket", "received_at",
                     F.current_timestamp().alias("_loaded_at")))
    out = out.cache()
    m["users_written"] = out.count()
    m["users_resends_collapsed"] = m["users_raw_rows"] - m["users_written"]
    out.coalesce(1).write.mode("overwrite").parquet(str(staging / "users"))
    return m


def remove_stale_temp_dirs(path: Path) -> None:
    """Delete leftovers of a Spark job that was killed mid-write. Spark only
    renames these into place on commit, so anything left over is garbage."""
    if not path.exists():
        return
    for d in list(path.glob(".spark-staging-*")) + list(path.glob("_temporary")):
        shutil.rmtree(d, ignore_errors=True)
        print(f"removed stale Spark temp dir {d}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", type=date.fromisoformat)
    ap.add_argument("--full", action="store_true")
    ap.add_argument("--raw-dir", type=Path, default=Path(os.getenv("PULSE_RAW_DIR", "data/lake/raw")))
    ap.add_argument("--staging-dir", type=Path,
                    default=Path(os.getenv("PULSE_STAGING_DIR", "data/lake/staging")))
    ap.add_argument("--quarantine-dir", type=Path,
                    default=Path(os.getenv("PULSE_QUARANTINE_DIR", "data/lake/quarantine")))
    a = ap.parse_args()
    if not a.full and not a.run_date:
        ap.error("pass --run-date YYYY-MM-DD or --full")

    for sub in ("events", "users"):
        remove_stale_temp_dirs(a.staging_dir / sub)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    run_date = None if a.full else a.run_date
    metrics = {"run": run_date.isoformat() if run_date else "full",
               "finished_at": None}
    metrics.update(clean_events(spark, a.raw_dir, a.staging_dir, a.quarantine_dir, run_date))
    metrics.update(clean_users(spark, a.raw_dir, a.staging_dir))
    metrics["finished_at"] = datetime.now(timezone.utc).isoformat()

    dq_dir = a.staging_dir / "_dq"
    dq_dir.mkdir(parents=True, exist_ok=True)
    (dq_dir / f"run={metrics['run']}.json").write_text(json.dumps(metrics, indent=2))
    print("DQ_METRICS " + json.dumps(metrics))
    spark.stop()


if __name__ == "__main__":
    main()
