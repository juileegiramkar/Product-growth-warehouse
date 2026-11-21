# Running the dbt project on Snowflake

The models only use cross-database macros (`dbt.dateadd`, `dbt.datediff`,
`dbt.date_trunc`, `dbt_utils.date_spine`), so the same project builds on DuckDB
and Snowflake. Only the source location changes: on DuckDB the sources read the
Spark Parquet in place, and on Snowflake they read `PULSE.RAW.EVENTS` / `USERS`.

1. Start a [Snowflake free trial](https://signup.snowflake.com/) and run `snowflake/setup.sql` in a worksheet.
2. Build the lake locally first (`docker compose up`, or `scripts/bootstrap.sh`).
3. Load the staging Parquet:
   ```bash
   export SNOWFLAKE_ACCOUNT=xy12345.us-east-1 SNOWFLAKE_USER=you
   bash snowflake/load.sh
   ```
4. Build the warehouse in Snowflake:
   ```bash
   pip install dbt-snowflake
   cd dbt
   export DBT_TARGET=snowflake SNOWFLAKE_PASSWORD=...   # plus ACCOUNT/USER from above
   dbt deps && dbt source freshness && dbt build
   dbt docs generate
   ```
5. Take screenshots of the `ANALYTICS_CORE` / `ANALYTICS_METRICS` schemas and the
   query history for the README. `generate_schema_name` writes `core` and `metrics`
   as the schema names.

Note: freshness on Snowflake compares `_loaded_at`, the time Spark wrote the
rows. Load within 24 hours of the Spark run, or the freshness check warns,
which is the behaviour you want.
