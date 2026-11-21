-- Run once in a Snowflake worksheet (30-day free trial is enough).
-- Then load the Spark staging Parquet with snowflake/load.sh and run
--   DBT_TARGET=snowflake dbt build
create database if not exists PULSE;
create schema if not exists PULSE.RAW;
create schema if not exists PULSE.ANALYTICS;
use schema PULSE.RAW;

create or replace file format parquet_ff type = parquet use_logical_type = true;
create or replace stage lake_stage file_format = parquet_ff;

create or replace table EVENTS (
    event_id          varchar,
    event_type        varchar,
    user_id           number,
    ts                timestamp_ntz,
    session_id        varchar,
    session_start     timestamp_ntz,
    seq_in_session    number,
    device            varchar,
    app_version       varchar,
    step_name         varchar,
    post_id           varchar,
    content_type      varchar,
    target_user_id    number,
    arrival_date      date,
    received_at       timestamp_ntz,
    is_late           boolean,
    _loaded_at        timestamp_ntz,
    _run              varchar
);

create or replace table USERS (
    user_id              number,
    username             varchar,
    signup_ts            timestamp_ntz,
    signup_date          date,
    country              varchar,
    device               varchar,
    acquisition_channel  varchar,
    age_bucket           varchar,
    received_at          timestamp_ntz,
    _loaded_at           timestamp_ntz
);
