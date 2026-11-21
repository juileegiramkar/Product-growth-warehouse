-- One row per user, already deduplicated by Spark (latest profile wins).
select
    cast(user_id as bigint)                         as user_id,
    username,
    cast(signup_ts as timestamp)                    as signup_ts,
    cast(cast(signup_ts as timestamp) as date)      as signup_date,
    country,
    device,
    acquisition_channel,
    age_bucket,
    cast(_loaded_at as timestamp)                   as loaded_at
from {{ source('lake', 'users') }}
