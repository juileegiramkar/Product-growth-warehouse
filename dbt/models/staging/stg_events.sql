-- Typed, renamed view over the Spark staging output. One row per event.
select
    event_id,
    event_type,
    cast(user_id as bigint)                         as user_id,
    cast(ts as timestamp)                           as event_ts,
    cast(cast(ts as timestamp) as date)             as event_date,
    session_id,
    cast(session_start as timestamp)                as session_start_ts,
    cast(seq_in_session as integer)                 as seq_in_session,
    device,
    app_version,
    step_name,
    post_id,
    content_type,
    cast(target_user_id as bigint)                  as target_user_id,
    cast(arrival_date as date)                      as arrival_date,
    is_late,
    cast(_loaded_at as timestamp)                   as loaded_at
from {{ source('lake', 'events') }}
