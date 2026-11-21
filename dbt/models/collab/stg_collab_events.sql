select
    event_id, event_type,
    cast(user_id as bigint)         as user_id,
    cast(team_id as bigint)         as team_id,
    file_id,
    cast(target_user_id as bigint)  as target_user_id,
    cast(ts as timestamp)           as event_ts,
    cast(cast(ts as timestamp) as date) as event_date,
    session_id, client,
    cast(_loaded_at as timestamp)   as loaded_at
from {{ source('collab', 'collab_events') }}
