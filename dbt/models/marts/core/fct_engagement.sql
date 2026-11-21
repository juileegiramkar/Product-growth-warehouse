-- One row per engagement event (like, comment, follow).
{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert'
) }}

select
    event_id,
    event_type,
    user_id                                     as actor_user_id,
    target_user_id,                             -- followee, or author of the post
    post_id,
    event_ts,
    event_date,
    session_id,
    device
from {{ ref('stg_events') }}
where event_type in ('like', 'comment', 'follow')
{% if is_incremental() %}
  and event_date >= (
      select {{ dbt.dateadd('day', -var('incremental_lookback_days'), 'max(event_date)') }}
      from {{ this }}
  )
{% endif %}
