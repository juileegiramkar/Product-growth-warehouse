-- One row per user per active day. Incremental: each run rebuilds the last
-- N days so late-arriving events (re-processed by Spark) are picked up.
{{ config(
    materialized='incremental',
    unique_key=['user_id', 'activity_date'],
    incremental_strategy='delete+insert'
) }}

with events as (
    select * from {{ ref('stg_events') }}
    {% if is_incremental() %}
    where event_date >= (
        select {{ dbt.dateadd('day', -var('incremental_lookback_days'), 'max(activity_date)') }}
        from {{ this }}
    )
    {% endif %}
)

select
    e.user_id,
    e.event_date                                                      as activity_date,
    count(distinct e.session_id)                                      as sessions,
    count(*)                                                          as events,
    sum(case when e.event_type = 'post_created' then 1 else 0 end)    as posts,
    sum(case when e.event_type = 'like' then 1 else 0 end)            as likes_given,
    sum(case when e.event_type = 'comment' then 1 else 0 end)         as comments,
    sum(case when e.event_type = 'follow' then 1 else 0 end)          as follows,
    sum(case when e.event_type = 'onboarding_step' then 1 else 0 end) as onboarding_steps,
    true                                                              as is_active,
    {{ dbt.datediff('u.signup_date', 'e.event_date', 'day') }}        as days_since_signup,
    current_timestamp                                                 as _updated_at
from events e
join {{ ref('stg_users') }} u on u.user_id = e.user_id
group by e.user_id, e.event_date, u.signup_date
