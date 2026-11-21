-- One row per user: which onboarding steps they finished, when, and whether
-- it happened inside their first 24 hours.
{% set steps = var('onboarding_steps') %}

with steps as (
    select user_id, step_name, min(event_ts) as completed_at
    from {{ ref('stg_events') }}
    where event_type = 'onboarding_step'
    group by 1, 2
),

pivoted as (
    select
        u.user_id,
        u.signup_ts,
        u.signup_date
        {% for s in steps %}
        , max(case when st.step_name = '{{ s }}' then st.completed_at end) as {{ s }}_at
        {% endfor %}
    from {{ ref('stg_users') }} u
    left join steps st on st.user_id = u.user_id
    group by 1, 2, 3
)

select
    user_id,
    signup_ts,
    signup_date
    {% for s in steps %}
    , {{ s }}_at
    , {{ s }}_at is not null                                              as did_{{ s }}
    , coalesce({{ s }}_at < {{ dbt.dateadd('hour', 24, 'signup_ts') }}, false) as did_{{ s }}_24h
    , {{ dbt.datediff('signup_ts', s ~ '_at', 'minute') }} / 60.0         as hours_to_{{ s }}
    {% endfor %}
    , {% for s in steps %}cast({{ s }}_at is not null as integer){{ ' + ' if not loop.last }}{% endfor %} as steps_completed
    , {% for s in steps %}cast(coalesce({{ s }}_at < {{ dbt.dateadd('hour', 24, 'signup_ts') }}, false) as integer){{ ' + ' if not loop.last }}{% endfor %} as steps_completed_24h
from pivoted
