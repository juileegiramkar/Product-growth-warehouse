-- One row per user with signup cohort, acquisition attributes and
-- first-24h onboarding flags (the features used in the retention analysis).
select
    u.user_id,
    u.username,
    u.signup_ts,
    u.signup_date,
    cast({{ dbt.date_trunc('week', 'u.signup_date') }} as date)  as signup_week,
    cast({{ dbt.date_trunc('month', 'u.signup_date') }} as date) as signup_month,
    u.country,
    u.device,
    u.acquisition_channel,
    u.age_bucket
    {% for s in var('onboarding_steps') %}
    , o.did_{{ s }}
    , o.did_{{ s }}_24h
    {% endfor %}
    , o.steps_completed
    , o.steps_completed_24h
from {{ ref('stg_users') }} u
left join {{ ref('fct_onboarding') }} o using (user_id)
