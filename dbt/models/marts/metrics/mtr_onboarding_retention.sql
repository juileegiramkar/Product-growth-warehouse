-- Day-7 retention split by whether each onboarding step was completed in the
-- user's first 24 hours. Feeds the "which step predicts retention" insight.
{% set steps = var('onboarding_steps') %}
with base as (
    select u.*, r.d7_retained
    from {{ ref('dim_users') }} u
    join {{ ref('fct_user_retention') }} r using (user_id)
    where r.d7_eligible
)
{% for s in steps %}
select
    {{ loop.index }}                                   as step_order,
    '{{ s }}'                                          as step_name,
    '{{ step_label(s) }}'                              as step_label,
    did_{{ s }}_24h                                    as completed_in_24h,
    count(*)                                           as users,
    sum(case when d7_retained then 1 else 0 end)       as d7_retained_users,
    round(avg(case when d7_retained then 1.0 else 0.0 end), 4) as d7_retention
from base
group by did_{{ s }}_24h
{{ 'union all' if not loop.last }}
{% endfor %}
