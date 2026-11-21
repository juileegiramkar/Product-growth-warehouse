-- Ordered onboarding funnel. users_reached = completed this step AND every
-- earlier step (within the first 7 days); also reports each step on its own.
{% set steps = var('onboarding_steps') %}
with o as (select * from {{ ref('fct_onboarding') }}),
total as (select count(*) as signups from o),
funnel as (
    select 0 as step_order, 'signup' as step_name, 'Signed up' as step_label,
           count(*) as users_reached, count(*) as users_completed_any_order,
           count(*) as users_reached_24h
    from o
    {% for s in steps %}
    union all
    select {{ loop.index }}, '{{ s }}', '{{ step_label(s) }}',
           sum(case when {% for p in steps[:loop.index] %}did_{{ p }}{{ ' and ' if not loop.last }}{% endfor %} then 1 else 0 end),
           sum(case when did_{{ s }} then 1 else 0 end),
           sum(case when {% for p in steps[:loop.index] %}did_{{ p }}_24h{{ ' and ' if not loop.last }}{% endfor %} then 1 else 0 end)
    from o
    {% endfor %}
)
select
    f.step_order,
    f.step_name,
    f.step_label,
    f.users_reached,
    round(f.users_reached * 1.0 / t.signups, 4)                                         as pct_of_signups,
    round(f.users_reached * 1.0 / nullif(lag(f.users_reached) over (order by f.step_order), 0), 4)
                                                                                        as step_conversion,
    f.users_completed_any_order,
    round(f.users_completed_any_order * 1.0 / t.signups, 4)                             as pct_completed_any_order,
    f.users_reached_24h,
    round(f.users_reached_24h * 1.0 / t.signups, 4)                                     as pct_reached_24h
from funnel f cross join total t
