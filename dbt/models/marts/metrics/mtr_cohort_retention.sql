-- Day-1 / 7 / 14 / 30 retention per weekly signup cohort (eligible users only).
select
    signup_week                                                           as cohort_week,
    count(*)                                                              as cohort_users
    {% for n in [1, 7, 14, 30] %}
    , sum(case when d{{ n }}_eligible then 1 else 0 end)                  as d{{ n }}_eligible_users
    , round(sum(case when d{{ n }}_eligible and d{{ n }}_retained then 1 else 0 end) * 1.0
            / nullif(sum(case when d{{ n }}_eligible then 1 else 0 end), 0), 4) as d{{ n }}_retention
    {% endfor %}
from {{ ref('fct_user_retention') }}
group by 1
