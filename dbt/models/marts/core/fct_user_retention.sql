-- One row per user: day-N retention flags. "Active on day N" = any activity
-- on exactly signup_date + N. *_eligible = the day has already happened.
with bounds as (
    select max(activity_date) as max_date from {{ ref('fct_user_activity') }}
),
act as (
    select user_id, days_since_signup from {{ ref('fct_user_activity') }}
)
select
    u.user_id,
    u.signup_date,
    u.signup_week
    {% for n in [1, 7, 14, 30] %}
    , {{ dbt.dateadd('day', n, 'u.signup_date') }} <= b.max_date                    as d{{ n }}_eligible
    , coalesce(max(case when a.days_since_signup = {{ n }} then 1 end), 0) = 1     as d{{ n }}_retained
    {% endfor %}
    , count(distinct a.days_since_signup)                                          as active_days
from {{ ref('dim_users') }} u
cross join bounds b
left join act a on a.user_id = u.user_id
group by u.user_id, u.signup_date, u.signup_week, b.max_date
