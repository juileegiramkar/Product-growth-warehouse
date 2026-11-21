-- Retention heatmap: share of each weekly signup cohort active at least once
-- in week N after signup (days 7N .. 7N+6). Only fully elapsed weeks.
with bounds as (select max(activity_date) as max_date from {{ ref('fct_user_activity') }}),
cohorts as (
    select signup_week, count(*) as cohort_users, max(signup_date) as last_signup
    from {{ ref('dim_users') }} group by 1
),
weeks as (
    select distinct cast(floor(days_since_signup / 7) as integer) as week_n
    from {{ ref('fct_user_activity') }}
),
active as (
    select u.signup_week, cast(floor(a.days_since_signup / 7) as integer) as week_n,
           count(distinct a.user_id) as active_users
    from {{ ref('fct_user_activity') }} a
    join {{ ref('dim_users') }} u using (user_id)
    group by 1, 2
)
select
    c.signup_week                                     as cohort_week,
    w.week_n                                          as weeks_since_signup,
    c.cohort_users,
    coalesce(a.active_users, 0)                       as active_users,
    round(coalesce(a.active_users, 0) * 1.0 / c.cohort_users, 4) as retention_rate
from cohorts c
cross join weeks w
cross join bounds b
left join active a on a.signup_week = c.signup_week and a.week_n = w.week_n
-- every member of the cohort must have completed week N
where {{ dbt.dateadd('day', '7 * w.week_n + 6', 'c.last_signup') }} <= b.max_date
