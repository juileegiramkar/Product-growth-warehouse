-- The three analysis queries from the build plan, written against the marts.
-- Compile with `dbt compile --select plan_queries`; production versions are
-- the mtr_* models in models/marts/metrics.

-- 1. DAU and trailing-30-day MAU
select f1.activity_date,
       count(distinct f1.user_id) as dau,
       (select count(distinct f2.user_id) from {{ ref('fct_user_activity') }} f2
         where f2.activity_date between {{ dbt.dateadd('day', -29, 'f1.activity_date') }} and f1.activity_date) as mau
from {{ ref('fct_user_activity') }} f1
group by 1
order by 1;

-- 2. 7-day retention by weekly signup cohort
select cast({{ dbt.date_trunc('week', 'u.signup_date') }} as date) as cohort_week,
       count(distinct u.user_id) as users,
       count(distinct case when a.activity_date = {{ dbt.dateadd('day', 7, 'u.signup_date') }} then a.user_id end) * 1.0
         / count(distinct u.user_id) as d7_retention
from {{ ref('dim_users') }} u
left join {{ ref('fct_user_activity') }} a using (user_id)
where {{ dbt.dateadd('day', 7, 'u.signup_date') }} <= (select max(activity_date) from {{ ref('fct_user_activity') }})
group by 1
order by 1;

-- 3. D7 retention for users who did vs did not complete each step in 24h
select step_label, completed_in_24h, users, d7_retention
from {{ ref('mtr_onboarding_retention') }}
order by step_order, completed_in_24h desc;
