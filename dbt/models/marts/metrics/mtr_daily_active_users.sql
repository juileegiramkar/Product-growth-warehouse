-- DAU / WAU / MAU and stickiness (DAU/MAU) per day.
-- MAU = distinct users active in the trailing 30 days (incl. today).
with days as (
    select date_day as activity_date
    from {{ ref('dim_date') }}
    where date_day between (select min(activity_date) from {{ ref('fct_user_activity') }})
                       and (select max(activity_date) from {{ ref('fct_user_activity') }})
),
rolling as (
    select
        d.activity_date,
        count(distinct case when a.activity_date = d.activity_date then a.user_id end) as dau,
        count(distinct case when a.activity_date > {{ dbt.dateadd('day', -7, 'd.activity_date') }}
                            then a.user_id end)                                        as wau,
        count(distinct a.user_id)                                                      as mau
    from days d
    join {{ ref('fct_user_activity') }} a
      on a.activity_date between {{ dbt.dateadd('day', -29, 'd.activity_date') }} and d.activity_date
    group by 1
),
signups as (
    select signup_date, count(*) as new_users from {{ ref('dim_users') }} group by 1
)
select
    r.activity_date,
    dd.is_weekend,
    coalesce(s.new_users, 0)                         as new_users,
    r.dau,
    r.wau,
    r.mau,
    round(r.dau * 1.0 / nullif(r.mau, 0), 4)         as dau_mau_stickiness,
    -- the first 29 days don't have a full 30-day lookback yet
    r.activity_date >= {{ dbt.dateadd('day', 29, "(select min(activity_date) from days)") }} as is_full_mau_window
from rolling r
join {{ ref('dim_date') }} dd on dd.date_day = r.activity_date
left join signups s on s.signup_date = r.activity_date
