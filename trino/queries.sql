-- Presto/Trino SQL over the same marts dbt built (hive.pulse.*).
--   docker compose exec trino trino --catalog hive --schema pulse -f /queries/queries.sql
-- or paste into the Trino CLI:  docker compose exec -it trino trino --catalog hive --schema pulse

-- 1. Last 7 days of DAU / MAU / stickiness
select activity_date, dau, mau, round(dau_mau_stickiness, 3) as stickiness
from mtr_daily_active_users
order by activity_date desc
limit 7;

-- 2. D7 retention by acquisition channel and whether the user followed 5 accounts in 24h
select u.acquisition_channel,
       u.did_follow_5_accounts_24h,
       count(*)                                                   as users,
       round(avg(case when r.d7_retained then 1e0 else 0e0 end), 3) as d7_retention
from dim_users u
join fct_user_retention r on r.user_id = u.user_id
where r.d7_eligible
group by 1, 2
order by 1, 2;

-- 3. Approximate distinct engagers per week (Presto's approx_distinct / HyperLogLog)
select date_trunc('week', event_date) as week,
       approx_distinct(actor_user_id) as engagers,
       count(*)                       as engagement_events
from fct_engagement
group by 1
order by 1;

-- 4. Most-followed accounts (window function)
select target_user_id, follows, rnk
from (
    select target_user_id, count(*) as follows,
           rank() over (order by count(*) desc) as rnk
    from fct_engagement
    where event_type = 'follow'
    group by 1
)
where rnk <= 10;
