-- Sanity check on the metric layer: DAU <= WAU <= MAU every day.
select * from {{ ref('mtr_daily_active_users') }}
where dau > wau or wau > mau
