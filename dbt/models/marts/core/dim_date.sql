-- Calendar dimension covering the simulation window plus headroom.
with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('pulse_start_date') ~ "' as date)",
        end_date="cast('2027-12-31' as date)"
    ) }}
)
select
    cast(date_day as date)                                   as date_day,
    cast({{ dbt.date_trunc('week', 'date_day') }} as date)   as week_start,
    cast({{ dbt.date_trunc('month', 'date_day') }} as date)  as month_start,
    dayofweek(date_day)             as day_of_week,
    dayofweek(date_day) in (0, 6)   as is_weekend
from spine
