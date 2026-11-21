{# Generic custom test: the newest value in a date column is within N days of
   the newest staged event (catches a model that ran but loaded nothing new). #}
{% test is_recent(model, column_name, max_lag_days=2) %}
with m as (select max({{ column_name }}) as newest from {{ model }}),
     s as (select max(event_date) as newest from {{ ref('stg_events') }})
select m.newest, s.newest as source_newest
from m cross join s
where m.newest < {{ dbt.dateadd('day', -max_lag_days, 's.newest') }}
{% endtest %}
