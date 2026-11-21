-- One row per team. Planted pattern: an invite accepted in week 1 -> the team keeps editing.
with first_seen as (
    select team_id, min(activity_date) as first_active_date, max(activity_date) as last_active_date
    from {{ ref('fct_team_activity') }} group by 1
),
wk1 as (
    select a.team_id, sum(a.invites_accepted) as invites_accepted_wk1
    from {{ ref('fct_team_activity') }} a join first_seen f using (team_id)
    where a.activity_date < {{ dbt.dateadd('day', 7, 'f.first_active_date') }}
    group by 1
),
bounds as (select max(activity_date) as max_date from {{ ref('fct_team_activity') }})
select
    f.team_id, f.first_active_date, f.last_active_date,
    coalesce(w.invites_accepted_wk1, 0) > 0                                         as collaborated_week1,
    {{ dbt.dateadd('day', 28, 'f.first_active_date') }} <= b.max_date               as d28_eligible,
    exists (select 1 from {{ ref('fct_team_activity') }} a
            where a.team_id = f.team_id and a.edits > 0
              and a.activity_date >= {{ dbt.dateadd('day', 28, 'f.first_active_date') }}) as active_after_d28
from first_seen f
left join wk1 w using (team_id)
cross join bounds b
