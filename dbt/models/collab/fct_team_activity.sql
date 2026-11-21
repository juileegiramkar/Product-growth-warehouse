select
    team_id,
    event_date                                                        as activity_date,
    count(distinct user_id)                                           as active_members,
    count(distinct case when event_type = 'file_edit' then user_id end) as editors,
    sum(case when event_type = 'file_edit' then 1 else 0 end)         as edits,
    sum(case when event_type = 'comment' then 1 else 0 end)           as comments,
    sum(case when event_type = 'invite_sent' then 1 else 0 end)       as invites_sent,
    sum(case when event_type = 'invite_accepted' then 1 else 0 end)   as invites_accepted,
    count(distinct session_id)                                        as sessions
from {{ ref('stg_collab_events') }}
group by 1, 2
