-- One row per post.
select
    post_id,
    user_id         as author_user_id,
    event_ts        as created_ts,
    event_date      as created_date,
    content_type
from {{ ref('stg_events') }}
where event_type = 'post_created'
