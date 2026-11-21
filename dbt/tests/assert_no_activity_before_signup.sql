-- Custom data-quality test: nobody can be active before they signed up.
-- Returns offending rows; the test passes when zero rows come back.
select a.user_id, a.activity_date, u.signup_date
from {{ ref('fct_user_activity') }} a
join {{ ref('dim_users') }} u using (user_id)
where a.activity_date < u.signup_date
