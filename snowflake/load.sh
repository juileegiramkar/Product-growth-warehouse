#!/usr/bin/env bash
# Upload the Spark staging output to a Snowflake internal stage and COPY it in.
# Needs SnowSQL (https://docs.snowflake.com/en/user-guide/snowsql-install-config)
# and SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER set; SnowSQL prompts for the password.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SQL=$(cat <<EOF
use schema PULSE.RAW;
remove @lake_stage;
put file://${ROOT}/data/lake/staging/events/*/*.parquet @lake_stage/events/ auto_compress=false parallel=8;
put file://${ROOT}/data/lake/staging/users/*.parquet @lake_stage/users/ auto_compress=false;
truncate table EVENTS;
truncate table USERS;
copy into EVENTS from @lake_stage/events/ match_by_column_name = case_insensitive;
copy into USERS  from @lake_stage/users/  match_by_column_name = case_insensitive;
select 'events' t, count(*) from EVENTS union all select 'users', count(*) from USERS;
EOF
)
snowsql -a "$SNOWFLAKE_ACCOUNT" -u "$SNOWFLAKE_USER" -q "$SQL"
