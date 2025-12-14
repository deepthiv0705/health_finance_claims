select
    member_id,
    member_name,
    age,
    gender,
    state,
    join_ts,
    ingestion_ts,
    join_year,
    join_month
from {{ source('claim_raw', 'MEMBERS') }}
