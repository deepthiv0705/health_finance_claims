select
    provider_id,
    provider_name,
    speciality,
    city,
    state,
    join_ts,
    ingestion_ts,
    join_year,
    join_month
from {{ source('claim_raw', 'PROVIDERS') }}
