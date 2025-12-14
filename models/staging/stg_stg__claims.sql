select
    claim_id,
    member_id,
    provider_id,
    diagnosis,
    claim_ts,
    amount,
    paid_amt,
    status,
    reimbursement_ratio,
    turnaround_days,
 {{ claim_value_bucket('amount') }} as claim_value_bucket,
    high_value_flag,
    ingestion_ts,
    year,
    month
from {{ source('claim_raw', 'CLAIMS') }}
