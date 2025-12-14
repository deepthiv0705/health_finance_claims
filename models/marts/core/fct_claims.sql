{{ config(
    materialized='incremental',
    unique_key='claim_id',
    incremental_strategy='merge'
) }}

select
    claim_id,
    member_id,
    provider_id,
    claim_ts,
    amount,
    paid_amt,

    round(paid_amt / nullif(amount,0), 2) as reimbursement_ratio,
    turnaround_days,
  {{ claim_value_bucket('amount') }} as claim_value_bucket,
    case when amount >= 50000 then '1'
        else '0'
    end as high_value_flag,
    status,
    ingestion_ts

from {{ ref('stg_stg__claims') }}

{% if is_incremental() %}
where ingestion_ts > (
    select max(ingestion_ts) from {{ this }}
)
{% endif %}
