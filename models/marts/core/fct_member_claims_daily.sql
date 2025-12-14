select
    member_id,
    date(claim_ts) as claim_date,

    count(*) as total_claims,
    sum(amount) as total_claimed_amount,
    sum(paid_amt) as total_paid_amount,
    avg(reimbursement_ratio) as avg_reimbursement_ratio

from {{ ref('fct_claims') }}
group by 1,2
