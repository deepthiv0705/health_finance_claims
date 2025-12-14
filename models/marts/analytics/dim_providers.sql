select
    p.provider_id,
    p.provider_name,
    p.speciality,
    p.city,
    p.state,

    count(c.claim_id) as total_claims,
    avg(c.turnaround_days) as avg_turnaround_days,

    case
        when avg(c.turnaround_days) > 15 then 'HIGH_RISK'
        else 'LOW_RISK'
    end as provider_risk_flag

from {{ ref('stg_stg__providers') }} p
left join {{ ref('fct_claims') }} c
on p.provider_id = c.provider_id
group by 1,2,3,4,5
