select
    member_id,
    member_name,
    age,
    state,
    dbt_valid_from,
    dbt_valid_to,
    case when dbt_valid_to is null then 'Y' else 'N' end as is_current
from {{ ref('members_snapshot') }}
