select *
from {{ ref('fct_claims') }}
where amount >= 50000
  and high_value_flag <> 1
