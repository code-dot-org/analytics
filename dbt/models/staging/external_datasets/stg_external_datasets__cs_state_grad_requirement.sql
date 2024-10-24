with 
grad_requirement as (
    select *
    from {{ ref('seed_cs_state_grad_requirement') }}
)

select * 
from grad_requirement