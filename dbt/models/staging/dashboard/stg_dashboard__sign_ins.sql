with sign_ins as (
    select *
    from {{ref('base_dashboard__sign_ins')}}
)
select *
from sign_ins