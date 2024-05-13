with projects as (
    select *
    from {{ ref('base_dashboard_pii__projects') }}
)
select *
from projects