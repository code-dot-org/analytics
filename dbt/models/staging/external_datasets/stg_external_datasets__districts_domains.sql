with 
district_domains as (
    select *
    from {{ ref('seed_districts_domains') }}
)

select 
    district_id,
    case
        when domain_name = '(No Value)'
        then null
        else domain_name
        end as domain_name
from district_domains
