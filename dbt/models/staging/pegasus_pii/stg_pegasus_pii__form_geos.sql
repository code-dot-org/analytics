with 
form_geos as (
    select *
    from {{ ref('base_pegasus_pii__form_geos') }}
)

select * 
from form_geos