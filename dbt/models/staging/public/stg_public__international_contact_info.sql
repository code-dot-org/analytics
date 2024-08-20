with international_contact_info as (
    select * 
    from {{ ref ('base_public__international_contact_info') }}
)

select * 
from international_contact_info