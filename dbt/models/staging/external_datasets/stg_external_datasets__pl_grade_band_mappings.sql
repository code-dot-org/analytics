with 

mappings as (
    select * 
    from {{ ref('seed_pl_grade_band_mappings') }}
)

select 
    lower(topic)        as topic
    , lower(grade_band) as grade_band
from mappings